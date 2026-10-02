// Package config reads the governord configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/Avik-creator/governor/internal/core"
)

// Defaults for the settings a file may leave out.
const (
	DefaultListen       = "127.0.0.1:7600"
	DefaultReapInterval = time.Second
	DefaultDrainTimeout = 5 * time.Second

	DefaultAdaptiveInterval = time.Second
	DefaultAdaptiveSamples  = 20
	DefaultAdaptiveOverload = 0.05
)

// Config is the whole configuration of one governord.
type Config struct {
	// Listen is the address the gRPC server binds to.
	Listen string `yaml:"listen"`

	// DatabaseURL is the Postgres to record to; empty keeps everything in memory.
	DatabaseURL string `yaml:"database_url"`

	// ReapInterval is how often expired sessions, leases and deadlines are applied.
	ReapInterval time.Duration `yaml:"reap_interval"`

	// DrainTimeout is how long in-flight calls get to finish on shutdown.
	DrainTimeout time.Duration `yaml:"drain_timeout"`

	// AdminKey opens sessions scoped to the root; empty allows none.
	AdminKey string `yaml:"admin_key"`

	// Root holds the caps of the root node, which are the shared pools.
	Root Caps `yaml:"root"`

	// Tenants are the nodes directly under the root, one per API key.
	Tenants []Tenant `yaml:"tenants"`

	// Adaptive lists the root limits that are tuned from what their leases report.
	Adaptive []Adaptive `yaml:"adaptive"`
}

// Adaptive describes the controller of one class's limit on the root.
type Adaptive struct {
	// Class is the class whose root limit is tuned; the limit in Root is where it starts.
	Class core.Class `yaml:"class"`

	// TargetP95 is the 95th percentile latency the downstream should stay under.
	TargetP95 time.Duration `yaml:"target_p95"`

	// MaxOverload is the share of releases that may report overload; zero means 0.05.
	MaxOverload float64 `yaml:"max_overload"`

	// MinLimit and MaxLimit bound what the controller may set.
	MinLimit int `yaml:"min_limit"`
	MaxLimit int `yaml:"max_limit"`

	// Interval is how often the limit is reconsidered; zero means one second.
	Interval time.Duration `yaml:"interval"`

	// MinSamples is how many releases an interval needs; zero means 20.
	MinSamples int `yaml:"min_samples"`
}

// Caps are the quotas and limits of one node.
type Caps struct {
	// Quotas caps total consumption per resource.
	Quotas map[core.Resource]int64 `yaml:"quotas"`

	// Limits caps leases held at once per class.
	Limits map[core.Class]int `yaml:"limits"`
}

// Tenant describes one tenant node and the key that opens sessions on it.
type Tenant struct {
	// Name identifies the tenant across restarts, so it must not change.
	Name string `yaml:"name"`

	// APIKey opens sessions confined to this tenant.
	APIKey string `yaml:"api_key"`

	// Weight is the tenant's share when competing for a class; zero means 1.
	Weight int `yaml:"weight"`

	Caps `yaml:",inline"`
}

// Spec returns the node spec these caps describe.
func (c Caps) Spec() core.Spec {
	return core.Spec{Quotas: c.Quotas, Limits: c.Limits}
}

// Spec returns the node spec of the tenant.
func (t Tenant) Spec() core.Spec {
	spec := t.Caps.Spec()
	spec.Name, spec.Weight = t.Name, t.Weight
	return spec
}

// Load reads, expands and validates the configuration file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes a configuration, replacing ${NAME} with environment variables.
func Parse(data []byte) (*Config, error) {
	var missing []string
	expanded := os.Expand(string(data), func(name string) string {
		value, ok := os.LookupEnv(name)
		if !ok && !slices.Contains(missing, name) {
			missing = append(missing, name)
		}
		return value
	})
	// An unset variable must not quietly become an empty key or database.
	if len(missing) > 0 {
		return nil, fmt.Errorf("environment variables are not set: %v", missing)
	}

	cfg := &Config{Listen: DefaultListen, ReapInterval: DefaultReapInterval, DrainTimeout: DefaultDrainTimeout}
	dec := yaml.NewDecoder(bytes.NewReader([]byte(expanded)))
	dec.KnownFields(true)
	// An empty file is a valid configuration made of defaults.
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validate reports the first setting that governord could not run with.
func (c *Config) validate() error {
	if c.Listen == "" {
		return errors.New("listen is empty")
	}
	if c.ReapInterval <= 0 || c.DrainTimeout <= 0 {
		return errors.New("reap_interval and drain_timeout must be positive")
	}
	if err := c.Root.validate(); err != nil {
		return fmt.Errorf("root: %w", err)
	}
	names := make(map[string]bool, len(c.Tenants))
	keys := map[string]string{}
	if c.AdminKey != "" {
		keys[c.AdminKey] = "admin_key"
	}
	for i, t := range c.Tenants {
		if t.Name == "" {
			return fmt.Errorf("tenant %d: name is empty", i)
		}
		if names[t.Name] {
			return fmt.Errorf("tenant %q is listed twice", t.Name)
		}
		names[t.Name] = true
		if t.APIKey == "" {
			return fmt.Errorf("tenant %q: api_key is empty", t.Name)
		}
		if other, ok := keys[t.APIKey]; ok {
			return fmt.Errorf("tenant %q shares its api_key with %s", t.Name, other)
		}
		keys[t.APIKey] = fmt.Sprintf("tenant %q", t.Name)
		if t.Weight < 0 {
			return fmt.Errorf("tenant %q: weight is negative", t.Name)
		}
		if err := t.Caps.validate(); err != nil {
			return fmt.Errorf("tenant %q: %w", t.Name, err)
		}
	}
	tuned := make(map[core.Class]bool, len(c.Adaptive))
	for i := range c.Adaptive {
		a := &c.Adaptive[i]
		if _, ok := c.Root.Limits[a.Class]; !ok {
			return fmt.Errorf("adaptive %q: the root has no limit for this class to start from", a.Class)
		}
		if tuned[a.Class] {
			return fmt.Errorf("adaptive %q is listed twice", a.Class)
		}
		tuned[a.Class] = true
		if a.Interval == 0 {
			a.Interval = DefaultAdaptiveInterval
		}
		if a.MinSamples == 0 {
			a.MinSamples = DefaultAdaptiveSamples
		}
		if a.MaxOverload == 0 {
			a.MaxOverload = DefaultAdaptiveOverload
		}
	}
	return nil
}

func (c Caps) validate() error {
	for r, limit := range c.Quotas {
		if r == "" || limit < 0 {
			return fmt.Errorf("quota %q must be named and not negative", r)
		}
	}
	for class, limit := range c.Limits {
		if class == "" || limit < 0 {
			return fmt.Errorf("limit %q must be named and not negative", class)
		}
	}
	return nil
}
