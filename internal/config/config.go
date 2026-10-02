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

	"github.com/avikmukherjee/governor/internal/core"
)

// Defaults for the settings a file may leave out.
const (
	DefaultListen       = "127.0.0.1:7600"
	DefaultReapInterval = time.Second
)

// Config is the whole configuration of one governord.
type Config struct {
	// Listen is the address the gRPC server binds to.
	Listen string `yaml:"listen"`

	// DatabaseURL is the Postgres to record to; empty keeps everything in memory.
	DatabaseURL string `yaml:"database_url"`

	// ReapInterval is how often expired sessions, leases and deadlines are applied.
	ReapInterval time.Duration `yaml:"reap_interval"`

	// AdminKey opens sessions scoped to the root; empty allows none.
	AdminKey string `yaml:"admin_key"`

	// Root holds the caps of the root node, which are the shared pools.
	Root Caps `yaml:"root"`

	// Tenants are the nodes directly under the root, one per API key.
	Tenants []Tenant `yaml:"tenants"`
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

	cfg := &Config{Listen: DefaultListen, ReapInterval: DefaultReapInterval}
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
	if c.ReapInterval <= 0 {
		return errors.New("reap_interval must be positive")
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
