package config

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/avikmukherjee/governor/internal/core"
)

const full = `
listen: 0.0.0.0:9000
database_url: ${TEST_DATABASE_URL}
reap_interval: 250ms
admin_key: ${TEST_ADMIN_KEY}
root:
  limits: {db: 20, http: 50}
tenants:
  - name: team-a
    api_key: ${TEST_KEY_A}
    weight: 3
    quotas: {http: 100000}
    limits: {agents: 5}
  - name: team-b
    api_key: key-b
`

func setEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TEST_DATABASE_URL", "postgres://localhost/governor")
	t.Setenv("TEST_ADMIN_KEY", "admin-secret")
	t.Setenv("TEST_KEY_A", "key-a")
}

func TestParse(t *testing.T) {
	setEnv(t)
	cfg, err := Parse([]byte(full))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Listen != "0.0.0.0:9000" || cfg.ReapInterval != 250*time.Millisecond {
		t.Errorf("listen, reap_interval = %q, %v", cfg.Listen, cfg.ReapInterval)
	}
	if cfg.DatabaseURL != "postgres://localhost/governor" || cfg.AdminKey != "admin-secret" {
		t.Errorf("database_url, admin_key = %q, %q", cfg.DatabaseURL, cfg.AdminKey)
	}
	if want := map[core.Class]int{"db": 20, "http": 50}; !maps.Equal(cfg.Root.Spec().Limits, want) {
		t.Errorf("root limits = %v, want %v", cfg.Root.Limits, want)
	}
	if len(cfg.Tenants) != 2 {
		t.Fatalf("got %d tenants, want 2", len(cfg.Tenants))
	}
	a := cfg.Tenants[0].Spec()
	if cfg.Tenants[0].APIKey != "key-a" || a.Name != "team-a" || a.Weight != 3 ||
		a.Quotas["http"] != 100000 || a.Limits["agents"] != 5 {
		t.Errorf("tenant a = %+v", cfg.Tenants[0])
	}
	if b := cfg.Tenants[1]; b.APIKey != "key-b" || b.Weight != 0 || b.Quotas != nil {
		t.Errorf("tenant b = %+v", b)
	}
}

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse of an empty file: %v", err)
	}
	if cfg.Listen != DefaultListen || cfg.ReapInterval != DefaultReapInterval {
		t.Errorf("listen, reap_interval = %q, %v, want the defaults", cfg.Listen, cfg.ReapInterval)
	}
	if cfg.DatabaseURL != "" || cfg.AdminKey != "" || len(cfg.Tenants) != 0 {
		t.Errorf("an empty file set %+v", cfg)
	}
}

func TestParseRejects(t *testing.T) {
	setEnv(t)
	tests := []struct {
		name string
		yaml string
		want string // a fragment of the error
	}{
		{"unset variable", "admin_key: ${TEST_NOT_SET}", "TEST_NOT_SET"},
		{"unknown field", "listn: :1", "listn"},
		{"malformed yaml", "tenants: [", "yaml"},
		{"empty listen", `listen: ""`, "listen"},
		{"zero reap interval", "reap_interval: 0s", "reap_interval"},
		{"negative root limit", "root: {limits: {db: -1}}", "root"},
		{"unnamed tenant", "tenants: [{api_key: k}]", "name is empty"},
		{"tenant without key", "tenants: [{name: a}]", "api_key is empty"},
		{"duplicate tenant", "tenants: [{name: a, api_key: k1}, {name: a, api_key: k2}]", "listed twice"},
		{"shared key", "tenants: [{name: a, api_key: k}, {name: b, api_key: k}]", "shares its api_key"},
		{"tenant with admin key", "admin_key: k\ntenants: [{name: a, api_key: k}]", "admin_key"},
		{"negative weight", "tenants: [{name: a, api_key: k, weight: -1}]", "weight"},
		{"negative quota", "tenants: [{name: a, api_key: k, quotas: {http: -1}}]", "quota"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Parse = %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	setEnv(t)
	path := filepath.Join(t.TempDir(), "governor.yaml")
	if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Load(path); err != nil {
		t.Errorf("Load: %v", err)
	}
	if _, err := Load(path + ".missing"); err == nil {
		t.Error("Load of a missing file succeeded")
	}
}
