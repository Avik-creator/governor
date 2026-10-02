package daemon

import (
	"testing"

	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// eventCounter is a Sink that counts events by kind.
type eventCounter map[core.EventKind]int

func (c eventCounter) Emit(ev core.Event) { c[ev.Kind]++ }

func TestReconcile(t *testing.T) {
	events := eventCounter{}
	engine, err := core.New(core.Config{Sink: events})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg := &config.Config{
		AdminKey: "admin",
		Root:     config.Caps{Limits: map[core.Class]int{"db": 4}},
		Tenants: []config.Tenant{
			{Name: "a", APIKey: "key-a", Weight: 2, Caps: config.Caps{Limits: map[core.Class]int{"db": 1}}},
			{Name: "b", APIKey: "key-b"},
		},
	}

	first, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	a, b := first["key-a"], first["key-b"]
	if first["admin"] != core.RootID || a == 0 || b == 0 || a == b || len(first) != 3 {
		t.Fatalf("keys = %v, want the root and two distinct tenants", first)
	}
	if len(engine.Sessions()) != 0 {
		t.Errorf("reconcile left %d sessions open", len(engine.Sessions()))
	}

	// A second start with the same configuration finds everything in place.
	created, limits := events[core.EventNodeCreated], events[core.EventLimitChanged]
	second, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if second["key-a"] != a || second["key-b"] != b {
		t.Errorf("keys = %v after a restart, want the same nodes %d and %d", second, a, b)
	}
	if events[core.EventNodeCreated] != created || events[core.EventLimitChanged] != limits {
		t.Errorf("an unchanged configuration recorded new nodes or limits: %v", events)
	}

	// A changed limit is applied to the tenant that already exists.
	cfg.Tenants[0].Limits["db"] = 3
	if _, _, err := reconcile(engine, cfg); err != nil {
		t.Fatalf("reconcile with a new limit: %v", err)
	}
	if events[core.EventLimitChanged] != limits+1 || events[core.EventNodeCreated] != created {
		t.Errorf("a changed limit recorded %v", events)
	}

	// A tenant whose node has ended gets a fresh one.
	admin, _, _, _ := engine.OpenSession(core.RootID, 0)
	if _, err := engine.Cancel(admin, b); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := engine.CloseSession(admin); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	third, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("reconcile after a tenant ended: %v", err)
	}
	if third["key-b"] == b || third["key-a"] != a {
		t.Errorf("keys = %v, want a kept at %d and b moved off %d", third, a, b)
	}
}
