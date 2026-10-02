package core

import (
	"errors"
	"maps"
	"testing"
	"time"
)

func TestSetQuota(t *testing.T) {
	rec := &recorder{}
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Sink: rec})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin := mustSession(t, e, RootID, 0)
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "tenant"})
	task := mustNode(t, e, admin, tenant, Spec{Quotas: map[Resource]int64{"http": 2}})
	worker := mustSession(t, e, tenant, 0)

	consume := func(amount int64) error {
		_, err := e.Consume(worker, task, "http", amount)
		return err
	}
	if err := consume(2); err != nil {
		t.Fatalf("Consume within the cap: %v", err)
	}
	if err := consume(1); !errors.Is(err, ErrDenied) {
		t.Fatalf("Consume over the cap = %v, want ErrDenied", err)
	}

	// Raising the cap lets the task carry on.
	if _, err := e.SetQuota(worker, task, "http", 3); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if err := consume(1); err != nil {
		t.Fatalf("Consume after the cap was raised: %v", err)
	}

	// A cap below what is used takes nothing back, but stops further use.
	if _, err := e.SetQuota(worker, task, "http", 1); err != nil {
		t.Fatalf("SetQuota below usage: %v", err)
	}
	if got := used(e, task, "http"); got != 3 {
		t.Errorf("used = %d after lowering the cap, want 3", got)
	}
	if err := consume(1); !errors.Is(err, ErrDenied) {
		t.Fatalf("Consume after the cap was lowered = %v, want ErrDenied", err)
	}

	// Setting the same cap again records nothing.
	before := len(rec.events)
	if _, err := e.SetQuota(worker, task, "http", 1); err != nil || len(rec.events) != before {
		t.Errorf("SetQuota unchanged = %v with %d new events, want none", err, len(rec.events)-before)
	}

	if _, err := e.SetQuota(worker, task, "http", Unlimited); err != nil {
		t.Fatalf("SetQuota(Unlimited): %v", err)
	}
	if err := consume(1000); err != nil {
		t.Fatalf("Consume with the cap removed: %v", err)
	}
	if _, err := e.SetQuota(worker, task, "sql", Unlimited); err != nil || len(rec.events) != before+2 {
		t.Errorf("removing a cap that is not set = %v with %d new events, want one", err, len(rec.events)-before)
	}
}

func TestEditRejects(t *testing.T) {
	e, admin, _ := newEngine(t, Spec{})
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "a"})
	other := mustNode(t, e, admin, RootID, Spec{Name: "b"})
	task := mustNode(t, e, admin, tenant, Spec{})
	ended := mustNode(t, e, admin, tenant, Spec{})
	if _, err := e.Cancel(admin, ended); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	worker := mustSession(t, e, tenant, 0)

	tests := []struct {
		name string
		sid  SessionID
		node NodeID
		r    Resource
		cap  int64
		want error
	}{
		{"own scope", worker, tenant, "http", 10, ErrForbidden},
		{"another tenant", worker, other, "http", 10, ErrForbidden},
		{"above the scope", worker, RootID, "http", 10, ErrForbidden},
		{"ended node", worker, ended, "http", 10, ErrClosed},
		{"unknown node", admin, 999, "http", 10, ErrUnknownNode},
		{"empty resource", admin, task, "", 10, ErrInvalid},
		{"negative cap", admin, task, "http", -2, ErrInvalid},
		{"dead session", 999, task, "http", 10, ErrSessionExpired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := e.SetQuota(tc.sid, tc.node, tc.r, tc.cap); !errors.Is(err, tc.want) {
				t.Errorf("SetQuota = %v, want %v", err, tc.want)
			}
			if _, err := e.SetLimitAs(tc.sid, tc.node, Class(tc.r), int(tc.cap)); !errors.Is(err, tc.want) {
				t.Errorf("SetLimitAs = %v, want %v", err, tc.want)
			}
		})
	}

	// The root has nothing above it, so a session scoped to it may change it.
	if _, err := e.SetQuota(admin, RootID, "http", 10); err != nil {
		t.Errorf("SetQuota on the root as admin: %v", err)
	}
	if _, err := e.SetQuota(admin, tenant, "http", 10); err != nil {
		t.Errorf("SetQuota on a tenant as admin: %v", err)
	}
	// Defaults only shape children, so a session may set them on its own scope but not outside it.
	if _, err := e.SetDefaults(worker, tenant, &Defaults{}); err != nil {
		t.Errorf("SetDefaults on the session's scope: %v", err)
	}
	if _, err := e.SetDefaults(worker, other, &Defaults{}); !errors.Is(err, ErrForbidden) {
		t.Errorf("SetDefaults on another tenant = %v, want ErrForbidden", err)
	}
	bad := &Defaults{Quotas: map[Resource]int64{"http": -1}}
	if _, err := e.SetDefaults(admin, tenant, bad); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetDefaults with a negative quota = %v, want ErrInvalid", err)
	}
}

func TestSetLimitAs(t *testing.T) {
	e, admin, _ := newEngine(t, Spec{})
	task := mustNode(t, e, admin, RootID, Spec{Limits: map[Class]int{"db": 1}})
	first := mustAcquire(t, e, admin, task, "db")

	out := make(chan grant, 1)
	acquireAsync(t, e, t.Context(), admin, task, "db", 1, out)
	waitQueued(t, e, 1)
	// Raising the limit lets the queued acquire through.
	if _, err := e.SetLimitAs(admin, task, "db", 2); err != nil {
		t.Fatalf("SetLimitAs: %v", err)
	}
	if g := <-out; g.err != nil {
		t.Fatalf("queued Acquire: %v", g.err)
	}
	if _, err := e.SetLimitAs(admin, task, "db", Unlimited); err != nil {
		t.Fatalf("SetLimitAs(Unlimited): %v", err)
	}
	mustAcquire(t, e, admin, task, "db")
	mustRelease(t, e, admin, first)
}

func TestDefaults(t *testing.T) {
	rec := &recorder{}
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Sink: rec})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin := mustSession(t, e, RootID, 0)
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "tenant"})
	defaults := &Defaults{
		Quotas:   map[Resource]int64{"tool_calls": 5, "agents": 2},
		Children: &Defaults{Quotas: map[Resource]int64{"tool_calls": 1}},
	}
	if _, err := e.SetDefaults(admin, tenant, defaults); err != nil {
		t.Fatalf("SetDefaults: %v", err)
	}
	// The engine keeps its own copy.
	defaults.Quotas["tool_calls"] = 999

	quotas := func(id NodeID) map[Resource]int64 {
		t.Helper()
		info, _, err := e.Describe(admin, id)
		if err != nil {
			t.Fatalf("Describe(%d): %v", id, err)
		}
		return info.Quotas
	}
	// The defaults win over the spec for what they name, and leave the rest alone.
	run, _, _, err := e.EnsureNode(admin, tenant, Spec{
		Name:   "run",
		Quotas: map[Resource]int64{"tool_calls": 500, "http": 7},
	}, "", 0)
	if err != nil {
		t.Fatalf("EnsureNode: %v", err)
	}
	want := map[Resource]int64{"tool_calls": 5, "agents": 2, "http": 7}
	if got := quotas(run); !maps.Equal(got, want) {
		t.Errorf("run quotas = %v, want %v", got, want)
	}
	// The run got the children's part as its own defaults, so its child is capped too.
	agent := mustNode(t, e, admin, run, Spec{Name: "agent"})
	if got := quotas(agent); !maps.Equal(got, map[Resource]int64{"tool_calls": 1}) {
		t.Errorf("agent quotas = %v, want tool_calls: 1", got)
	}
	if got := quotas(mustNode(t, e, admin, agent, Spec{})); len(got) != 0 {
		t.Errorf("a node below the defaults has quotas %v, want none", got)
	}

	// Changing the defaults leaves the nodes that exist as they are.
	if _, err := e.SetDefaults(admin, tenant, nil); err != nil {
		t.Fatalf("SetDefaults(nil): %v", err)
	}
	if got := quotas(run); !maps.Equal(got, want) {
		t.Errorf("run quotas = %v after the defaults were cleared, want %v", got, want)
	}
	if got := quotas(mustNode(t, e, admin, tenant, Spec{Name: "later"})); len(got) != 0 {
		t.Errorf("a node created after the defaults were cleared has quotas %v", got)
	}

	restored := mustRestore(t, clock, rec.events)
	sameState(t, e, restored)
}

func TestDescribe(t *testing.T) {
	e, admin, clock := newEngine(t, Spec{})
	deadline := clock.Now().Add(time.Hour)
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "tenant", Quotas: map[Resource]int64{"http": 10}})
	first := mustNode(t, e, admin, tenant, Spec{Name: "first", Limits: map[Class]int{"db": 2}, Deadline: deadline})
	second := mustNode(t, e, admin, tenant, Spec{Name: "second"})
	mustNode(t, e, admin, first, Spec{})
	if _, err := e.Consume(admin, first, "http", 3); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	mustAcquire(t, e, admin, first, "db")
	if _, err := e.Cancel(admin, second); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	node, children, err := e.Describe(admin, tenant)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if node.ID != tenant || node.Parent != RootID || node.Name != "tenant" || node.Children != 2 ||
		node.Quotas["http"] != 10 || node.Used["http"] != 3 || node.Held["db"] != 1 {
		t.Errorf("tenant = %+v", node)
	}
	if len(children) != 2 || children[0].ID != first || children[1].ID != second {
		t.Fatalf("children = %+v, want first then second", children)
	}
	if c := children[0]; c.State != StateActive || c.Limits["db"] != 2 || c.Held["db"] != 1 ||
		c.Used["http"] != 3 || !c.Deadline.Equal(deadline) || c.Children != 1 || !c.EndedAt.IsZero() {
		t.Errorf("first = %+v", c)
	}
	if c := children[1]; c.State != StateCancelled || !c.EndedAt.Equal(clock.Now()) {
		t.Errorf("second = %+v", c)
	}

	// What Describe returns is a copy.
	node.Quotas["http"] = 1
	if again, _, _ := e.Describe(admin, tenant); again.Quotas["http"] != 10 {
		t.Errorf("changing a description changed the node")
	}
	worker := mustSession(t, e, first, 0)
	if _, _, err := e.Describe(worker, tenant); !errors.Is(err, ErrForbidden) {
		t.Errorf("Describe above the scope = %v, want ErrForbidden", err)
	}
}
