package core

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"maps"
	"slices"
	"testing"
	"time"
)

// replay yields recorded events the way a store would.
func replay(events []Event) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for _, ev := range events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// viaJSON passes events through their stored form, as a real restart does.
func viaJSON(t *testing.T, events []Event) []Event {
	t.Helper()
	out := make([]Event, len(events))
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if err := json.Unmarshal(data, &out[i]); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
	}
	return out
}

func mustRestore(t *testing.T, clock Clock, events []Event) *Engine {
	t.Helper()
	e, err := Restore(Config{Clock: clock}, replay(viaJSON(t, events)))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return e
}

func nodeID(n *node) NodeID {
	if n == nil {
		return 0
	}
	return n.id
}

// sameState fails unless got holds exactly the durable state of want.
func sameState(t *testing.T, want, got *Engine) {
	t.Helper()
	want.mu.Lock()
	defer want.mu.Unlock()
	got.mu.Lock()
	defer got.mu.Unlock()

	if want.ids != got.ids || want.seq != got.seq {
		t.Fatalf("ids, seq = %d, %d, want %d, %d", got.ids, got.seq, want.ids, want.seq)
	}
	if len(want.nodes) != len(got.nodes) || len(want.sessions) != len(got.sessions) || len(want.leases) != len(got.leases) {
		t.Fatalf("restored %d nodes, %d sessions, %d leases, want %d, %d, %d",
			len(got.nodes), len(got.sessions), len(got.leases),
			len(want.nodes), len(want.sessions), len(want.leases))
	}
	for id, w := range want.nodes {
		g := got.nodes[id]
		if g == nil {
			t.Fatalf("node %d is missing", id)
		}
		same := w.name == g.name && nodeID(w.parent) == nodeID(g.parent) && w.tenant.id == g.tenant.id &&
			w.depth == g.depth && w.weight == g.weight && w.priority == g.priority &&
			w.state == g.state && w.deadline.Equal(g.deadline) &&
			maps.Equal(w.quota, g.quota) && maps.Equal(w.used, g.used) && maps.Equal(w.self, g.self) &&
			maps.Equal(w.limits, g.limits) && maps.Equal(w.held, g.held) &&
			maps.Equal(w.gone, g.gone) && w.endedAt.Equal(g.endedAt) &&
			slices.Equal(slices.Sorted(maps.Keys(w.leases)), slices.Sorted(maps.Keys(g.leases)))
		if !same {
			t.Fatalf("node %d differs:\n got %+v\nwant %+v", id, *g, *w)
		}
		for name, child := range w.named {
			if nodeID(g.named[name]) != child.id {
				t.Fatalf("node %d has newest child %d named %q, want %d", id, nodeID(g.named[name]), name, child.id)
			}
		}
		for r, top := range w.top {
			if nodeID(g.top[r]) != top.id {
				t.Fatalf("node %d has top consumer %d for %s, want %d", id, nodeID(g.top[r]), r, top.id)
			}
		}
	}
	if !slices.Equal(slices.Sorted(maps.Keys(want.timed)), slices.Sorted(maps.Keys(got.timed))) {
		t.Fatalf("nodes with a deadline differ")
	}
	for id, w := range want.sessions {
		g := got.sessions[id]
		if g == nil || w.scope.id != g.scope.id || w.ttl != g.ttl || len(w.leases) != len(g.leases) {
			t.Fatalf("session %d differs: got %+v, want %+v", id, g, w)
		}
	}
	for id, w := range want.leases {
		g := got.leases[id]
		if g == nil || w.n.id != g.n.id || w.class != g.class || w.s.id != g.s.id || !w.expires.Equal(g.expires) {
			t.Fatalf("lease %d differs: got %+v, want %+v", id, g, w)
		}
		if (want.timedLeases[id] != nil) != (got.timedLeases[id] != nil) {
			t.Fatalf("lease %d has a hold time in only one engine", id)
		}
	}
	// The original may already have forgotten old leases; the rest must agree.
	for id, w := range want.tombs {
		if g, ok := got.tombs[id]; !ok || g.end != w.end || !g.at.Equal(w.at) {
			t.Fatalf("ended lease %d differs: got %+v, want %+v", id, g, w)
		}
	}
}

func TestRestore(t *testing.T) {
	rec := &recorder{}
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Sink: rec, Root: Spec{Limits: map[Class]int{"db": 3}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin := mustSession(t, e, RootID, 0)
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "tenant", Weight: 3, Quotas: map[Resource]int64{"http": 100}})
	task := mustNode(t, e, admin, tenant, Spec{Name: "task", Priority: 2, Deadline: clock.Now().Add(time.Hour)})
	ended := mustNode(t, e, admin, tenant, Spec{Name: "ended"})
	worker := mustSession(t, e, tenant, 30*time.Second)
	gone := mustSession(t, e, tenant, time.Minute)

	if _, err := e.Consume(worker, task, "http", 40); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := e.Consume(worker, ended, "http", 7); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	held := mustAcquire(t, e, worker, task, "db")
	timed, _, err := e.Acquire(context.Background(), worker, task, "db", AcquireOptions{MaxHold: time.Minute})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	released := mustAcquire(t, e, gone, ended, "db")
	mustRelease(t, e, gone, released)
	revoked := mustAcquire(t, e, gone, ended, "db")
	if _, err := e.Close(admin, ended); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := e.CloseSession(gone); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if _, err := e.SetLimit(tenant, "db", 2); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}

	// The restart happens 20s later, with the worker's TTL two thirds gone.
	clock.Advance(20 * time.Second)
	r := mustRestore(t, clock, rec.events)
	sameState(t, e, r)
	checkInvariants(t, r, map[NodeID]State{})

	t.Run("quotas and limits still bind", func(t *testing.T) {
		_, err := r.Consume(worker, task, "http", 54)
		if d, ok := errors.AsType[*DeniedError](err); !ok || d.Used != 47 || d.TopConsumer != task {
			t.Errorf("Consume = %v, want a denial at 47 used with task as top consumer", err)
		}
		// The tenant's limit of 2 is full, so a third acquire has to queue.
		ctx, cancel := context.WithCancel(context.Background())
		out := make(chan grant, 1)
		acquireAsync(t, r, ctx, worker, task, "db", 1, out)
		cancel()
		if g := <-out; !errors.Is(g.err, context.Canceled) {
			t.Errorf("queued Acquire = %v, want context.Canceled", g.err)
		}
	})

	t.Run("ended things stay ended", func(t *testing.T) {
		mustState(t, r, ended, StateDone)
		if _, err := r.Heartbeat(gone); !errors.Is(err, ErrSessionExpired) {
			t.Errorf("Heartbeat(closed session) = %v, want ErrSessionExpired", err)
		}
		if _, err := r.Release(worker, revoked, Report{}); !errors.Is(err, ErrLeaseRevoked) {
			t.Errorf("Release(revoked) = %v, want ErrLeaseRevoked", err)
		}
		if _, err := r.Release(gone, released, Report{}); err != nil {
			t.Errorf("Release(already released) = %v, want nil", err)
		}
	})

	t.Run("sessions get one fresh TTL", func(t *testing.T) {
		clock.Advance(29 * time.Second)
		if err := r.Validate(held); err != nil {
			t.Fatalf("Validate 29s after the restart: %v", err)
		}
		clock.Advance(time.Second)
		if err := r.Validate(held); !errors.Is(err, ErrLeaseExpired) {
			t.Errorf("Validate 30s after the restart = %v, want ErrLeaseExpired", err)
		}
	})

	t.Run("ids and seq carry on", func(t *testing.T) {
		rec2 := &recorder{}
		r, err := Restore(Config{Clock: clock, Sink: rec2}, replay(rec.events))
		if err != nil {
			t.Fatalf("Restore: %v", err)
		}
		if len(rec2.events) != 0 {
			t.Fatalf("Restore emitted %d events, want 0", len(rec2.events))
		}
		last := rec.events[len(rec.events)-1].Seq
		id, seq, err := r.CreateNode(admin, tenant, Spec{})
		if err != nil {
			t.Fatalf("CreateNode: %v", err)
		}
		if seq != last+1 || rec2.events[0].Seq != last+1 {
			t.Errorf("seq = %d, want %d", seq, last+1)
		}
		if uint64(id) <= uint64(timed) || uint64(id) <= uint64(revoked) {
			t.Errorf("new id %d is not greater than every restored id", id)
		}
	})
}

func TestRestoreWithoutEvents(t *testing.T) {
	rec := &recorder{}
	e, err := Restore(Config{Sink: rec, Root: Spec{Limits: map[Class]int{"db": 1}}}, replay(nil))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if len(rec.events) != 1 || rec.events[0].Kind != EventNodeCreated {
		t.Fatalf("events = %v, want the root's creation", rec.events)
	}
	if got := e.root.limits["db"]; got != 1 {
		t.Errorf("root limit = %d, want 1", got)
	}
}

func TestRestoreRejectsCorruptLog(t *testing.T) {
	root := Event{Seq: 1, Kind: EventNodeCreated, Node: RootID, Spec: &Spec{}}
	session := Event{Seq: 2, Kind: EventSessionOpened, Node: RootID, Session: 2}
	tests := []struct {
		name   string
		events []Event
	}{
		{"does not start at one", []Event{{Seq: 2, Kind: EventNodeCreated, Node: RootID, Spec: &Spec{}}}},
		{"gap in seq", []Event{root, {Seq: 3, Kind: EventSessionOpened, Node: RootID, Session: 2}}},
		{"no root", []Event{{Seq: 1, Kind: EventSessionOpened, Node: RootID, Session: 2}}},
		{"second root", []Event{root, {Seq: 2, Kind: EventNodeCreated, Node: RootID, Spec: &Spec{}}}},
		{"node without spec", []Event{root, {Seq: 2, Kind: EventNodeCreated, Node: 2, Parent: RootID}}},
		{"unknown parent", []Event{root, {Seq: 2, Kind: EventNodeCreated, Node: 3, Parent: 2, Spec: &Spec{}}}},
		{"consume on unknown node", []Event{root, {Seq: 2, Kind: EventConsumed, Node: 9, Resource: "http", Amount: 1}}},
		{"lease for unknown session", []Event{root, {Seq: 2, Kind: EventLeaseGranted, Node: RootID, Session: 9, Lease: 3}}},
		{"unknown lease ended", []Event{root, session, {Seq: 3, Kind: EventLeaseEnded, Node: RootID, Lease: 9}}},
		{"unknown kind", []Event{root, {Seq: 2, Kind: "renamed", Node: RootID}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Restore(Config{}, replay(tc.events)); !errors.Is(err, ErrCorrupt) {
				t.Errorf("Restore = %v, want ErrCorrupt", err)
			}
		})
	}

	t.Run("read error", func(t *testing.T) {
		boom := errors.New("connection lost")
		events := func(yield func(Event, error) bool) {
			_ = yield(root, nil) && yield(Event{}, boom)
		}
		if _, err := Restore(Config{}, events); !errors.Is(err, boom) {
			t.Errorf("Restore = %v, want the read error", err)
		}
	})
}
