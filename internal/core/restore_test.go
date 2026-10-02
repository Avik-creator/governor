package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// snapshotViaJSON passes a snapshot through its stored form.
func snapshotViaJSON(t *testing.T, snap *Snapshot) *Snapshot {
	t.Helper()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out := &Snapshot{}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	return out
}

// mustRestoreFrom restores from a snapshot and the events recorded after it.
func mustRestoreFrom(t *testing.T, clock Clock, snap *Snapshot, events []Event) *Engine {
	t.Helper()
	// Event n is at index n-1, so the events after the snapshot start at index Seq.
	tail := viaJSON(t, events[snap.Seq:])
	e, err := Restore(Config{Clock: clock}, snapshotViaJSON(t, snap), replay(tail))
	if err != nil {
		t.Fatalf("Restore from a snapshot at event %d: %v", snap.Seq, err)
	}
	return e
}

func mustRestore(t *testing.T, clock Clock, events []Event) *Engine {
	t.Helper()
	e, err := Restore(Config{Clock: clock}, nil, replay(viaJSON(t, events)))
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

// sameCounts compares two counters, treating a missing entry as zero.
func sameCounts[K comparable](a, b map[K]int) bool {
	for k := range union(a, b) {
		if a[k] != b[k] {
			return false
		}
	}
	return true
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
			maps.Equal(w.limits, g.limits) && sameCounts(w.held, g.held) &&
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
		if !slices.Equal(w.order, g.order) || !maps.Equal(w.requests, g.requests) {
			t.Fatalf("session %d remembers requests %v, want %v", id, g.requests, w.requests)
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
		r, err := Restore(Config{Clock: clock, Sink: rec2}, nil, replay(rec.events))
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

func TestRestoreFromSnapshot(t *testing.T) {
	rec := &recorder{}
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Sink: rec, NodeRetention: time.Hour, Root: Spec{Limits: map[Class]int{"db": 3}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin := mustSession(t, e, RootID, 0)
	tenant := mustNode(t, e, admin, RootID, Spec{Name: "tenant", Weight: 2, Quotas: map[Resource]int64{"http": 100}})
	task := mustNode(t, e, admin, tenant, Spec{Name: "task", Priority: 1, Deadline: clock.Now().Add(3 * time.Hour)})
	old := mustNode(t, e, admin, tenant, Spec{Name: "old"})
	removed := mustNode(t, e, admin, tenant, Spec{Name: "removed"})
	worker := mustSession(t, e, tenant, time.Minute)
	for id, amount := range map[NodeID]int64{task: 20, old: 9, removed: 4} {
		if _, err := e.Consume(admin, id, "http", amount); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	mustAcquire(t, e, worker, task, "db")
	released := mustAcquire(t, e, worker, task, "db")
	mustRelease(t, e, worker, released)
	if _, err := e.SetLimit(tenant, "db", 2); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	// One node ends and is removed before the snapshot; another ends and is still remembered.
	if _, err := e.Cancel(admin, removed); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	clock.Advance(time.Hour)
	if _, err := e.Heartbeat(worker); err == nil {
		t.Fatal("the worker's session should have lapsed with the hour")
	}
	worker = mustSession(t, e, tenant, 0)
	held := mustAcquire(t, e, worker, task, "db")
	e.Reap()
	if _, err := e.Close(admin, old); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snap := e.Snapshot()
	if snap.Seq != rec.events[len(rec.events)-1].Seq {
		t.Fatalf("snapshot is at event %d, want the last event %d", snap.Seq, rec.events[len(rec.events)-1].Seq)
	}
	r := mustRestoreFrom(t, clock, snap, rec.events)
	sameState(t, e, r)
	checkInvariants(t, r, map[NodeID]State{})

	// Work after the snapshot is replayed on top of it.
	if _, err := e.Consume(worker, task, "http", 5); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	later := mustNode(t, e, admin, tenant, Spec{Name: "later"})
	mustRelease(t, e, worker, held)
	clock.Advance(time.Hour)
	e.Reap()
	r = mustRestoreFrom(t, clock, snap, rec.events)
	sameState(t, e, r)
	checkInvariants(t, r, map[NodeID]State{})
	mustState(t, r, later, StateActive)
	if _, err := r.State(old); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("State(node removed after the snapshot) = %v, want ErrUnknownNode", err)
	}
	_, err = r.Consume(admin, later, "http", 63)
	if d, ok := errors.AsType[*DeniedError](err); !ok || d.Used != 38 || d.TopConsumer != task {
		t.Errorf("Consume = %v, want a denial at 38 used with task as top consumer", err)
	}

	// A snapshot whose parts do not fit together is refused.
	broken := snapshotViaJSON(t, snap)
	broken.Sessions[0].Scope = 9999
	if _, err := Restore(Config{}, broken, replay(nil)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Restore from a broken snapshot = %v, want ErrCorrupt", err)
	}
	// Events that do not follow the snapshot are refused too.
	if _, err := Restore(Config{}, snap, replay(rec.events)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Restore with events from before the snapshot = %v, want ErrCorrupt", err)
	}
}

func TestRequestIDs(t *testing.T) {
	rec := &recorder{}
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Sink: rec, Root: Spec{Quotas: map[Resource]int64{"http": 100}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sid := mustSession(t, e, RootID, 0)
	other := mustSession(t, e, RootID, 0)
	ctx := context.Background()

	node, _, err := e.CreateNodeOnce(sid, "create", RootID, Spec{Name: "task"})
	if err != nil {
		t.Fatalf("CreateNodeOnce: %v", err)
	}
	if _, err := e.ConsumeOnce(sid, "charge", node, "http", 7); err != nil {
		t.Fatalf("ConsumeOnce: %v", err)
	}
	lease, _, err := e.Acquire(ctx, sid, node, "db", AcquireOptions{Request: "hold"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	mustRelease(t, e, sid, lease)
	snap := e.Snapshot()

	// The same requests repeated: on the engine, after a replay of its events, and after a snapshot.
	engines := map[string]*Engine{
		"same engine":   e,
		"after replay":  mustRestore(t, clock, rec.events),
		"from snapshot": mustRestoreFrom(t, clock, snap, rec.events),
	}
	for name, e := range engines {
		t.Run(name, func(t *testing.T) {
			events := e.Seq()
			if again, _, err := e.CreateNodeOnce(sid, "create", RootID, Spec{Name: "task"}); err != nil || again != node {
				t.Errorf("repeated CreateNodeOnce = %d, %v, want node %d", again, err, node)
			}
			if _, err := e.ConsumeOnce(sid, "charge", node, "http", 7); err != nil {
				t.Errorf("repeated ConsumeOnce: %v", err)
			}
			// The lease has been released, but the answer to the request is still the same lease.
			again, _, err := e.Acquire(ctx, sid, node, "db", AcquireOptions{Request: "hold"})
			if err != nil || again != lease {
				t.Errorf("repeated Acquire = %d, %v, want lease %d", again, err, lease)
			}
			if got := used(e, RootID, "http"); got != 7 || e.Seq() != events {
				t.Errorf("repeats left %d used and %d new events, want 7 and none", got, e.Seq()-events)
			}

			// The same id for a different request is a mistake by the caller.
			if _, err := e.ConsumeOnce(sid, "charge", node, "http", 8); !errors.Is(err, ErrInvalid) {
				t.Errorf("ConsumeOnce with a reused id = %v, want ErrInvalid", err)
			}
			if _, _, err := e.CreateNodeOnce(sid, "charge", RootID, Spec{}); !errors.Is(err, ErrInvalid) {
				t.Errorf("CreateNodeOnce with a reused id = %v, want ErrInvalid", err)
			}
			// Ids belong to a session, and a request without one is always carried out.
			if _, err := e.ConsumeOnce(other, "charge", node, "http", 1); err != nil {
				t.Errorf("ConsumeOnce by another session: %v", err)
			}
			if _, err := e.ConsumeOnce(sid, "", node, "http", 1); err != nil {
				t.Errorf("ConsumeOnce without an id: %v", err)
			}
			if got := used(e, RootID, "http"); got != 9 {
				t.Errorf("used = %d, want 9", got)
			}
		})
	}
}

func TestRequestIDsAreCapped(t *testing.T) {
	e, sid, _ := newEngine(t, Spec{})
	for i := range maxRequests + 1 {
		if _, err := e.ConsumeOnce(sid, fmt.Sprint("r", i), RootID, "http", 1); err != nil {
			t.Fatalf("ConsumeOnce: %v", err)
		}
	}
	// The oldest id has been forgotten, so repeating it is a new request; the newest is remembered.
	for _, id := range []string{"r0", fmt.Sprint("r", maxRequests)} {
		if _, err := e.ConsumeOnce(sid, id, RootID, "http", 1); err != nil {
			t.Fatalf("ConsumeOnce: %v", err)
		}
	}
	if got := used(e, RootID, "http"); got != maxRequests+2 {
		t.Errorf("used = %d, want %d", got, maxRequests+2)
	}
}

func TestRestoreWithoutEvents(t *testing.T) {
	rec := &recorder{}
	e, err := Restore(Config{Sink: rec, Root: Spec{Limits: map[Class]int{"db": 1}}}, nil, replay(nil))
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
			if _, err := Restore(Config{}, nil, replay(tc.events)); !errors.Is(err, ErrCorrupt) {
				t.Errorf("Restore = %v, want ErrCorrupt", err)
			}
		})
	}

	t.Run("read error", func(t *testing.T) {
		boom := errors.New("connection lost")
		events := func(yield func(Event, error) bool) {
			_ = yield(root, nil) && yield(Event{}, boom)
		}
		if _, err := Restore(Config{}, nil, events); !errors.Is(err, boom) {
			t.Errorf("Restore = %v, want the read error", err)
		}
	})
}
