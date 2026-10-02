package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recorder is a Sink that keeps every event it is given.
type recorder struct {
	events []Event
}

func (r *recorder) Emit(ev Event) {
	r.events = append(r.events, ev)
}

// grant is the outcome of one Acquire made by a helper goroutine.
type grant struct {
	node  NodeID
	lease LeaseID
	err   error
}

func newEngine(t *testing.T, root Spec) (*Engine, *ManualClock) {
	t.Helper()
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	e, err := New(Config{Clock: clock, Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e, clock
}

func mustNode(t *testing.T, e *Engine, parent NodeID, spec Spec) NodeID {
	t.Helper()
	id, _, err := e.CreateNode(parent, spec)
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	return id
}

func mustSession(t *testing.T, e *Engine, ttl time.Duration) SessionID {
	t.Helper()
	sid, _, err := e.OpenSession(ttl)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sid
}

func mustAcquire(t *testing.T, e *Engine, sid SessionID, id NodeID, class Class) LeaseID {
	t.Helper()
	l, _, err := e.Acquire(context.Background(), sid, id, class, AcquireOptions{})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	return l
}

func mustRelease(t *testing.T, e *Engine, sid SessionID, l LeaseID) {
	t.Helper()
	if _, err := e.Release(sid, l, Report{}); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func mustState(t *testing.T, e *Engine, id NodeID, want State) {
	t.Helper()
	got, err := e.State(id)
	if err != nil {
		t.Fatalf("State(%d): %v", id, err)
	}
	if got != want {
		t.Errorf("State(%d) = %s, want %s", id, got, want)
	}
}

// used reads a node's subtree usage straight from the engine.
func used(e *Engine, id NodeID, r Resource) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nodes[id].used[r]
}

// held reads how many leases of class a node's subtree holds.
func held(e *Engine, id NodeID, class Class) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nodes[id].held[class]
}

// queued counts the acquires waiting in every queue.
func queued(e *Engine) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, q := range e.queues {
		for _, tq := range q.ring {
			n += len(tq.waiters)
		}
	}
	return n
}

// waitQueued blocks until exactly n acquires are waiting.
func waitQueued(t *testing.T, e *Engine, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for queued(e) != n {
		if time.Now().After(deadline) {
			t.Fatalf("queued = %d, want %d", queued(e), n)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// acquireAsync queues n acquires at id in order and reports each on out.
func acquireAsync(t *testing.T, e *Engine, ctx context.Context, sid SessionID, id NodeID, class Class, n int, out chan<- grant) {
	t.Helper()
	for range n {
		before := queued(e)
		go func() {
			l, _, err := e.Acquire(ctx, sid, id, class, AcquireOptions{})
			out <- grant{node: id, lease: l, err: err}
		}()
		waitQueued(t, e, before+1)
	}
}

func TestConsumeDrawsFromAncestors(t *testing.T) {
	e, _ := newEngine(t, Spec{Quotas: map[Resource]int64{"http": 1000}})
	tenant := mustNode(t, e, RootID, Spec{Name: "tenant-a", Quotas: map[Resource]int64{"http": 100}})
	task := mustNode(t, e, tenant, Spec{Name: "task"})
	sub := mustNode(t, e, task, Spec{Name: "subtask", Quotas: map[Resource]int64{"http": 30}})
	crawler := mustNode(t, e, tenant, Spec{Name: "crawler"})

	if _, err := e.Consume(0, crawler, "http", 80); err != nil {
		t.Fatalf("Consume(crawler, 80): %v", err)
	}
	if _, err := e.Consume(0, sub, "http", 18); err != nil {
		t.Fatalf("Consume(sub, 18): %v", err)
	}

	_, err := e.Consume(0, sub, "http", 5)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Consume(sub, 5) = %v, want ErrDenied", err)
	}
	var d *DeniedError
	if !errors.As(err, &d) {
		t.Fatalf("Consume(sub, 5) = %T, want *DeniedError", err)
	}
	want := DeniedError{
		Node: tenant, Name: "tenant-a", Resource: "http",
		Used: 98, Limit: 100, Requested: 5,
		TopConsumer: crawler, TopConsumerName: "crawler", TopConsumerUsed: 80,
	}
	if *d != want {
		t.Errorf("denial = %+v, want %+v", *d, want)
	}

	// A denial changes nothing anywhere on the chain.
	for _, tc := range []struct {
		id   NodeID
		want int64
	}{{sub, 18}, {task, 18}, {tenant, 98}, {RootID, 98}} {
		if got := used(e, tc.id, "http"); got != tc.want {
			t.Errorf("used(%d) = %d, want %d", tc.id, got, tc.want)
		}
	}

	// The tenant has room for 2, so the subtask's own cap of 30 is not the limit.
	if _, err := e.Consume(0, sub, "http", 2); err != nil {
		t.Errorf("Consume(sub, 2): %v", err)
	}
}

func TestConsumeRejects(t *testing.T) {
	e, _ := newEngine(t, Spec{})
	node := mustNode(t, e, RootID, Spec{Quotas: map[Resource]int64{"sql": 0}})
	ended := mustNode(t, e, RootID, Spec{})
	if _, err := e.Cancel(ended); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	tests := []struct {
		name   string
		sid    SessionID
		id     NodeID
		amount int64
		want   error
	}{
		{"zero amount", 0, node, 0, ErrInvalid},
		{"negative amount", 0, node, -1, ErrInvalid},
		{"unknown node", 0, 9999, 1, ErrUnknownNode},
		{"ended node", 0, ended, 1, ErrClosed},
		{"unknown session", 9999, node, 1, ErrSessionExpired},
		{"limit of zero", 0, node, 1, ErrDenied},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := e.Consume(tc.sid, tc.id, "sql", tc.amount); !errors.Is(err, tc.want) {
				t.Errorf("Consume = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestConsumeConcurrent(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		amount  int64
		wantOK  int
	}{
		{"two workers of 80 into 100", 2, 80, 1},
		{"fifty workers of 1 into 100", 50, 1, 50},
		{"two hundred workers of 1 into 100", 200, 1, 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newEngine(t, Spec{})
			tenant := mustNode(t, e, RootID, Spec{Quotas: map[Resource]int64{"http": 100}})
			var (
				wg sync.WaitGroup
				mu sync.Mutex
				ok int
			)
			for range tc.workers {
				wg.Go(func() {
					task, _, err := e.CreateNode(tenant, Spec{})
					if err != nil {
						t.Errorf("CreateNode: %v", err)
						return
					}
					if _, err := e.Consume(0, task, "http", tc.amount); err == nil {
						mu.Lock()
						ok++
						mu.Unlock()
					}
				})
			}
			wg.Wait()
			if ok != tc.wantOK {
				t.Errorf("successful consumes = %d, want %d", ok, tc.wantOK)
			}
			if got := used(e, tenant, "http"); got != int64(tc.wantOK)*tc.amount {
				t.Errorf("used = %d, want %d", got, int64(tc.wantOK)*tc.amount)
			}
		})
	}
}

func TestEndNode(t *testing.T) {
	tests := []struct {
		name      string
		end       func(*Engine, NodeID) (uint64, error)
		wantState State
	}{
		{"cancel", (*Engine).Cancel, StateCancelled},
		{"close", (*Engine).Close, StateDone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newEngine(t, Spec{})
			tenant := mustNode(t, e, RootID, Spec{})
			task := mustNode(t, e, tenant, Spec{})
			sub := mustNode(t, e, task, Spec{})
			sibling := mustNode(t, e, tenant, Spec{})
			done := e.Done(sub)

			if _, err := tc.end(e, task); err != nil {
				t.Fatalf("end: %v", err)
			}
			mustState(t, e, task, tc.wantState)
			mustState(t, e, sub, StateCancelled)
			mustState(t, e, sibling, StateActive)
			mustState(t, e, tenant, StateActive)
			select {
			case <-done:
			default:
				t.Error("Done(sub) is still open")
			}

			// Ending again is a no-op and does not change the state.
			if _, err := e.Cancel(task); err != nil {
				t.Errorf("second end: %v", err)
			}
			mustState(t, e, task, tc.wantState)

			if _, err := e.Consume(0, sub, "http", 1); !errors.Is(err, ErrClosed) {
				t.Errorf("Consume on ended node = %v, want ErrClosed", err)
			}
			if _, _, err := e.CreateNode(task, Spec{}); !errors.Is(err, ErrClosed) {
				t.Errorf("CreateNode under ended node = %v, want ErrClosed", err)
			}
		})
	}
}

func TestEndNodeRejects(t *testing.T) {
	e, _ := newEngine(t, Spec{})
	if _, err := e.Cancel(RootID); !errors.Is(err, ErrInvalid) {
		t.Errorf("Cancel(root) = %v, want ErrInvalid", err)
	}
	if _, err := e.Close(9999); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("Close(unknown) = %v, want ErrUnknownNode", err)
	}
}

func TestCreateNodeRejects(t *testing.T) {
	e, _ := newEngine(t, Spec{})
	tests := []struct {
		name string
		spec Spec
	}{
		{"negative quota", Spec{Quotas: map[Resource]int64{"http": -1}}},
		{"negative limit", Spec{Limits: map[Class]int{"db": -1}}},
		{"empty resource", Spec{Quotas: map[Resource]int64{"": 1}}},
		{"empty class", Spec{Limits: map[Class]int{"": 1}}},
		{"negative weight", Spec{Weight: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := e.CreateNode(RootID, tc.spec); !errors.Is(err, ErrInvalid) {
				t.Errorf("CreateNode = %v, want ErrInvalid", err)
			}
		})
	}

	t.Run("deeper than MaxDepth", func(t *testing.T) {
		id := RootID
		for range MaxDepth {
			id = mustNode(t, e, id, Spec{})
		}
		if _, _, err := e.CreateNode(id, Spec{}); !errors.Is(err, ErrInvalid) {
			t.Errorf("CreateNode = %v, want ErrInvalid", err)
		}
	})
}

func TestSpecIsCopied(t *testing.T) {
	e, _ := newEngine(t, Spec{})
	quotas := map[Resource]int64{"http": 1}
	node := mustNode(t, e, RootID, Spec{Quotas: quotas})
	quotas["http"] = 1000
	if _, err := e.Consume(0, node, "http", 2); !errors.Is(err, ErrDenied) {
		t.Errorf("Consume = %v, want ErrDenied", err)
	}
}

func TestDeadlineIsInherited(t *testing.T) {
	e, clock := newEngine(t, Spec{})
	now := clock.Now()
	parent := mustNode(t, e, RootID, Spec{Deadline: now.Add(10 * time.Second)})
	later := mustNode(t, e, parent, Spec{Deadline: now.Add(time.Hour)})
	sooner := mustNode(t, e, parent, Spec{Deadline: now.Add(5 * time.Second)})
	none := mustNode(t, e, parent, Spec{})

	clock.Advance(5 * time.Second)
	mustState(t, e, sooner, StateDeadlineExceeded)
	mustState(t, e, later, StateActive)
	mustState(t, e, parent, StateActive)

	// Touching a child after the parent's deadline ends the parent, not the child.
	clock.Advance(5 * time.Second)
	if _, err := e.Consume(0, later, "http", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Consume after deadline = %v, want ErrClosed", err)
	}
	mustState(t, e, parent, StateDeadlineExceeded)
	mustState(t, e, later, StateCancelled)
	mustState(t, e, none, StateCancelled)
	mustState(t, e, sooner, StateDeadlineExceeded)
}

func TestReapAppliesDeadlines(t *testing.T) {
	e, clock := newEngine(t, Spec{})
	node := mustNode(t, e, RootID, Spec{Deadline: clock.Now().Add(time.Second)})
	done := e.Done(node)
	clock.Advance(time.Second)
	e.Reap()
	select {
	case <-done:
	default:
		t.Error("Done is still open after Reap")
	}
}

func TestNewRejectsRootDeadline(t *testing.T) {
	_, err := New(Config{Root: Spec{Deadline: time.Now().Add(time.Hour)}})
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("New = %v, want ErrInvalid", err)
	}
}

func TestAcquireHonoursWholeChain(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 20}})
	tenant := mustNode(t, e, RootID, Spec{Limits: map[Class]int{"db": 10}})
	task := mustNode(t, e, tenant, Spec{})
	other := mustNode(t, e, RootID, Spec{})
	sid := mustSession(t, e, 0)

	var leases []LeaseID
	for range 10 {
		leases = append(leases, mustAcquire(t, e, sid, task, "db"))
	}
	out := make(chan grant, 1)
	acquireAsync(t, e, context.Background(), sid, task, "db", 1, out)

	// The tenant is full at 10, but the root still has room for another tenant.
	for range 10 {
		mustAcquire(t, e, sid, other, "db")
	}
	if got := held(e, RootID, "db"); got != 20 {
		t.Errorf("held(root) = %d, want 20", got)
	}

	mustRelease(t, e, sid, leases[0])
	// One release frees a slot in both the tenant and the root.
	waitQueued(t, e, 0)
	if g := <-out; g.err != nil {
		t.Errorf("queued Acquire: %v", g.err)
	}
	if got := held(e, tenant, "db"); got != 10 {
		t.Errorf("held(tenant) = %d, want 10", got)
	}
}

func TestRelease(t *testing.T) {
	const ttl = 10 * time.Second
	tests := []struct {
		name string
		// arrange changes the lease's fate and returns the session to release with.
		arrange func(e *Engine, clock *ManualClock, node NodeID, owner SessionID, l LeaseID) SessionID
		want    error
	}{
		{
			name:    "held by this session",
			arrange: func(_ *Engine, _ *ManualClock, _ NodeID, owner SessionID, _ LeaseID) SessionID { return owner },
		},
		{
			name: "held by another session",
			arrange: func(e *Engine, _ *ManualClock, _ NodeID, _ SessionID, _ LeaseID) SessionID {
				sid, _, _ := e.OpenSession(ttl)
				return sid
			},
			want: ErrNotOwner,
		},
		{
			name: "already released",
			arrange: func(e *Engine, _ *ManualClock, _ NodeID, owner SessionID, l LeaseID) SessionID {
				e.Release(owner, l, Report{})
				return owner
			},
		},
		{
			name: "session expired",
			arrange: func(_ *Engine, clock *ManualClock, _ NodeID, owner SessionID, _ LeaseID) SessionID {
				clock.Advance(ttl)
				return owner
			},
			want: ErrLeaseExpired,
		},
		{
			name: "node cancelled",
			arrange: func(e *Engine, _ *ManualClock, node NodeID, owner SessionID, _ LeaseID) SessionID {
				e.Cancel(node)
				return owner
			},
			want: ErrLeaseRevoked,
		},
		{
			name: "forgotten after retention",
			arrange: func(e *Engine, clock *ManualClock, _ NodeID, owner SessionID, l LeaseID) SessionID {
				e.Release(owner, l, Report{})
				sid, _, _ := e.OpenSession(0)
				clock.Advance(2 * time.Minute)
				e.Reap()
				return sid
			},
			want: ErrUnknownLease,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, clock := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
			node := mustNode(t, e, RootID, Spec{})
			owner := mustSession(t, e, ttl)
			l := mustAcquire(t, e, owner, node, "db")

			sid := tc.arrange(e, clock, node, owner, l)
			if _, err := e.Release(sid, l, Report{}); !errors.Is(err, tc.want) {
				t.Errorf("Release = %v, want %v", err, tc.want)
			}
			if tc.want != ErrNotOwner {
				if got := held(e, RootID, "db"); got != 0 {
					t.Errorf("held(root) = %d, want 0", got)
				}
			}
		})
	}

	t.Run("never issued", func(t *testing.T) {
		e, _ := newEngine(t, Spec{})
		sid := mustSession(t, e, ttl)
		if _, err := e.Release(sid, 9999, Report{}); !errors.Is(err, ErrUnknownLease) {
			t.Errorf("Release = %v, want ErrUnknownLease", err)
		}
	})
}

func TestReleaseReportsToObserver(t *testing.T) {
	var got []Report
	e, err := New(Config{Observer: func(_ Class, r Report) { got = append(got, r) }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sid := mustSession(t, e, 0)
	l := mustAcquire(t, e, sid, RootID, "db")
	want := Report{Latency: 40 * time.Millisecond, Overloaded: true}
	for range 2 {
		if _, err := e.Release(sid, l, want); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}
	if len(got) != 1 || got[0] != want {
		t.Errorf("observer saw %v, want one %v", got, want)
	}
}

func TestDeadWorkerLeasesAreReclaimed(t *testing.T) {
	e, clock := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	node := mustNode(t, e, RootID, Spec{})
	dead := mustSession(t, e, 10*time.Second)
	live := mustSession(t, e, 0)
	stale := mustAcquire(t, e, dead, node, "db")

	out := make(chan grant, 1)
	acquireAsync(t, e, context.Background(), live, node, "db", 1, out)

	clock.Advance(10 * time.Second)
	e.Reap()
	g := <-out
	if g.err != nil {
		t.Fatalf("queued Acquire: %v", g.err)
	}
	if g.lease <= stale {
		t.Errorf("new lease %d is not greater than the stale lease %d", g.lease, stale)
	}

	// Everything the dead worker tries afterwards is rejected.
	if err := e.Validate(stale); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("Validate(stale) = %v, want ErrLeaseExpired", err)
	}
	if _, err := e.Heartbeat(dead); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("Heartbeat = %v, want ErrSessionExpired", err)
	}
	if _, err := e.Consume(dead, node, "http", 1); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("Consume = %v, want ErrSessionExpired", err)
	}
	if _, _, err := e.Acquire(context.Background(), dead, node, "db", AcquireOptions{}); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("Acquire = %v, want ErrSessionExpired", err)
	}
	if err := e.Validate(g.lease); err != nil {
		t.Errorf("Validate(new) = %v", err)
	}
}

func TestSessionExpiresWithoutReaper(t *testing.T) {
	e, clock := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	sid := mustSession(t, e, 10*time.Second)
	mustAcquire(t, e, sid, RootID, "db")
	clock.Advance(10 * time.Second)
	if _, err := e.Heartbeat(sid); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("Heartbeat = %v, want ErrSessionExpired", err)
	}
	if got := held(e, RootID, "db"); got != 0 {
		t.Errorf("held(root) = %d, want 0", got)
	}
}

func TestHeartbeatKeepsLeases(t *testing.T) {
	e, clock := newEngine(t, Spec{})
	sid := mustSession(t, e, 10*time.Second)
	l := mustAcquire(t, e, sid, RootID, "db")
	for range 5 {
		clock.Advance(9 * time.Second)
		if _, err := e.Heartbeat(sid); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
		e.Reap()
	}
	if err := e.Validate(l); err != nil {
		t.Errorf("Validate = %v", err)
	}
}

func TestMaxHoldExpiresLease(t *testing.T) {
	e, clock := newEngine(t, Spec{Limits: map[Class]int{"tool": 1}})
	sid := mustSession(t, e, 0)
	l, _, err := e.Acquire(context.Background(), sid, RootID, "tool", AcquireOptions{MaxHold: time.Minute})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	out := make(chan grant, 1)
	acquireAsync(t, e, context.Background(), sid, RootID, "tool", 1, out)

	clock.Advance(59 * time.Second)
	e.Reap()
	waitQueued(t, e, 1)

	clock.Advance(time.Second)
	e.Reap()
	if g := <-out; g.err != nil {
		t.Fatalf("queued Acquire: %v", g.err)
	}
	if _, err := e.Release(sid, l, Report{}); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("Release = %v, want ErrLeaseExpired", err)
	}
	if got := held(e, RootID, "tool"); got != 1 {
		t.Errorf("held(root) = %d, want 1", got)
	}
}

func TestAcquireContextCancelled(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	sid := mustSession(t, e, 0)
	l := mustAcquire(t, e, sid, RootID, "db")

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan grant, 1)
	acquireAsync(t, e, ctx, sid, RootID, "db", 1, out)
	cancel()
	if g := <-out; !errors.Is(g.err, context.Canceled) {
		t.Fatalf("Acquire = %v, want context.Canceled", g.err)
	}
	waitQueued(t, e, 0)

	// The cancelled waiter must not take the slot when it frees up.
	mustRelease(t, e, sid, l)
	if got := held(e, RootID, "db"); got != 0 {
		t.Errorf("held(root) = %d, want 0", got)
	}

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, _, err := e.Acquire(cancelled, sid, RootID, "db", AcquireOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Acquire with cancelled context = %v, want context.Canceled", err)
	}
}

func TestEndingNodeFailsWaiters(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	tenant := mustNode(t, e, RootID, Spec{})
	task := mustNode(t, e, tenant, Spec{})
	sid := mustSession(t, e, 0)
	mustAcquire(t, e, sid, RootID, "db")

	out := make(chan grant, 2)
	acquireAsync(t, e, context.Background(), sid, task, "db", 2, out)
	if _, err := e.Cancel(tenant); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	for range 2 {
		if g := <-out; !errors.Is(g.err, ErrClosed) {
			t.Errorf("queued Acquire = %v, want ErrClosed", g.err)
		}
	}
	waitQueued(t, e, 0)
}

func TestClosingSessionFailsWaiters(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	holder := mustSession(t, e, 0)
	waiting := mustSession(t, e, 0)
	mustAcquire(t, e, holder, RootID, "db")

	out := make(chan grant, 1)
	acquireAsync(t, e, context.Background(), waiting, RootID, "db", 1, out)
	if _, err := e.CloseSession(waiting); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if g := <-out; !errors.Is(g.err, ErrSessionExpired) {
		t.Errorf("queued Acquire = %v, want ErrSessionExpired", g.err)
	}
}

func TestFairnessAcrossTenants(t *testing.T) {
	tests := []struct {
		name             string
		weightA, weightB int
		queuedA, queuedB int
		window           int // how many of the first grants to look at
		wantA            int // how many of those must go to tenant A
	}{
		{"equal weights alternate", 1, 1, 40, 40, 40, 20},
		{"weights of three to one", 3, 1, 60, 20, 40, 30},
		{"noisy tenant cannot starve a quiet one", 1, 1, 100, 2, 4, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
			a := mustNode(t, e, RootID, Spec{Name: "a", Weight: tc.weightA})
			b := mustNode(t, e, RootID, Spec{Name: "b", Weight: tc.weightB})
			sid := mustSession(t, e, 0)
			blocker := mustAcquire(t, e, sid, RootID, "db")

			total := tc.queuedA + tc.queuedB
			out := make(chan grant, total)
			acquireAsync(t, e, context.Background(), sid, a, "db", tc.queuedA, out)
			acquireAsync(t, e, context.Background(), sid, b, "db", tc.queuedB, out)

			// With one slot, each release hands it to exactly one waiter.
			mustRelease(t, e, sid, blocker)
			gotA := 0
			for i := range total {
				g := <-out
				if g.err != nil {
					t.Fatalf("queued Acquire: %v", g.err)
				}
				if i < tc.window && g.node == a {
					gotA++
				}
				mustRelease(t, e, sid, g.lease)
			}
			if gotA != tc.wantA {
				t.Errorf("tenant a got %d of the first %d grants, want %d", gotA, tc.window, tc.wantA)
			}
		})
	}
}

func TestPriorityWithinTenant(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	tenant := mustNode(t, e, RootID, Spec{})
	low := mustNode(t, e, tenant, Spec{Priority: 0})
	high := mustNode(t, e, tenant, Spec{Priority: 5})
	// The highest priority task cannot run at all, and must not block the others.
	capped := mustNode(t, e, tenant, Spec{Priority: 9, Limits: map[Class]int{"db": 0}})
	sid := mustSession(t, e, 0)
	blocker := mustAcquire(t, e, sid, RootID, "db")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan grant, 5)
	acquireAsync(t, e, ctx, sid, capped, "db", 1, out)
	acquireAsync(t, e, ctx, sid, low, "db", 2, out)
	acquireAsync(t, e, ctx, sid, high, "db", 2, out)

	mustRelease(t, e, sid, blocker)
	for i, want := range []NodeID{high, high, low, low} {
		g := <-out
		if g.err != nil {
			t.Fatalf("queued Acquire: %v", g.err)
		}
		if g.node != want {
			t.Errorf("grant %d went to node %d, want %d", i, g.node, want)
		}
		mustRelease(t, e, sid, g.lease)
	}
	waitQueued(t, e, 1)
}

func TestLoweringLimitRevokesNothing(t *testing.T) {
	e, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 4}})
	sid := mustSession(t, e, 0)
	var leases []LeaseID
	for range 4 {
		leases = append(leases, mustAcquire(t, e, sid, RootID, "db"))
	}
	if _, err := e.SetLimit(RootID, "db", 2); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	for _, l := range leases {
		if err := e.Validate(l); err != nil {
			t.Errorf("Validate(%d) = %v", l, err)
		}
	}

	out := make(chan grant, 1)
	acquireAsync(t, e, context.Background(), sid, RootID, "db", 1, out)
	// Two releases bring held down to the new limit, which is still full.
	mustRelease(t, e, sid, leases[0])
	mustRelease(t, e, sid, leases[1])
	waitQueued(t, e, 1)
	mustRelease(t, e, sid, leases[2])
	if g := <-out; g.err != nil {
		t.Errorf("queued Acquire: %v", g.err)
	}

	// Raising the limit lets waiters in without any release.
	acquireAsync(t, e, context.Background(), sid, RootID, "db", 1, out)
	if _, err := e.SetLimit(RootID, "db", 3); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	if g := <-out; g.err != nil {
		t.Errorf("queued Acquire after raise: %v", g.err)
	}
}

func TestEventsCarryIncreasingSeq(t *testing.T) {
	rec := &recorder{}
	e, err := New(Config{Sink: rec, Root: Spec{Limits: map[Class]int{"db": 1}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	node, seqNode, err := e.CreateNode(RootID, Spec{})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	sid, seqSession, _ := e.OpenSession(time.Minute)
	seqConsume, _ := e.Consume(sid, node, "http", 3)
	l, seqLease, _ := e.Acquire(context.Background(), sid, node, "db", AcquireOptions{})
	seqRelease, _ := e.Release(sid, l, Report{})
	seqCancel, _ := e.Cancel(node)

	wantKinds := []EventKind{
		EventNodeCreated, EventNodeCreated, EventSessionOpened, EventConsumed,
		EventLeaseGranted, EventLeaseEnded, EventNodeEnded,
	}
	if len(rec.events) != len(wantKinds) {
		t.Fatalf("got %d events, want %d", len(rec.events), len(wantKinds))
	}
	for i, ev := range rec.events {
		if ev.Kind != wantKinds[i] {
			t.Errorf("event %d is %s, want %s", i, ev.Kind, wantKinds[i])
		}
		if ev.Seq != uint64(i+1) {
			t.Errorf("event %d has seq %d, want %d", i, ev.Seq, i+1)
		}
	}
	// Each call returns the seq of the event it caused.
	gotSeqs := []uint64{seqNode, seqSession, seqConsume, seqLease, seqRelease, seqCancel}
	for i, got := range gotSeqs {
		if want := uint64(i + 2); got != want {
			t.Errorf("call %d returned seq %d, want %d", i, got, want)
		}
	}
}
