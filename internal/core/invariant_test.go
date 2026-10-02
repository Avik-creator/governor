package core

import (
	"context"
	"errors"
	"maps"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

// auditor is a Sink that checks the properties only visible as events happen.
type auditor struct {
	t         *testing.T
	e         *Engine
	seq       uint64
	lastLease LeaseID
	ends      map[LeaseID]int // how many times each granted lease has ended
}

// Emit runs under the engine lock, so it may read engine state directly.
func (a *auditor) Emit(ev Event) {
	if ev.Seq != a.seq+1 {
		a.t.Errorf("event seq %d follows %d", ev.Seq, a.seq)
	}
	a.seq = ev.Seq
	switch ev.Kind {
	case EventLeaseGranted:
		if ev.Lease <= a.lastLease {
			a.t.Errorf("fencing: lease %d granted after lease %d", ev.Lease, a.lastLease)
		}
		a.lastLease = ev.Lease
		a.ends[ev.Lease] = 0
		// I4: a grant never takes any node on the chain over its limit.
		for n := a.e.nodes[ev.Node]; n != nil; n = n.parent {
			if limit, ok := n.limits[ev.Class]; ok && n.held[ev.Class] > limit {
				a.t.Errorf("I4: grant put node %d at %d of %d %s", n.id, n.held[ev.Class], limit, ev.Class)
			}
		}
	case EventLeaseEnded:
		// I5: a lease ends exactly once, and only after it was granted.
		n, ok := a.ends[ev.Lease]
		if !ok || n != 0 {
			a.t.Errorf("I5: lease %d ended %d times before this one (granted=%t)", ev.Lease, n, ok)
		}
		a.ends[ev.Lease] = n + 1
	}
}

// checkInvariants recomputes the invariants of SPEC.md §5 from scratch.
func checkInvariants(t *testing.T, e *Engine, terminal map[NodeID]State) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, n := range e.nodes {
		// I1: usage never exceeds a cap.
		for r, limit := range n.quota {
			if n.used[r] > limit {
				t.Fatalf("I1: node %d used %d of %d %s", n.id, n.used[r], limit, r)
			}
		}

		// I2: a node's usage is its own plus its children's.
		sumUsed := maps.Clone(n.self)
		heldSum := make(map[Class]int)
		for _, l := range n.leases {
			heldSum[l.class]++
		}
		for _, c := range n.children {
			for r, u := range c.used {
				sumUsed[r] += u
				if top := n.top[r]; top == nil || top.parent != n || top.used[r] < u {
					t.Fatalf("top consumer of node %d for %s is not its largest child", n.id, r)
				}
			}
			for class, h := range c.held {
				heldSum[class] += h
			}
		}
		for r := range union(sumUsed, n.used) {
			if sumUsed[r] != n.used[r] {
				t.Fatalf("I2: node %d has used %d %s, parts add up to %d", n.id, n.used[r], r, sumUsed[r])
			}
		}

		// I3: a node's held count is its own leases plus its children's.
		for class := range union(heldSum, n.held) {
			if heldSum[class] != n.held[class] {
				t.Fatalf("I3: node %d holds %d %s, parts add up to %d", n.id, n.held[class], class, heldSum[class])
			}
		}

		// I7: a terminal state never changes, and nothing under it is active.
		if was, ok := terminal[n.id]; ok && was != n.state {
			t.Fatalf("I7: node %d went from %s to %s", n.id, was, n.state)
		}
		if n.state.Terminal() {
			terminal[n.id] = n.state
			if len(n.leases) != 0 {
				t.Fatalf("I6: ended node %d still has %d leases", n.id, len(n.leases))
			}
			for _, c := range n.children {
				if !c.state.Terminal() {
					t.Fatalf("I7: node %d is active under ended node %d", c.id, n.id)
				}
			}
		}
	}

	// I6: every live lease belongs to a live session and an active node.
	for id, l := range e.leases {
		if l.n.state != StateActive || e.sessions[l.s.id] != l.s {
			t.Fatalf("I6: lease %d is orphaned", id)
		}
		if l.n.leases[id] != l || l.s.leases[id] != l {
			t.Fatalf("I6: lease %d is missing from its node or session", id)
		}
		// I9: a lease is only ever held inside its session's scope.
		if !l.s.covers(l.n) {
			t.Fatalf("I9: lease %d at node %d is outside the scope of session %d", id, l.n.id, l.s.id)
		}
	}

	// I8: no queued acquire fits, and the queues hold no dead entries.
	for class, q := range e.queues {
		if len(q.ring) == 0 || q.cursor < 0 || q.cursor >= len(q.ring) {
			t.Fatalf("queue %s has %d tenants and cursor %d", class, len(q.ring), q.cursor)
		}
		for _, tq := range q.ring {
			if len(tq.waiters) == 0 {
				t.Fatalf("queue %s keeps an empty tenant %d", class, tq.tenant.id)
			}
			for i, w := range tq.waiters {
				switch {
				case w.settled:
					t.Fatalf("queue %s keeps a settled waiter", class)
				case w.n.state != StateActive || e.sessions[w.s.id] != w.s:
					t.Fatalf("queue %s keeps a waiter of an ended node or session", class)
				case !w.s.covers(w.n):
					t.Fatalf("I9: queue %s keeps a waiter outside its session's scope", class)
				case w.n.fits(class):
					t.Fatalf("I8: waiter at node %d fits %s but is queued", w.n.id, class)
				case i > 0 && tq.waiters[i-1].n.priority < w.n.priority:
					t.Fatalf("queue %s is not ordered by priority", class)
				}
			}
		}
	}
}

// union returns the keys present in either map.
func union[K comparable, V any](a, b map[K]V) map[K]struct{} {
	out := make(map[K]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

// fuzzer drives an engine with random operations and keeps its own quota model.
type fuzzer struct {
	t     *testing.T
	rng   *rand.Rand
	e     *Engine
	clock *ManualClock
	audit *auditor

	nodes    []NodeID
	parent   map[NodeID]NodeID
	consumed map[NodeID]map[Resource]int64 // successful consumes, by node
	admin    SessionID                     // scoped to the root, never expires or closes
	sessions []SessionID
	scope    map[SessionID]NodeID
	cancels  []func() // each cancels one queued acquire and waits for it to return
	terminal map[NodeID]State
	wg       sync.WaitGroup
}

var (
	fuzzResources = []Resource{"http", "sql"}
	fuzzClasses   = []Class{"db", "api"}
)

func newFuzzer(t *testing.T, seed int64) *fuzzer {
	t.Helper()
	f := &fuzzer{
		t:        t,
		rng:      rand.New(rand.NewSource(seed)),
		clock:    NewManualClock(time.Unix(1_700_000_000, 0)),
		audit:    &auditor{t: t, ends: make(map[LeaseID]int)},
		nodes:    []NodeID{RootID},
		parent:   make(map[NodeID]NodeID),
		consumed: make(map[NodeID]map[Resource]int64),
		scope:    make(map[SessionID]NodeID),
		terminal: make(map[NodeID]State),
	}
	e, err := New(Config{
		Clock: f.clock,
		Sink:  f.audit,
		Root: Spec{
			Quotas: map[Resource]int64{"http": 600},
			Limits: map[Class]int{"db": 4, "api": 6},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.e, f.audit.e = e, e
	f.admin = mustSession(t, e, RootID, 0)
	f.scope[f.admin] = RootID
	return f
}

func (f *fuzzer) node() NodeID {
	return f.nodes[f.rng.Intn(len(f.nodes))]
}

// session returns a random session, or zero if none has been opened.
func (f *fuzzer) session() SessionID {
	if len(f.sessions) == 0 {
		return 0
	}
	return f.sessions[f.rng.Intn(len(f.sessions))]
}

// actor returns a random session, falling back to the admin session.
func (f *fuzzer) actor() SessionID {
	if sid := f.session(); sid != 0 && f.rng.Intn(3) != 0 {
		return sid
	}
	return f.admin
}

// inScope is the model's answer to whether sid may act on id.
func (f *fuzzer) inScope(sid SessionID, id NodeID) bool {
	scope, ok := f.scope[sid]
	for a := id; ok && a != 0; a = f.parent[a] {
		if a == scope {
			return true
		}
	}
	return false
}

// target picks a node for sid to act on, usually one inside its scope.
func (f *fuzzer) target(sid SessionID) NodeID {
	if f.rng.Intn(4) == 0 {
		return f.node()
	}
	var inside []NodeID
	for _, id := range f.nodes {
		if f.inScope(sid, id) {
			inside = append(inside, id)
		}
	}
	if len(inside) == 0 {
		return f.node()
	}
	return inside[f.rng.Intn(len(inside))]
}

// checkScope checks I9 on the outcome of a call that sid made on id.
func (f *fuzzer) checkScope(sid SessionID, id NodeID, before uint64, err error) {
	f.t.Helper()
	inside, forbidden := f.inScope(sid, id), errors.Is(err, ErrForbidden)
	switch {
	case forbidden && inside:
		f.t.Fatalf("session %d was forbidden from node %d inside its scope", sid, id)
	case forbidden && f.audit.seq != before:
		f.t.Fatalf("I9: forbidden call by session %d on node %d changed state", sid, id)
	case !inside && !forbidden && !errors.Is(err, ErrSessionExpired):
		f.t.Fatalf("I9: session %d acted on node %d outside its scope: %v", sid, id, err)
	}
}

// openSession opens a session scoped to the root or to a random node.
func (f *fuzzer) openSession() {
	scope := RootID
	if f.rng.Intn(2) == 0 {
		scope = f.node()
	}
	ttl := time.Duration(f.rng.Intn(4)) * 5 * time.Second
	if sid, _, err := f.e.OpenSession(scope, ttl); err == nil {
		f.sessions = append(f.sessions, sid)
		f.scope[sid] = scope
	}
}

// endNode cancels or closes a random node as a random session.
func (f *fuzzer) endNode(end func(*Engine, SessionID, NodeID) (uint64, error)) {
	sid := f.actor()
	id, before := f.target(sid), f.audit.seq
	_, err := end(f.e, sid, id)
	f.checkScope(sid, id, before, err)
}

// step performs one random operation.
func (f *fuzzer) step() {
	switch op := f.rng.Intn(100); {
	case op < 12:
		f.createNode()
	case op < 37:
		f.consume()
	case op < 42:
		f.openSession()
	case op < 67:
		f.acquire()
	case op < 82:
		f.release()
	case op < 85:
		f.endNode((*Engine).Cancel)
	case op < 87:
		f.endNode((*Engine).Close)
	case op < 88:
		f.e.CloseSession(f.session())
	case op < 91:
		f.e.Heartbeat(f.session())
	case op < 94:
		f.e.SetLimit(f.node(), fuzzClasses[f.rng.Intn(2)], f.rng.Intn(7))
	case op < 96:
		if len(f.cancels) > 0 {
			f.cancels[f.rng.Intn(len(f.cancels))]()
		}
	case op < 97:
		f.e.Validate(LeaseID(f.rng.Intn(int(f.e.ids) + 2)))
	default:
		f.clock.Advance(time.Duration(f.rng.Intn(4)) * time.Second)
		if f.rng.Intn(2) == 0 {
			f.e.Reap()
		}
	}
}

func (f *fuzzer) createNode() {
	if len(f.nodes) >= 150 {
		return
	}
	spec := Spec{Weight: f.rng.Intn(4), Priority: f.rng.Intn(3)}
	if f.rng.Intn(2) == 0 {
		spec.Quotas = map[Resource]int64{fuzzResources[f.rng.Intn(2)]: int64(f.rng.Intn(60))}
	}
	if f.rng.Intn(2) == 0 {
		spec.Limits = map[Class]int{fuzzClasses[f.rng.Intn(2)]: f.rng.Intn(4)}
	}
	if f.rng.Intn(4) == 0 {
		spec.Deadline = f.clock.Now().Add(time.Duration(f.rng.Intn(40)) * time.Second)
	}
	sid := f.actor()
	parent, before := f.target(sid), f.audit.seq
	id, _, err := f.e.CreateNode(sid, parent, spec)
	f.checkScope(sid, parent, before, err)
	if err == nil {
		f.nodes = append(f.nodes, id)
		f.parent[id] = parent
	}
}

// consume checks every outcome against the fuzzer's own record of usage.
func (f *fuzzer) consume() {
	sid := f.actor()
	id, r := f.target(sid), fuzzResources[f.rng.Intn(2)]
	amount, before := int64(1+f.rng.Intn(8)), f.audit.seq
	_, err := f.e.Consume(sid, id, r, amount)
	f.checkScope(sid, id, before, err)
	var d *DeniedError
	switch {
	case err == nil:
		if f.consumed[id] == nil {
			f.consumed[id] = make(map[Resource]int64)
		}
		f.consumed[id][r] += amount
	case errors.As(err, &d):
		if want := f.expected(r)[d.Node]; d.Used != want {
			f.t.Fatalf("denial reports %d used at node %d, model says %d", d.Used, d.Node, want)
		}
		if d.Used+d.Requested <= d.Limit {
			f.t.Fatalf("unjustified denial: %v", err)
		}
	}
}

// expected derives every node's usage of r from the log of successful consumes.
func (f *fuzzer) expected(r Resource) map[NodeID]int64 {
	want := make(map[NodeID]int64)
	for id, byResource := range f.consumed {
		for a := id; a != 0; a = f.parent[a] {
			want[a] += byResource[r]
		}
	}
	return want
}

// acquire starts an Acquire and returns once it has been granted, failed or queued.
func (f *fuzzer) acquire() {
	sid := f.actor()
	id, class := f.target(sid), fuzzClasses[f.rng.Intn(2)]
	opts := AcquireOptions{MaxHold: time.Duration(f.rng.Intn(3)) * 6 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	// Apply any due expiry first, so the Acquire itself cannot shrink the queue.
	f.e.Heartbeat(sid)
	f.e.State(id)
	before, seq := queued(f.e), f.audit.seq
	var err error
	returned := make(chan struct{})
	f.wg.Go(func() {
		defer close(returned)
		_, _, err = f.e.Acquire(ctx, sid, id, class, opts)
	})
	for {
		select {
		case <-returned:
			cancel()
			f.checkScope(sid, id, seq, err)
			return
		default:
		}
		if queued(f.e) > before {
			if !f.inScope(sid, id) {
				f.t.Fatalf("I9: session %d queued at node %d outside its scope", sid, id)
			}
			f.cancels = append(f.cancels, func() {
				cancel()
				<-returned
			})
			return
		}
		runtime.Gosched()
	}
}

// release releases a random live lease, sometimes with the wrong session.
func (f *fuzzer) release() {
	f.e.mu.Lock()
	ids := make([]LeaseID, 0, len(f.e.leases))
	for id := range f.e.leases {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var (
		id    LeaseID
		owner SessionID
	)
	if len(ids) > 0 {
		id = ids[f.rng.Intn(len(ids))]
		owner = f.e.leases[id].s.id
	}
	f.e.mu.Unlock()
	if id == 0 {
		return
	}
	if f.rng.Intn(10) == 0 {
		owner = f.session()
	}
	f.e.Release(owner, id, Report{})
}

// check verifies the invariants and the quota model after a step.
func (f *fuzzer) check() {
	f.t.Helper()
	checkInvariants(f.t, f.e, f.terminal)
	for _, r := range fuzzResources {
		want := f.expected(r)
		for _, id := range f.nodes {
			if got := used(f.e, id, r); got != want[id] {
				f.t.Fatalf("node %d has used %d %s, model says %d", id, got, r, want[id])
			}
		}
	}
}

// drain ends every session and checks that nothing is left held or queued.
func (f *fuzzer) drain() {
	f.t.Helper()
	for _, cancel := range f.cancels {
		cancel()
	}
	for _, sid := range append(f.sessions, f.admin) {
		f.e.CloseSession(sid)
	}
	f.wg.Wait()
	// A grant can race with a cancelled context and is then handed back.
	f.check()

	f.e.mu.Lock()
	defer f.e.mu.Unlock()
	if len(f.e.leases) != 0 || len(f.e.queues) != 0 || len(f.e.sessions) != 0 {
		f.t.Fatalf("after drain: %d leases, %d queues, %d sessions",
			len(f.e.leases), len(f.e.queues), len(f.e.sessions))
	}
	for _, n := range f.e.nodes {
		for class, h := range n.held {
			if h != 0 {
				f.t.Fatalf("after drain: node %d still holds %d %s", n.id, h, class)
			}
		}
	}
	for id, n := range f.audit.ends {
		if n != 1 {
			f.t.Fatalf("I5: lease %d ended %d times", id, n)
		}
	}
}

func TestRandomOperations(t *testing.T) {
	seeds, steps := 40, 1500
	if testing.Short() {
		seeds, steps = 5, 500
	}
	for seed := range int64(seeds) {
		f := newFuzzer(t, seed)
		for i := range steps {
			f.step()
			f.check()
			if t.Failed() {
				t.Fatalf("seed %d failed at step %d", seed, i)
			}
		}
		f.drain()
		if t.Failed() {
			t.Fatalf("seed %d failed while draining", seed)
		}
	}
}

// benchEngine returns an engine and a task under a tenant, reaped in the background.
func benchEngine(b *testing.B) (*Engine, NodeID) {
	b.Helper()
	e, err := New(Config{
		Retention: time.Millisecond,
		Root: Spec{
			Quotas: map[Resource]int64{"http": 1 << 62},
			Limits: map[Class]int{"db": 1 << 30, "pool": 2},
		},
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	go e.Run(ctx, 10*time.Millisecond)
	admin, _, _ := e.OpenSession(RootID, 0)
	tenant, _, _ := e.CreateNode(admin, RootID, Spec{Quotas: map[Resource]int64{"http": 1 << 61}})
	task, _, _ := e.CreateNode(admin, tenant, Spec{})
	return e, task
}

func BenchmarkConsume(b *testing.B) {
	e, task := benchEngine(b)
	b.RunParallel(func(pb *testing.PB) {
		sid, _, _ := e.OpenSession(task, 0)
		sub, _, _ := e.CreateNode(sid, task, Spec{})
		for pb.Next() {
			if _, err := e.Consume(sid, sub, "http", 1); err != nil {
				b.Errorf("Consume: %v", err)
				return
			}
		}
	})
}

func BenchmarkAcquireRelease(b *testing.B) {
	tests := []struct {
		name  string
		class Class
	}{
		{"uncontended", "db"},
		{"queued behind 2 slots", "pool"},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			e, task := benchEngine(b)
			ctx := context.Background()
			b.RunParallel(func(pb *testing.PB) {
				sid, _, _ := e.OpenSession(task, 0)
				sub, _, _ := e.CreateNode(sid, task, Spec{})
				for pb.Next() {
					l, _, err := e.Acquire(ctx, sid, sub, tc.class, AcquireOptions{})
					if err != nil {
						b.Errorf("Acquire: %v", err)
						return
					}
					if _, err := e.Release(sid, l, Report{}); err != nil {
						b.Errorf("Release: %v", err)
						return
					}
				}
			})
		})
	}
}
