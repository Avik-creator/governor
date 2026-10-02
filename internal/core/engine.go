package core

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// MaxDepth is the deepest a node may sit below the root.
const MaxDepth = 32

// Config configures an Engine; the zero value is usable.
type Config struct {
	// Clock is the time source; nil means SystemClock.
	Clock Clock

	// Sink receives every state change; nil discards them.
	Sink Sink

	// Observer receives the report of every released lease, outside the lock.
	Observer func(Class, Report)

	// Retention is how long an ended lease stays known; zero means one minute.
	Retention time.Duration

	// NodeRetention is how long an ended node stays known before it is removed; zero means a day.
	NodeRetention time.Duration

	// Root describes the root node, whose limits are the shared pools.
	Root Spec
}

// Engine owns the tree; its methods are safe for concurrent use and linearizable.
type Engine struct {
	mu            sync.Mutex
	clock         Clock
	sink          Sink
	observer      func(Class, Report)
	retention     time.Duration
	nodeRetention time.Duration

	ids uint64 // last id issued to a node, session or lease
	seq uint64 // sequence number of the last event

	root        *node
	nodes       map[NodeID]*node
	ended       []*node          // nodes that have ended, oldest first, waiting to be removed
	timed       map[NodeID]*node // active nodes with a deadline of their own
	sessions    map[SessionID]*session
	leases      map[LeaseID]*lease
	timedLeases map[LeaseID]*lease // live leases with a maximum hold time
	tombs       map[LeaseID]tomb
	queues      map[Class]*classQueue
}

type node struct {
	id       NodeID
	name     string
	parent   *node
	tenant   *node // ancestor at depth 1, or the node itself at depth 0 or 1
	children map[NodeID]*node
	named    map[string]*node // newest child with each name, for EnsureNode
	depth    int
	weight   int
	priority int

	quota map[Resource]int64
	used  map[Resource]int64 // consumed in this subtree, including by nodes since removed
	gone  map[Resource]int64 // consumed by children that have been removed
	self  map[Resource]int64 // consumed directly at this node
	top   map[Resource]*node // child with the highest usage

	limits map[Class]int
	held   map[Class]int // leases held in this subtree

	deadline time.Time // effective: the earliest on the chain
	state    State
	endedAt  time.Time // when the node ended; zero while it is active
	leases   map[LeaseID]*lease
	done     chan struct{} // closed when the node ends; created on demand
}

// New returns an Engine whose root node is described by cfg.Root.
func New(cfg Config) (*Engine, error) {
	if err := cfg.Root.validate(); err != nil {
		return nil, err
	}
	if !cfg.Root.Deadline.IsZero() {
		return nil, fmt.Errorf("%w: the root cannot have a deadline", ErrInvalid)
	}
	e := newEmpty(cfg)
	spec := cfg.Root.clone()
	e.root = e.addNode(RootID, nil, spec)
	e.emit(Event{Kind: EventNodeCreated, Node: RootID, Spec: &spec})
	return e, nil
}

// newEmpty returns an engine with no nodes, not even the root.
func newEmpty(cfg Config) *Engine {
	e := &Engine{
		clock:         cfg.Clock,
		sink:          cfg.Sink,
		observer:      cfg.Observer,
		retention:     cfg.Retention,
		nodeRetention: cfg.NodeRetention,
		ids:           uint64(RootID),
		nodes:         make(map[NodeID]*node),
		timed:         make(map[NodeID]*node),
		sessions:      make(map[SessionID]*session),
		leases:        make(map[LeaseID]*lease),
		timedLeases:   make(map[LeaseID]*lease),
		tombs:         make(map[LeaseID]tomb),
		queues:        make(map[Class]*classQueue),
	}
	if e.clock == nil {
		e.clock = SystemClock{}
	}
	if e.retention <= 0 {
		e.retention = time.Minute
	}
	if e.nodeRetention <= 0 {
		e.nodeRetention = 24 * time.Hour
	}
	return e
}

// validate reports whether s can describe a node, wrapping ErrInvalid if not.
func (s Spec) validate() error {
	for r, limit := range s.Quotas {
		if r == "" {
			return fmt.Errorf("%w: empty resource name", ErrInvalid)
		}
		if limit < 0 {
			return fmt.Errorf("%w: quota %q is negative", ErrInvalid, r)
		}
	}
	for c, limit := range s.Limits {
		if c == "" {
			return fmt.Errorf("%w: empty class name", ErrInvalid)
		}
		if limit < 0 {
			return fmt.Errorf("%w: limit %q is negative", ErrInvalid, c)
		}
	}
	if s.Weight < 0 {
		return fmt.Errorf("%w: weight is negative", ErrInvalid)
	}
	return nil
}

// clone returns s with its own copies of the limit maps.
func (s Spec) clone() Spec {
	s.Quotas = maps.Clone(s.Quotas)
	s.Limits = maps.Clone(s.Limits)
	return s
}

func (e *Engine) nextID() uint64 {
	e.ids++
	return e.ids
}

// emit stamps ev with the next sequence number and hands it to the sink.
func (e *Engine) emit(ev Event) {
	e.seq++
	if e.sink == nil {
		return
	}
	ev.Seq = e.seq
	ev.Time = e.clock.Now()
	e.sink.Emit(ev)
}

func (e *Engine) addNode(id NodeID, parent *node, spec Spec) *node {
	n := &node{
		id:       id,
		name:     spec.Name,
		parent:   parent,
		children: make(map[NodeID]*node),
		named:    make(map[string]*node),
		weight:   max(spec.Weight, DefaultWeight),
		priority: spec.Priority,
		quota:    maps.Clone(spec.Quotas),
		used:     make(map[Resource]int64),
		gone:     make(map[Resource]int64),
		self:     make(map[Resource]int64),
		top:      make(map[Resource]*node),
		limits:   maps.Clone(spec.Limits),
		held:     make(map[Class]int),
		deadline: spec.Deadline,
		state:    StateActive,
		leases:   make(map[LeaseID]*lease),
	}
	if n.limits == nil {
		n.limits = make(map[Class]int)
	}
	n.tenant = n
	if parent != nil {
		n.depth = parent.depth + 1
		if n.depth > 1 {
			n.tenant = parent.tenant
		}
		parent.children[id] = n
		parent.named[n.name] = n
		if n.deadline.IsZero() || parent.deadlinePassed(n.deadline) {
			n.deadline = parent.deadline
		}
		if !n.deadline.IsZero() && !n.deadline.Equal(parent.deadline) {
			e.timed[id] = n
		}
	}
	e.nodes[id] = n
	return n
}

// deadlinePassed reports whether n has a deadline at or before now.
func (n *node) deadlinePassed(now time.Time) bool {
	return !n.deadline.IsZero() && !now.Before(n.deadline)
}

func closedErr(n *node) error {
	return fmt.Errorf("%w: node %d is %s", ErrClosed, n.id, n.state)
}

// activeNode resolves id to a node that can still admit work.
func (e *Engine) activeNode(id NodeID) (*node, error) {
	n := e.nodes[id]
	if n == nil {
		return nil, fmt.Errorf("%w: %d", ErrUnknownNode, id)
	}
	return n, e.admit(n)
}

// admit applies n's deadline and reports whether n can still take work.
func (e *Engine) admit(n *node) error {
	if n.state == StateActive && n.deadlinePassed(e.clock.Now()) {
		e.expireDeadline(n)
		e.dispatch()
	}
	if n.state != StateActive {
		return closedErr(n)
	}
	return nil
}

// target resolves id to a node inside the scope of the live session sid.
func (e *Engine) target(sid SessionID, id NodeID) (*session, *node, error) {
	s, err := e.liveSession(sid)
	if err != nil {
		return nil, nil, err
	}
	n := e.nodes[id]
	if n == nil {
		return nil, nil, fmt.Errorf("%w: %d", ErrUnknownNode, id)
	}
	if !s.covers(n) {
		return nil, nil, fmt.Errorf("%w: node %d", ErrForbidden, id)
	}
	return s, n, nil
}

// activeTarget is target for operations that need the node to admit work.
func (e *Engine) activeTarget(sid SessionID, id NodeID) (*session, *node, error) {
	s, n, err := e.target(sid, id)
	if err != nil {
		return nil, nil, err
	}
	return s, n, e.admit(n)
}

// expireDeadline ends the topmost node on n's chain whose deadline has passed.
func (e *Engine) expireDeadline(n *node) {
	now := e.clock.Now()
	top := n
	for top.parent != nil && top.parent.deadlinePassed(now) {
		top = top.parent
	}
	e.endNode(top, StateDeadlineExceeded)
}

// CreateNode adds a child under parent and returns its id and event sequence.
func (e *Engine) CreateNode(sid SessionID, parent NodeID, spec Spec) (NodeID, uint64, error) {
	if err := spec.validate(); err != nil {
		return 0, 0, err
	}
	spec = spec.clone()
	e.mu.Lock()
	defer e.mu.Unlock()
	_, p, err := e.activeTarget(sid, parent)
	if err != nil {
		return 0, 0, err
	}
	if p.depth >= MaxDepth {
		return 0, 0, fmt.Errorf("%w: tree is deeper than %d", ErrInvalid, MaxDepth)
	}
	n := e.addNode(NodeID(e.nextID()), p, spec)
	e.emit(Event{Kind: EventNodeCreated, Node: n.id, Parent: p.id, Spec: &spec})
	return n.id, e.seq, nil
}

// EnsureNode returns parent's newest child named spec.Name, ended or not, creating it if there is none.
func (e *Engine) EnsureNode(
	sid SessionID, parent NodeID, spec Spec, r Resource, amount int64,
) (NodeID, bool, uint64, error) {
	if err := spec.validate(); err != nil {
		return 0, false, 0, err
	}
	if spec.Name == "" || amount < 0 || (amount > 0 && r == "") {
		return 0, false, 0, fmt.Errorf("%w: a name is required, and a charge needs a resource", ErrInvalid)
	}
	spec = spec.clone()
	e.mu.Lock()
	defer e.mu.Unlock()
	_, p, err := e.target(sid, parent)
	if err != nil {
		return 0, false, 0, err
	}
	// An ended child is returned too, so cancelling it is not undone by asking again.
	if n := p.named[spec.Name]; n != nil {
		return n.id, false, e.seq, nil
	}
	if err := e.admit(p); err != nil {
		return 0, false, 0, err
	}
	if p.depth >= MaxDepth {
		return 0, false, 0, fmt.Errorf("%w: tree is deeper than %d", ErrInvalid, MaxDepth)
	}
	// The charge and the creation happen together, or neither does.
	if amount > 0 {
		if err := p.check(r, amount); err != nil {
			return 0, false, 0, err
		}
	}
	n := e.addNode(NodeID(e.nextID()), p, spec)
	e.emit(Event{Kind: EventNodeCreated, Node: n.id, Parent: p.id, Spec: &spec})
	if amount > 0 {
		p.charge(r, amount)
		e.emit(Event{Kind: EventConsumed, Node: p.id, Session: sid, Resource: r, Amount: amount})
	}
	return n.id, true, e.seq, nil
}

// check reports the denial if amount more of r would not fit on n's whole chain.
func (n *node) check(r Resource, amount int64) error {
	for a := n; a != nil; a = a.parent {
		if limit, ok := a.quota[r]; ok && amount > limit-a.used[r] {
			return a.denied(r, amount, limit)
		}
	}
	return nil
}

// Consume charges amount of r to the node and its whole chain, or to nothing.
func (e *Engine) Consume(sid SessionID, id NodeID, r Resource, amount int64) (uint64, error) {
	if r == "" || amount <= 0 {
		return 0, fmt.Errorf("%w: bad resource or amount", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, n, err := e.activeTarget(sid, id)
	if err != nil {
		return 0, err
	}
	if err := n.check(r, amount); err != nil {
		return 0, err
	}
	n.charge(r, amount)
	e.emit(Event{Kind: EventConsumed, Node: n.id, Session: sid, Resource: r, Amount: amount})
	return e.seq, nil
}

// charge adds amount of r to n and its chain, tracking each largest consumer.
func (n *node) charge(r Resource, amount int64) {
	n.self[r] += amount
	for a := n; a != nil; a = a.parent {
		a.used[r] += amount
		if p := a.parent; p != nil && (p.top[r] == nil || a.used[r] > p.top[r].used[r]) {
			p.top[r] = a
		}
	}
}

// denied builds the error for a consume that would exceed n's cap.
func (n *node) denied(r Resource, amount, limit int64) error {
	d := &DeniedError{
		Node:      n.id,
		Name:      n.name,
		Resource:  r,
		Used:      n.used[r],
		Limit:     limit,
		Requested: amount,
	}
	if t := n.top[r]; t != nil {
		d.TopConsumer, d.TopConsumerName, d.TopConsumerUsed = t.id, t.name, t.used[r]
	}
	return d
}

// Cancel ends a node and its subtree as cancelled; repeating it is a no-op.
func (e *Engine) Cancel(sid SessionID, id NodeID) (uint64, error) {
	return e.end(sid, id, StateCancelled)
}

// Close ends a node as done and cancels its descendants that are still active.
func (e *Engine) Close(sid SessionID, id NodeID) (uint64, error) {
	return e.end(sid, id, StateDone)
}

func (e *Engine) end(sid SessionID, id NodeID, state State) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, n, err := e.target(sid, id)
	if err != nil {
		return 0, err
	}
	if n == e.root {
		return 0, fmt.Errorf("%w: the root cannot be ended", ErrInvalid)
	}
	if n.state == StateActive {
		e.endNode(n, state)
		e.dispatch()
	}
	return e.seq, nil
}

// endNode ends n and its subtree; the caller must dispatch afterwards.
func (e *Engine) endNode(n *node, state State) {
	e.markEnded(n, state)
	e.failWaiters(func(w *waiter) error {
		if w.n.state.Terminal() {
			return closedErr(w.n)
		}
		return nil
	})
}

// markEnded moves n to state, revokes its leases and cancels its descendants.
func (e *Engine) markEnded(n *node, state State) {
	if n.state != StateActive {
		return
	}
	e.setEnded(n, state, e.clock.Now())
	for _, l := range n.leases {
		e.endLease(l, LeaseRevoked)
	}
	if n.done != nil {
		close(n.done)
	}
	e.emit(Event{Kind: EventNodeEnded, Node: n.id, State: state})
	for _, c := range n.children {
		e.markEnded(c, StateCancelled)
	}
}

// setEnded records that n ended in state at the given time and queues it for removal.
func (e *Engine) setEnded(n *node, state State, at time.Time) {
	n.state, n.endedAt = state, at
	delete(e.timed, n.id)
	e.ended = append(e.ended, n)
}

// removeEnded forgets the nodes that ended longer ago than the node retention.
func (e *Engine) removeEnded(now time.Time) {
	cutoff := now.Add(-e.nodeRetention)
	due := 0
	for ; due < len(e.ended) && !e.ended[due].endedAt.After(cutoff); due++ {
		n := e.ended[due]
		// A node removed together with its parent is still in the queue.
		if e.nodes[n.id] != n {
			continue
		}
		// A session confined to a node that no longer exists could do nothing, so it ends first.
		for _, s := range e.sessions {
			if s.within(n) {
				e.endSession(s, LeaseExpired)
			}
		}
		e.remove(n)
		e.emit(Event{Kind: EventNodeRemoved, Node: n.id})
	}
	e.ended = slices.Delete(e.ended, 0, due)
}

// remove forgets n and its subtree, keeping what they consumed in the parent's totals.
func (e *Engine) remove(n *node) {
	e.forget(n)
	p := n.parent
	for r, used := range n.used {
		p.gone[r] += used
	}
	delete(p.children, n.id)
	if p.named[n.name] == n {
		// The name now belongs to the newest child that still has it, if any.
		delete(p.named, n.name)
		for _, c := range p.children {
			if newest := p.named[n.name]; c.name == n.name && (newest == nil || c.id > newest.id) {
				p.named[n.name] = c
			}
		}
	}
	for r, top := range p.top {
		if top != n {
			continue
		}
		if next := p.largest(r); next != nil {
			p.top[r] = next
		} else {
			delete(p.top, r)
		}
	}
}

// forget drops n and everything under it from the engine's index.
func (e *Engine) forget(n *node) {
	for _, c := range n.children {
		e.forget(c)
	}
	delete(e.nodes, n.id)
	delete(e.timed, n.id)
}

// largest returns the child that has used the most of r, the older one on a tie, or nil.
func (n *node) largest(r Resource) *node {
	var top *node
	for _, c := range n.children {
		used := c.used[r]
		if used == 0 {
			continue
		}
		if top == nil || used > top.used[r] || (used == top.used[r] && c.id < top.id) {
			top = c
		}
	}
	return top
}

// Done returns a channel that is closed once the node has ended.
func (e *Engine) Done(sid SessionID, id NodeID) (<-chan struct{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, n, err := e.target(sid, id)
	if err != nil {
		return nil, err
	}
	if n.done == nil {
		n.done = make(chan struct{})
		if n.state != StateActive {
			close(n.done)
		}
	}
	return n.done, nil
}

// Children lists the children of a node, ordered by id.
func (e *Engine) Children(sid SessionID, id NodeID) ([]Child, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, n, err := e.target(sid, id)
	if err != nil {
		return nil, err
	}
	children := make([]Child, 0, len(n.children))
	for _, c := range n.children {
		children = append(children, Child{ID: c.id, Name: c.name, State: c.state})
	}
	slices.SortFunc(children, func(a, b Child) int { return cmp.Compare(a.ID, b.ID) })
	return children, nil
}

// State returns the node's current state, applying its deadline first.
func (e *Engine) State(id NodeID) (State, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.nodes[id]
	if n == nil {
		return "", fmt.Errorf("%w: %d", ErrUnknownNode, id)
	}
	if n.state == StateActive && n.deadlinePassed(e.clock.Now()) {
		e.expireDeadline(n)
		e.dispatch()
	}
	return n.state, nil
}

// Reap applies everything that is due: expired sessions, leases and deadlines.
func (e *Engine) Reap() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock.Now()
	for _, s := range e.sessions {
		if s.expired(now) {
			e.endSession(s, LeaseExpired)
		}
	}
	for _, l := range e.timedLeases {
		if l.expired(now) {
			e.endLease(l, LeaseExpired)
		}
	}
	for _, n := range e.timed {
		if n.state == StateActive && n.deadlinePassed(now) {
			e.expireDeadline(n)
		}
	}
	e.dispatch()
	e.removeEnded(now)
	cutoff := now.Add(-e.retention)
	for id, t := range e.tombs {
		if t.at.Before(cutoff) {
			delete(e.tombs, id)
		}
	}
}

// Run calls Reap every interval until ctx is done.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.Reap()
		}
	}
}
