package core

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

type session struct {
	id      SessionID
	scope   *node         // the session may only act in this node's subtree
	ttl     time.Duration // zero never expires
	expires time.Time     // zero when the session never expires
	leases  map[LeaseID]*lease
	done    chan struct{} // closed when the session ends; created on demand

	requests map[string]outcome // what the session's recent requests with an id did
	order    []string           // their ids, oldest first
}

// within reports whether the session's scope is n or lies beneath it.
func (s *session) within(n *node) bool {
	scope := s.scope
	for scope.depth > n.depth {
		scope = scope.parent
	}
	return scope == n
}

// covers reports whether n is the session's scope or lies beneath it.
func (s *session) covers(n *node) bool {
	for n.depth > s.scope.depth {
		n = n.parent
	}
	return n == s.scope
}

// expired reports whether the session's TTL has lapsed at now.
func (s *session) expired(now time.Time) bool {
	return s.ttl > 0 && !now.Before(s.expires)
}

type lease struct {
	id      LeaseID
	n       *node
	class   Class
	s       *session
	expires time.Time // zero unless the lease has a maximum hold time
}

// expired reports whether the lease's maximum hold time has lapsed at now.
func (l *lease) expired(now time.Time) bool {
	return !l.expires.IsZero() && !now.Before(l.expires)
}

// tomb remembers how a lease ended, so a late release gets the right answer.
type tomb struct {
	end LeaseEnd
	at  time.Time
}

// OpenSession opens a session confined to scope's subtree and returns its expiry.
func (e *Engine) OpenSession(scope NodeID, ttl time.Duration) (SessionID, time.Time, uint64, error) {
	if ttl < 0 {
		return 0, time.Time{}, 0, fmt.Errorf("%w: ttl is negative", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := e.activeNode(scope)
	if err != nil {
		return 0, time.Time{}, 0, err
	}
	s := &session{
		id:     SessionID(e.nextID()),
		scope:  n,
		ttl:    ttl,
		leases: make(map[LeaseID]*lease),
	}
	if ttl > 0 {
		s.expires = e.clock.Now().Add(ttl)
	}
	e.sessions[s.id] = s
	e.emit(Event{Kind: EventSessionOpened, Node: n.id, Session: s.id, TTL: ttl, Expires: s.expires})
	return s.id, s.expires, e.seq, nil
}

// Heartbeat renews a session and all its leases, returning the new expiry.
func (e *Engine) Heartbeat(sid SessionID) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.liveSession(sid)
	if err != nil {
		return time.Time{}, err
	}
	if s.ttl > 0 {
		s.expires = e.clock.Now().Add(s.ttl)
	}
	return s.expires, nil
}

// Scope returns the node a live session is confined to.
func (e *Engine) Scope(sid SessionID) (NodeID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.liveSession(sid)
	if err != nil {
		return 0, err
	}
	return s.scope.id, nil
}

// Sessions returns the ids of every session, in no particular order.
func (e *Engine) Sessions() []SessionID {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Collect(maps.Keys(e.sessions))
}

// SessionDone returns a channel that is closed once the session has ended.
func (e *Engine) SessionDone(sid SessionID) <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.liveSession(sid)
	if err != nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if s.done == nil {
		s.done = make(chan struct{})
	}
	return s.done
}

// CloseSession ends a session and releases every lease it holds.
func (e *Engine) CloseSession(sid SessionID) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s := e.sessions[sid]; s != nil {
		e.endSession(s, LeaseReleased)
		e.dispatch()
	}
	return e.seq, nil
}

// liveSession resolves sid to a session whose TTL has not lapsed.
func (e *Engine) liveSession(sid SessionID) (*session, error) {
	s := e.sessions[sid]
	if s == nil {
		return nil, ErrSessionExpired
	}
	if s.expired(e.clock.Now()) {
		e.endSession(s, LeaseExpired)
		e.dispatch()
		return nil, ErrSessionExpired
	}
	return s, nil
}

// endSession ends s and its leases; the caller must dispatch afterwards.
func (e *Engine) endSession(s *session, why LeaseEnd) {
	for _, l := range s.leases {
		e.endLease(l, why)
	}
	delete(e.sessions, s.id)
	if s.done != nil {
		close(s.done)
	}
	e.emit(Event{Kind: EventSessionEnded, Session: s.id})
	e.failWaiters(func(w *waiter) error {
		if w.s == s {
			return ErrSessionExpired
		}
		return nil
	})
}

// fits reports whether one more lease of class fits on n's whole chain.
func (n *node) fits(class Class) bool {
	for a := n; a != nil; a = a.parent {
		if limit, ok := a.limits[class]; ok && a.held[class] >= limit {
			return false
		}
	}
	return true
}

// grant issues a lease of class at n to s; the caller has checked that it fits.
func (e *Engine) grant(n *node, class Class, s *session, opts AcquireOptions) *lease {
	l := &lease{id: LeaseID(e.nextID()), n: n, class: class, s: s}
	if opts.MaxHold > 0 {
		l.expires = e.clock.Now().Add(opts.MaxHold)
	}
	e.addLease(l)
	e.emit(Event{
		Kind:    EventLeaseGranted,
		Node:    n.id,
		Session: s.id,
		Lease:   l.id,
		Class:   class,
		Expires: l.expires,
		Request: opts.Request,
	})
	s.remember(opts.Request, outcome{kind: EventLeaseGranted, node: n.id, class: class, result: uint64(l.id)})
	return l
}

// addLease records l as held by its node, its session and the whole chain.
func (e *Engine) addLease(l *lease) {
	for a := l.n; a != nil; a = a.parent {
		a.held[l.class]++
	}
	l.n.leases[l.id] = l
	l.s.leases[l.id] = l
	e.leases[l.id] = l
	if !l.expires.IsZero() {
		e.timedLeases[l.id] = l
	}
}

// dropLease undoes addLease and remembers how and when the lease ended.
func (e *Engine) dropLease(l *lease, why LeaseEnd, at time.Time) {
	for a := l.n; a != nil; a = a.parent {
		a.held[l.class]--
	}
	delete(l.n.leases, l.id)
	delete(l.s.leases, l.id)
	delete(e.leases, l.id)
	delete(e.timedLeases, l.id)
	e.tombs[l.id] = tomb{end: why, at: at}
}

// endLease is the only place a lease ends; the caller must dispatch afterwards.
func (e *Engine) endLease(l *lease, why LeaseEnd) {
	e.dropLease(l, why, e.clock.Now())
	e.emit(Event{Kind: EventLeaseEnded, Node: l.n.id, Lease: l.id, Class: l.class, End: why})
}

// Acquire grants a lease of class at the node, queueing while its chain is full.
func (e *Engine) Acquire(ctx context.Context, sid SessionID, id NodeID, class Class, opts AcquireOptions) (LeaseID, uint64, error) {
	if class == "" || opts.MaxHold < 0 {
		return 0, 0, fmt.Errorf("%w: bad class or hold time", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	e.mu.Lock()
	s, err := e.liveSession(sid)
	if err != nil {
		e.mu.Unlock()
		return 0, 0, err
	}
	// A repeated request gets the lease it was granted before, even if that lease has since ended.
	did := outcome{kind: EventLeaseGranted, node: id, class: class}
	if lease, ok, err := s.recall(opts.Request, did); ok || err != nil {
		seq := e.seq
		e.mu.Unlock()
		return LeaseID(lease), seq, err
	}
	_, n, err := e.activeTarget(sid, id)
	if err != nil {
		e.mu.Unlock()
		return 0, 0, err
	}
	if n.fits(class) {
		l := e.grant(n, class, s, opts)
		seq := e.seq
		e.mu.Unlock()
		return l.id, seq, nil
	}
	w := &waiter{n: n, class: class, s: s, opts: opts, ch: make(chan granted, 1)}
	e.enqueue(w)
	e.mu.Unlock()

	select {
	case g := <-w.ch:
		return g.lease, g.seq, g.err
	case <-ctx.Done():
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if !w.settled {
		e.removeWaiter(w)
		return 0, 0, ctx.Err()
	}
	// The grant raced with the cancellation, so hand the lease back.
	if g := <-w.ch; g.err == nil {
		if l := e.leases[g.lease]; l != nil {
			e.endLease(l, LeaseReleased)
			e.dispatch()
		}
	}
	return 0, 0, ctx.Err()
}

// Release returns a lease; see SPEC.md §3.3 for the result in each case.
func (e *Engine) Release(sid SessionID, id LeaseID, r Report) (uint64, error) {
	class, seq, err := e.release(sid, id)
	if err == nil && class != "" && e.observer != nil {
		e.observer(class, r)
	}
	return seq, err
}

// release does the locked part of Release; class is empty if nothing was freed.
func (e *Engine) release(sid SessionID, id LeaseID) (Class, uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.leases[id]
	if l == nil {
		return "", e.seq, e.endedErr(id)
	}
	if l.s.id != sid {
		return "", 0, ErrNotOwner
	}
	if err := e.stale(l); err != nil {
		return "", 0, err
	}
	e.endLease(l, LeaseReleased)
	e.dispatch()
	return l.class, e.seq, nil
}

// Validate reports whether a lease is still held, for use as a fencing check.
func (e *Engine) Validate(id LeaseID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.leases[id]
	if l == nil {
		if err := e.endedErr(id); err != nil {
			return err
		}
		return ErrUnknownLease
	}
	return e.stale(l)
}

// stale ends a held lease whose session, hold time or node has lapsed.
func (e *Engine) stale(l *lease) error {
	now := e.clock.Now()
	switch {
	case l.s.expired(now):
		e.endSession(l.s, LeaseExpired)
	case l.expired(now):
		e.endLease(l, LeaseExpired)
	case l.n.deadlinePassed(now):
		e.expireDeadline(l.n)
	default:
		return nil
	}
	e.dispatch()
	return e.endedErr(l.id)
}

// endedErr says how a lease that is no longer held ended; nil means released.
func (e *Engine) endedErr(id LeaseID) error {
	t, ok := e.tombs[id]
	switch {
	case !ok:
		return ErrUnknownLease
	case t.end == LeaseExpired:
		return ErrLeaseExpired
	case t.end == LeaseRevoked:
		return ErrLeaseRevoked
	default:
		return nil
	}
}

// SetLimit changes a node's cap for a class; lowering it revokes nothing.
func (e *Engine) SetLimit(id NodeID, class Class, limit int) (uint64, error) {
	if class == "" || limit < 0 {
		return 0, fmt.Errorf("%w: bad class or limit", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := e.activeNode(id)
	if err != nil {
		return 0, err
	}
	// Setting the limit it already has changes nothing, so nothing is recorded.
	if old, ok := n.limits[class]; ok && old == limit {
		return e.seq, nil
	}
	n.limits[class] = limit
	e.emit(Event{Kind: EventLimitChanged, Node: n.id, Class: class, Limit: limit})
	e.dispatch()
	return e.seq, nil
}
