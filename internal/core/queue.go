package core

import (
	"slices"
	"sort"
)

// waiter is one queued Acquire.
type waiter struct {
	n       *node
	class   Class
	s       *session
	opts    AcquireOptions
	ch      chan granted // buffered, receives exactly one result
	settled bool         // a result has been sent on ch
}

// granted is the result delivered to a waiter.
type granted struct {
	lease LeaseID
	seq   uint64
	err   error
}

// tenantQueue holds one tenant's waiters, by priority and then arrival.
type tenantQueue struct {
	tenant  *node
	waiters []*waiter
	deficit int // grants left in this tenant's current round
}

// classQueue serves the tenants waiting for one class in round-robin order.
type classQueue struct {
	ring     []*tenantQueue
	byTenant map[NodeID]*tenantQueue
	cursor   int // index in ring of the tenant to serve next
}

// enqueue adds w behind every waiter of its tenant with the same or higher priority.
func (e *Engine) enqueue(w *waiter) {
	q := e.queues[w.class]
	if q == nil {
		q = &classQueue{byTenant: make(map[NodeID]*tenantQueue)}
		e.queues[w.class] = q
	}
	tq := q.byTenant[w.n.tenant.id]
	if tq == nil {
		tq = &tenantQueue{tenant: w.n.tenant}
		q.byTenant[tq.tenant.id] = tq
		q.ring = append(q.ring, tq)
	}
	i := sort.Search(len(tq.waiters), func(i int) bool {
		return tq.waiters[i].n.priority < w.n.priority
	})
	tq.waiters = slices.Insert(tq.waiters, i, w)
}

// removeWaiter takes w out of its queue and drops queues that become empty.
func (e *Engine) removeWaiter(w *waiter) {
	q := e.queues[w.class]
	if q == nil {
		return
	}
	tq := q.byTenant[w.n.tenant.id]
	if tq == nil {
		return
	}
	i := slices.Index(tq.waiters, w)
	if i < 0 {
		return
	}
	tq.waiters = slices.Delete(tq.waiters, i, i+1)
	if len(tq.waiters) > 0 {
		return
	}
	i = slices.Index(q.ring, tq)
	q.ring = slices.Delete(q.ring, i, i+1)
	delete(q.byTenant, tq.tenant.id)
	if i < q.cursor {
		q.cursor--
	}
	if q.cursor >= len(q.ring) {
		q.cursor = 0
	}
	if len(q.ring) == 0 {
		delete(e.queues, w.class)
	}
}

// settle delivers a result to w and removes it from its queue.
func (e *Engine) settle(w *waiter, g granted) {
	w.settled = true
	w.ch <- g
	e.removeWaiter(w)
}

// failWaiters fails every waiter for which reason returns an error.
func (e *Engine) failWaiters(reason func(*waiter) error) {
	type failure struct {
		w   *waiter
		err error
	}
	var failed []failure
	for _, q := range e.queues {
		for _, tq := range q.ring {
			for _, w := range tq.waiters {
				if err := reason(w); err != nil {
					failed = append(failed, failure{w, err})
				}
			}
		}
	}
	for _, f := range failed {
		e.settle(f.w, granted{err: f.err})
	}
}

// firstFit returns the tenant's first waiter whose chain has room, or nil.
func (tq *tenantQueue) firstFit(class Class) *waiter {
	if !tq.tenant.fits(class) {
		return nil
	}
	for _, w := range tq.waiters {
		if w.n.fits(class) {
			return w
		}
	}
	return nil
}

// pick finds the next waiter to serve by deficit round robin, or returns nil.
func (q *classQueue) pick(class Class) (int, *waiter) {
	for i := range q.ring {
		idx := (q.cursor + i) % len(q.ring)
		tq := q.ring[idx]
		w := tq.firstFit(class)
		if w == nil {
			continue
		}
		// Tenants passed over on the way forfeit the rest of their round.
		for j := range i {
			q.ring[(q.cursor+j)%len(q.ring)].deficit = 0
		}
		return idx, w
	}
	return 0, nil
}

// expireStale ends w's session or node if it has lapsed, and reports whether it did.
func (e *Engine) expireStale(w *waiter) bool {
	now := e.clock.Now()
	switch {
	case w.s.expired(now):
		e.endSession(w.s, LeaseExpired)
	case w.n.deadlinePassed(now):
		e.expireDeadline(w.n)
	default:
		return false
	}
	return true
}

// dispatchClass grants leases of class until no waiter fits.
func (e *Engine) dispatchClass(class Class, q *classQueue) (ended bool) {
	for {
		idx, w := q.pick(class)
		if w == nil {
			return ended
		}
		if e.expireStale(w) {
			ended = true
			continue
		}
		tq := q.ring[idx]
		q.cursor = idx
		if tq.deficit <= 0 {
			tq.deficit = tq.tenant.weight
		}
		tq.deficit--
		l := e.grant(w.n, class, w.s, w.opts)
		e.settle(w, granted{lease: l.id, seq: e.seq})
		if tq.deficit == 0 && len(tq.waiters) > 0 {
			q.cursor = (idx + 1) % len(q.ring)
		}
	}
}

// dispatch grants every queued acquire that fits, in every class.
func (e *Engine) dispatch() {
	for again := true; again; {
		again = false
		for class, q := range e.queues {
			if e.dispatchClass(class, q) {
				again = true
			}
		}
	}
}
