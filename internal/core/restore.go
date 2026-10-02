package core

import (
	"fmt"
	"iter"
)

// Restore rebuilds an engine from a snapshot, if there is one, and the events recorded after it.
func Restore(cfg Config, snap *Snapshot, events iter.Seq2[Event, error]) (*Engine, error) {
	e := newEmpty(cfg)
	if snap != nil {
		if err := e.load(snap); err != nil {
			return nil, fmt.Errorf("%w: snapshot at event %d: %v", ErrCorrupt, snap.Seq, err)
		}
	}
	for ev, err := range events {
		if err != nil {
			return nil, err
		}
		if err := e.apply(ev); err != nil {
			return nil, fmt.Errorf("%w: event %d (%s): %v", ErrCorrupt, ev.Seq, ev.Kind, err)
		}
	}
	if e.root == nil {
		return New(cfg)
	}
	// Heartbeats are not recorded, so every session gets one fresh TTL to reconnect.
	now := e.clock.Now()
	for _, s := range e.sessions {
		if s.ttl > 0 {
			s.expires = now.Add(s.ttl)
		}
	}
	return e, nil
}

// recorded remembers what a replayed request did, if its session is still known.
func (e *Engine) recorded(ev Event, did outcome) {
	if s := e.sessions[ev.Session]; s != nil {
		s.remember(ev.Request, did)
	}
}

// apply replays one recorded event without checking limits or emitting anything.
func (e *Engine) apply(ev Event) error {
	if ev.Seq != e.seq+1 {
		return fmt.Errorf("follows event %d", e.seq)
	}
	e.seq = ev.Seq
	e.ids = max(e.ids, uint64(ev.Node), uint64(ev.Session), uint64(ev.Lease))

	if ev.Kind == EventSessionEnded {
		delete(e.sessions, ev.Session)
		return nil
	}
	if ev.Kind == EventNodeCreated && ev.Node == RootID && e.root == nil && ev.Spec != nil {
		e.root = e.addNode(RootID, nil, *ev.Spec)
		return nil
	}
	// Every other event concerns a node that an earlier event created.
	n := e.nodes[ev.Node]
	if ev.Kind == EventNodeCreated {
		n = e.nodes[ev.Parent]
	}
	if n == nil {
		return fmt.Errorf("unknown node %d", ev.Node)
	}

	switch ev.Kind {
	case EventNodeCreated:
		if ev.Spec == nil || e.nodes[ev.Node] != nil {
			return fmt.Errorf("node %d has no spec or already exists", ev.Node)
		}
		e.addNode(ev.Node, n, *ev.Spec)
		e.recorded(ev, outcome{kind: ev.Kind, node: ev.Parent, result: uint64(ev.Node)})
	case EventNodeEnded:
		e.setEnded(n, ev.State, ev.Time)
	case EventNodeRemoved:
		if n == e.root || n.state == StateActive {
			return fmt.Errorf("node %d cannot be removed", n.id)
		}
		e.remove(n)
	case EventConsumed:
		n.charge(ev.Resource, ev.Amount)
		e.recorded(ev, outcome{kind: ev.Kind, node: ev.Node, resource: ev.Resource, amount: ev.Amount})
	case EventSessionOpened:
		e.sessions[ev.Session] = &session{
			id:      ev.Session,
			scope:   n,
			ttl:     ev.TTL,
			expires: ev.Expires,
			leases:  make(map[LeaseID]*lease),
		}
	case EventLeaseGranted:
		s := e.sessions[ev.Session]
		if s == nil {
			return fmt.Errorf("unknown session %d", ev.Session)
		}
		e.addLease(&lease{id: ev.Lease, n: n, class: ev.Class, s: s, expires: ev.Expires})
		e.recorded(ev, outcome{kind: ev.Kind, node: ev.Node, class: ev.Class, result: uint64(ev.Lease)})
	case EventLeaseEnded:
		l := e.leases[ev.Lease]
		if l == nil {
			return fmt.Errorf("unknown lease %d", ev.Lease)
		}
		e.dropLease(l, ev.End, ev.Time)
	case EventLimitChanged:
		n.limits[ev.Class] = ev.Limit
	default:
		return fmt.Errorf("unknown kind")
	}
	return nil
}
