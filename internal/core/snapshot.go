package core

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Snapshot is the whole durable state of an engine after the event numbered Seq.
type Snapshot struct {
	// Seq is the last event the snapshot includes, and IDs the last id issued.
	Seq uint64 `json:"seq"`
	IDs uint64 `json:"ids"`

	// Nodes are ordered by id, so every parent comes before its children.
	Nodes    []NodeState    `json:"nodes"`
	Sessions []SessionState `json:"sessions,omitempty"`
	Leases   []LeaseState   `json:"leases,omitempty"`
	Tombs    []TombState    `json:"tombs,omitempty"`
}

// NodeState is one node of a Snapshot.
type NodeState struct {
	ID     NodeID `json:"id"`
	Parent NodeID `json:"parent,omitempty"`

	// Spec holds the node's current limits and its effective deadline.
	Spec    Spec      `json:"spec"`
	State   State     `json:"state"`
	EndedAt time.Time `json:"ended_at,omitzero"`

	// Self is what the node consumed directly, and Gone what its removed children did.
	Self map[Resource]int64 `json:"self,omitempty"`
	Gone map[Resource]int64 `json:"gone,omitempty"`

	// Top names the child that has used the most of each resource.
	Top map[Resource]NodeID `json:"top,omitempty"`
}

// SessionState is one session of a Snapshot.
type SessionState struct {
	ID      SessionID     `json:"id"`
	Scope   NodeID        `json:"scope"`
	TTL     time.Duration `json:"ttl,omitempty"`
	Expires time.Time     `json:"expires,omitzero"`
}

// LeaseState is one held lease of a Snapshot.
type LeaseState struct {
	ID      LeaseID   `json:"id"`
	Node    NodeID    `json:"node"`
	Class   Class     `json:"class"`
	Session SessionID `json:"session"`
	Expires time.Time `json:"expires,omitzero"`
}

// TombState remembers how an ended lease of a Snapshot ended.
type TombState struct {
	Lease LeaseID   `json:"lease"`
	End   LeaseEnd  `json:"end"`
	At    time.Time `json:"at"`
}

// Seq returns the sequence number of the last event.
func (e *Engine) Seq() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq
}

// Snapshot captures the engine's state at one instant; replaying later events onto it restores the engine.
func (e *Engine) Snapshot() *Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	snap := &Snapshot{Seq: e.seq, IDs: e.ids}
	for _, n := range e.nodes {
		state := NodeState{
			ID:     n.id,
			Parent: nodeIDOf(n.parent),
			Spec: Spec{
				Name:     n.name,
				Quotas:   maps.Clone(n.quota),
				Limits:   maps.Clone(n.limits),
				Deadline: n.deadline,
				Weight:   n.weight,
				Priority: n.priority,
			},
			State:   n.state,
			EndedAt: n.endedAt,
			Self:    maps.Clone(n.self),
			Gone:    maps.Clone(n.gone),
			Top:     make(map[Resource]NodeID, len(n.top)),
		}
		for r, top := range n.top {
			state.Top[r] = top.id
		}
		snap.Nodes = append(snap.Nodes, state)
	}
	for _, s := range e.sessions {
		snap.Sessions = append(snap.Sessions, SessionState{ID: s.id, Scope: s.scope.id, TTL: s.ttl, Expires: s.expires})
	}
	for _, l := range e.leases {
		snap.Leases = append(snap.Leases, LeaseState{
			ID: l.id, Node: l.n.id, Class: l.class, Session: l.s.id, Expires: l.expires,
		})
	}
	for id, t := range e.tombs {
		snap.Tombs = append(snap.Tombs, TombState{Lease: id, End: t.end, At: t.at})
	}
	// Maps iterate in random order; sorting makes equal states produce equal snapshots.
	slices.SortFunc(snap.Nodes, func(a, b NodeState) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(snap.Sessions, func(a, b SessionState) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(snap.Leases, func(a, b LeaseState) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(snap.Tombs, func(a, b TombState) int { return cmp.Compare(a.Lease, b.Lease) })
	return snap
}

func nodeIDOf(n *node) NodeID {
	if n == nil {
		return 0
	}
	return n.id
}

// load fills an empty engine from a snapshot.
func (e *Engine) load(snap *Snapshot) error {
	for _, state := range snap.Nodes {
		parent := e.nodes[state.Parent]
		switch {
		case e.nodes[state.ID] != nil:
			return fmt.Errorf("node %d appears twice", state.ID)
		case state.ID == RootID && state.Parent != 0, state.ID != RootID && parent == nil:
			return fmt.Errorf("node %d has no known parent", state.ID)
		}
		n := e.addNode(state.ID, parent, state.Spec)
		if state.ID == RootID {
			e.root = n
		}
		maps.Copy(n.self, state.Self)
		maps.Copy(n.gone, state.Gone)
		if state.State.Terminal() {
			n.state, n.endedAt = state.State, state.EndedAt
			delete(e.timed, n.id)
			e.ended = append(e.ended, n)
		}
		// A node's usage is its own and its removed children's, counted on every ancestor too.
		for a := n; a != nil; a = a.parent {
			for r, used := range state.Self {
				a.used[r] += used
			}
			for r, used := range state.Gone {
				a.used[r] += used
			}
		}
	}
	if e.root == nil {
		return fmt.Errorf("there is no root node")
	}
	for _, state := range snap.Nodes {
		for r, id := range state.Top {
			top := e.nodes[id]
			if top == nil || top.parent != e.nodes[state.ID] {
				return fmt.Errorf("node %d names %d as a child, which it is not", state.ID, id)
			}
			e.nodes[state.ID].top[r] = top
		}
	}
	// Nodes leave in the order they ended, so the queue has to be in that order again.
	slices.SortStableFunc(e.ended, func(a, b *node) int { return a.endedAt.Compare(b.endedAt) })

	for _, state := range snap.Sessions {
		scope := e.nodes[state.Scope]
		if scope == nil {
			return fmt.Errorf("session %d is confined to unknown node %d", state.ID, state.Scope)
		}
		e.sessions[state.ID] = &session{
			id: state.ID, scope: scope, ttl: state.TTL, expires: state.Expires, leases: make(map[LeaseID]*lease),
		}
	}
	for _, state := range snap.Leases {
		n, s := e.nodes[state.Node], e.sessions[state.Session]
		if n == nil || s == nil {
			return fmt.Errorf("lease %d belongs to an unknown node or session", state.ID)
		}
		e.addLease(&lease{id: state.ID, n: n, class: state.Class, s: s, expires: state.Expires})
	}
	for _, state := range snap.Tombs {
		e.tombs[state.Lease] = tomb{end: state.End, at: state.At}
	}
	e.seq, e.ids = snap.Seq, snap.IDs
	return nil
}
