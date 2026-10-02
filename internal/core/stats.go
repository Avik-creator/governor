package core

import (
	"cmp"
	"maps"
	"slices"
)

// Stats is a reading of the engine for monitoring; its cost grows with the tenants, not the nodes.
type Stats struct {
	// Nodes counts every node still known, ended or not, and Sessions every open session.
	Nodes    int
	Sessions int

	// Waiting is how many acquires are queued for each class.
	Waiting map[Class]int

	// Root is the root node, and Tenants are its active children, ordered by id.
	Root    NodeStats
	Tenants []NodeStats
}

// NodeStats is the caps of one node and what its whole subtree has used and holds.
type NodeStats struct {
	ID     NodeID
	Name   string
	Quotas map[Resource]int64
	Used   map[Resource]int64
	Limits map[Class]int
	Held   map[Class]int
}

// Stats reads the engine without applying anything that is due, so it changes nothing.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Stats{
		Nodes:    len(e.nodes),
		Sessions: len(e.sessions),
		Waiting:  make(map[Class]int, len(e.queues)),
		Root:     e.root.stats(),
	}
	for class, q := range e.queues {
		for _, tq := range q.ring {
			s.Waiting[class] += len(tq.waiters)
		}
	}
	for _, c := range e.root.children {
		if c.state == StateActive {
			s.Tenants = append(s.Tenants, c.stats())
		}
	}
	slices.SortFunc(s.Tenants, func(a, b NodeStats) int { return cmp.Compare(a.ID, b.ID) })
	return s
}

// stats copies what Stats reports about n.
func (n *node) stats() NodeStats {
	return NodeStats{
		ID:     n.id,
		Name:   n.name,
		Quotas: maps.Clone(n.quota),
		Used:   maps.Clone(n.used),
		Limits: maps.Clone(n.limits),
		Held:   maps.Clone(n.held),
	}
}
