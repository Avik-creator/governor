package core

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
)

// validate reports whether d can be a node's defaults, wrapping ErrInvalid if not.
func (d *Defaults) validate() error {
	for depth := 0; d != nil; d, depth = d.Children, depth+1 {
		if depth >= MaxDepth {
			return fmt.Errorf("%w: defaults are nested deeper than %d", ErrInvalid, MaxDepth)
		}
		for r, limit := range d.Quotas {
			if r == "" || limit < 0 {
				return fmt.Errorf("%w: default quota %q must be named and not negative", ErrInvalid, r)
			}
		}
	}
	return nil
}

// clone returns a copy of d that shares nothing with it.
func (d *Defaults) clone() *Defaults {
	if d == nil {
		return nil
	}
	return &Defaults{Quotas: maps.Clone(d.Quotas), Children: d.Children.clone()}
}

// inherit returns spec as a new child of n gets it: n's defaults replace what they name.
func (n *node) inherit(spec Spec) Spec {
	d := n.defaults
	if d == nil {
		return spec
	}
	if len(d.Quotas) > 0 && spec.Quotas == nil {
		spec.Quotas = make(map[Resource]int64, len(d.Quotas))
	}
	maps.Copy(spec.Quotas, d.Quotas)
	if d.Children != nil {
		spec.Defaults = d.Children.clone()
	}
	return spec
}

// editable resolves id to an active node whose caps the live session sid may change.
func (e *Engine) editable(sid SessionID, id NodeID) (*node, error) {
	s, n, err := e.target(sid, id)
	if err != nil {
		return nil, err
	}
	// A session cannot loosen the caps it is itself held to; only the root has nothing above it.
	if n == s.scope && n != e.root {
		return nil, fmt.Errorf("%w: node %d is the session's own scope", ErrForbidden, id)
	}
	return n, e.admit(n)
}

// SetQuota changes a node's cap for a resource; a cap below what is used only stops further use.
func (e *Engine) SetQuota(sid SessionID, id NodeID, r Resource, limit int64) (uint64, error) {
	if r == "" || limit < Unlimited {
		return 0, fmt.Errorf("%w: bad resource or quota", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := e.editable(sid, id)
	if err != nil {
		return 0, err
	}
	// Setting the cap it already has changes nothing, so nothing is recorded.
	if old, ok := n.quota[r]; (ok && old == limit) || (!ok && limit == Unlimited) {
		return e.seq, nil
	}
	n.setQuota(r, limit)
	e.emit(Event{Kind: EventQuotaChanged, Node: n.id, Resource: r, Amount: limit})
	return e.seq, nil
}

// setQuota applies a cap, or removes it if limit is Unlimited.
func (n *node) setQuota(r Resource, limit int64) {
	if limit == Unlimited {
		delete(n.quota, r)
		return
	}
	if n.quota == nil {
		n.quota = make(map[Resource]int64)
	}
	n.quota[r] = limit
}

// SetLimitAs is SetLimit made by a session, which must be allowed to change the node.
func (e *Engine) SetLimitAs(sid SessionID, id NodeID, class Class, limit int) (uint64, error) {
	if class == "" || limit < Unlimited {
		return 0, fmt.Errorf("%w: bad class or limit", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := e.editable(sid, id)
	if err != nil {
		return 0, err
	}
	return e.setLimit(n, class, limit), nil
}

// setLimit applies a limit, or removes it if limit is Unlimited, and lets waiters through.
func (e *Engine) setLimit(n *node, class Class, limit int) uint64 {
	// Setting the limit it already has changes nothing, so nothing is recorded.
	if old, ok := n.limits[class]; (ok && old == limit) || (!ok && limit == Unlimited) {
		return e.seq
	}
	n.applyLimit(class, limit)
	e.emit(Event{Kind: EventLimitChanged, Node: n.id, Class: class, Limit: limit})
	e.dispatch()
	return e.seq
}

// applyLimit stores a limit, or removes it if limit is Unlimited.
func (n *node) applyLimit(class Class, limit int) {
	if limit == Unlimited {
		delete(n.limits, class)
		return
	}
	n.limits[class] = limit
}

// SetDefaults changes what each new child of a node starts with; nil gives them nothing.
func (e *Engine) SetDefaults(sid SessionID, id NodeID, d *Defaults) (uint64, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}
	d = d.clone()
	e.mu.Lock()
	defer e.mu.Unlock()
	// Defaults only shape a node's children, so a session may set them on its own scope.
	_, n, err := e.activeTarget(sid, id)
	if err != nil {
		return 0, err
	}
	n.defaults = d
	e.emit(Event{Kind: EventDefaultsChanged, Node: n.id, Defaults: d.clone()})
	return e.seq, nil
}

// Describe reports a node and its children, ordered by id.
func (e *Engine) Describe(sid SessionID, id NodeID) (NodeInfo, []NodeInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, n, err := e.target(sid, id)
	if err != nil {
		return NodeInfo{}, nil, err
	}
	children := make([]NodeInfo, 0, len(n.children))
	for _, c := range n.children {
		children = append(children, c.info())
	}
	slices.SortFunc(children, func(a, b NodeInfo) int { return cmp.Compare(a.ID, b.ID) })
	return n.info(), children, nil
}

// info copies what Describe reports about n.
func (n *node) info() NodeInfo {
	info := NodeInfo{
		ID:       n.id,
		Name:     n.name,
		State:    n.state,
		Deadline: n.deadline,
		EndedAt:  n.endedAt,
		Quotas:   maps.Clone(n.quota),
		Used:     maps.Clone(n.used),
		Limits:   maps.Clone(n.limits),
		Held:     maps.Clone(n.held),
		Defaults: n.defaults.clone(),
		Children: len(n.children),
	}
	if n.parent != nil {
		info.Parent = n.parent.id
	}
	return info
}
