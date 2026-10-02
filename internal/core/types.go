// Package core is Governor's in-memory authority; SPEC.md is its contract.
package core

import "time"

// NodeID identifies a node in the tree.
type NodeID uint64

// LeaseID identifies a lease and, never being reissued, is its fencing token.
type LeaseID uint64

// SessionID identifies a worker session.
type SessionID uint64

// RootID is the id of the root node, which always exists and cannot be ended.
const RootID NodeID = 1

// Resource names a quota, such as "http", "sql" or "retry".
type Resource string

// Class names a concurrency class held through a lease, such as "db".
type Class string

// State is the lifecycle state of a node; the empty string is not a valid state.
type State string

// Node states. Every state other than StateActive is terminal.
const (
	StateActive           State = "active"
	StateDone             State = "done"
	StateCancelled        State = "cancelled"
	StateDeadlineExceeded State = "deadline_exceeded"
)

// Terminal reports whether s is a final state.
func (s State) Terminal() bool {
	return s != StateActive
}

// DefaultWeight is the scheduling weight used when Spec.Weight is zero.
const DefaultWeight = 1

// Unlimited, given as a quota or a limit to set, removes the cap instead.
const Unlimited = -1

// Defaults is what a node gives each new child, whatever the child's own spec asks for.
type Defaults struct {
	// Quotas replace the child's caps for the resources named here.
	Quotas map[Resource]int64 `json:"quotas,omitempty"`

	// Children become the child's own defaults, for the nodes created under it.
	Children *Defaults `json:"children,omitempty"`
}

// Spec describes a node to create; the zero Spec has no limits of its own.
type Spec struct {
	// Name is a label for denials and snapshots; it need not be unique.
	Name string `json:"name,omitempty"`

	// Quotas caps total consumption per resource; absent means uncapped here.
	Quotas map[Resource]int64 `json:"quotas,omitempty"`

	// Limits caps leases held at once per class; absent means uncapped here.
	Limits map[Class]int `json:"limits,omitempty"`

	// Deadline is when the node ends; the zero time means none of its own.
	Deadline time.Time `json:"deadline,omitzero"`

	// Weight is a tenant's share when competing for a class; zero means 1.
	Weight int `json:"weight,omitempty"`

	// Priority orders queued acquires within a tenant; higher goes first.
	Priority int `json:"priority,omitempty"`

	// Defaults is what each new child of the node starts with; nil means nothing.
	Defaults *Defaults `json:"defaults,omitempty"`
}

// Child describes one child of a node, as listed by Engine.Children.
type Child struct {
	ID    NodeID
	Name  string
	State State
}

// NodeInfo describes one node as Engine.Describe reports it.
type NodeInfo struct {
	ID     NodeID
	Parent NodeID
	Name   string
	State  State

	// Deadline is the effective deadline, and EndedAt is zero while the node is active.
	Deadline time.Time
	EndedAt  time.Time

	// Quotas and Limits are the node's own caps; Used and Held count its whole subtree.
	Quotas map[Resource]int64
	Used   map[Resource]int64
	Limits map[Class]int
	Held   map[Class]int

	// Defaults is what each new child starts with, and Children how many children there are.
	Defaults *Defaults
	Children int
}

// AcquireOptions adjusts a single lease acquisition.
type AcquireOptions struct {
	// MaxHold expires the lease after this long; zero means no maximum.
	MaxHold time.Duration

	// Request makes a repeated acquire with the same id return the same lease.
	Request string
}

// Report is what a holder tells the engine when it releases a lease.
type Report struct {
	// Latency is how long the guarded operation took; zero means not measured.
	Latency time.Duration

	// Overloaded reports that the downstream signalled overload.
	Overloaded bool
}
