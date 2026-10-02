package core

import "time"

// EventKind says what changed in the engine.
type EventKind string

// Event kinds, one per durable state change.
const (
	EventNodeCreated   EventKind = "node_created"
	EventNodeEnded     EventKind = "node_ended"
	EventConsumed      EventKind = "consumed"
	EventSessionOpened EventKind = "session_opened"
	EventSessionEnded  EventKind = "session_ended"
	EventLeaseGranted  EventKind = "lease_granted"
	EventLeaseEnded    EventKind = "lease_ended"
	EventLimitChanged  EventKind = "limit_changed"
)

// LeaseEnd says how a lease ended.
type LeaseEnd string

// The ways a lease can end.
const (
	LeaseReleased LeaseEnd = "released"
	LeaseExpired  LeaseEnd = "expired"
	LeaseRevoked  LeaseEnd = "revoked"
)

// Event is one state change; only the fields its Kind needs are set.
type Event struct {
	// Seq orders events and lets a caller wait until its change is durable.
	Seq  uint64    `json:"seq"`
	Kind EventKind `json:"kind"`
	Time time.Time `json:"time"`

	// Node is the node concerned or a session's scope; Parent is set on creation.
	Node   NodeID `json:"node,omitempty"`
	Parent NodeID `json:"parent,omitempty"`

	// Spec is set on node_created, and State on node_ended.
	Spec  *Spec `json:"spec,omitempty"`
	State State `json:"state,omitempty"`

	// Session and Lease identify the session and lease the event concerns.
	Session SessionID `json:"session,omitempty"`
	Lease   LeaseID   `json:"lease,omitempty"`

	// Expires is when a session, or a lease with a maximum hold time, expires.
	Expires time.Time `json:"expires,omitzero"`

	// Resource and Amount are set on consumed.
	Resource Resource `json:"resource,omitempty"`
	Amount   int64    `json:"amount,omitempty"`

	// Class is set on lease and limit events, and Limit on limit_changed.
	Class Class `json:"class,omitempty"`
	Limit int   `json:"limit,omitempty"`

	// End is set on lease_ended.
	End LeaseEnd `json:"end,omitempty"`
}

// Sink receives every event, in order, while the engine lock is held.
type Sink interface {
	Emit(Event)
}
