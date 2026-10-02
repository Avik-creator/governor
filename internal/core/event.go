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
	Seq  uint64
	Kind EventKind
	Time time.Time

	// Node is the node the event concerns, and Parent its parent on creation.
	Node   NodeID
	Parent NodeID

	// Spec is set on node_created, and State on node_ended.
	Spec  *Spec
	State State

	// Session and Lease identify the session and lease the event concerns.
	Session SessionID
	Lease   LeaseID

	// Expires is when a session, or a lease with a maximum hold time, expires.
	Expires time.Time

	// Resource and Amount are set on consumed.
	Resource Resource
	Amount   int64

	// Class is set on lease and limit events, and Limit on limit_changed.
	Class Class
	Limit int

	// End is set on lease_ended.
	End LeaseEnd
}

// Sink receives every event, in order, while the engine lock is held.
type Sink interface {
	Emit(Event)
}
