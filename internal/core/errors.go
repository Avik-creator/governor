package core

import (
	"errors"
	"fmt"
)

// Sentinel errors, matched with errors.Is.
var (
	// ErrInvalid reports a malformed argument.
	ErrInvalid = errors.New("governor: invalid argument")

	// ErrUnknownNode reports a node id that does not exist.
	ErrUnknownNode = errors.New("governor: unknown node")

	// ErrSessionExpired reports a session that has expired or never existed.
	ErrSessionExpired = errors.New("governor: session expired")

	// ErrUnknownLease reports a lease that was never issued or was forgotten.
	ErrUnknownLease = errors.New("governor: unknown lease")

	// ErrLeaseExpired reports a lease that ended with its session or hold time.
	ErrLeaseExpired = errors.New("governor: lease expired")

	// ErrLeaseRevoked reports a lease that ended because its node ended.
	ErrLeaseRevoked = errors.New("governor: lease revoked")

	// ErrNotOwner reports a lease that belongs to another session.
	ErrNotOwner = errors.New("governor: lease held by another session")

	// ErrDenied reports a consume that would exceed a quota; see DeniedError.
	ErrDenied = errors.New("governor: quota exceeded")

	// ErrClosed reports an operation on a node that has ended.
	ErrClosed = errors.New("governor: node has ended")
)

// DeniedError is the detail of a quota denial; it matches ErrDenied.
type DeniedError struct {
	// Node is the node whose cap was hit, and Name is its label.
	Node NodeID
	Name string

	// Resource is the quota that was exhausted.
	Resource Resource

	// Used, Limit and Requested describe the cap at the time of the denial.
	Used      int64
	Limit     int64
	Requested int64

	// TopConsumer is the child of Node that has used the most, if any.
	TopConsumer     NodeID
	TopConsumerName string
	TopConsumerUsed int64
}

// Error describes the denial in one line.
func (e *DeniedError) Error() string {
	msg := fmt.Sprintf("%v: %s at node %d (%s): used %d of %d, requested %d",
		ErrDenied, e.Resource, e.Node, e.Name, e.Used, e.Limit, e.Requested)
	if e.TopConsumer != 0 {
		msg += fmt.Sprintf("; largest consumer is node %d (%s) with %d",
			e.TopConsumer, e.TopConsumerName, e.TopConsumerUsed)
	}
	return msg
}

// Unwrap makes errors.Is(err, ErrDenied) true.
func (e *DeniedError) Unwrap() error {
	return ErrDenied
}
