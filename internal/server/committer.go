// Package server exposes the engine as the GovernorService gRPC API.
package server

import "context"

// Committer reports when the events up to a sequence number are durable.
type Committer interface {
	// WaitDurable blocks until every event up to seq is committed, or fails.
	WaitDurable(ctx context.Context, seq uint64) error
}

// NopCommitter treats every event as durable at once, for use without a store.
type NopCommitter struct{}

// WaitDurable returns immediately.
func (NopCommitter) WaitDurable(context.Context, uint64) error {
	return nil
}
