package governor

import (
	"context"
	"errors"
	"time"

	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// Fractions of the TTL that pace the heartbeat and the local deadline.
const (
	// beatsPerTTL is how many heartbeats are sent in one TTL.
	beatsPerTTL = 3

	// marginPerTTL is the share of the TTL given up as a safety margin: one in this many.
	marginPerTTL = 5
)

// localDeadline is when the client must assume its session is gone.
func (c *Client) localDeadline(renewed time.Time) time.Time {
	// The margin covers clock drift and work that is slow to notice the cancellation.
	return renewed.Add(c.ttl - c.ttl/marginPerTTL)
}

// heartbeat renews the session and gives it up when the local deadline passes.
func (c *Client) heartbeat(renewed time.Time) {
	defer close(c.beats)
	if c.ttl == 0 {
		// A session without a TTL never expires, so there is nothing to renew.
		<-c.ctx.Done()
		return
	}
	interval := c.ttl / beatsPerTTL
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Until(c.localDeadline(renewed)))
	defer deadline.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-deadline.C:
			// The server may still hold the session, but this client can no longer prove it.
			c.cancel(ErrSessionLost)
			return
		case <-ticker.C:
		}
		// time.Now carries a monotonic reading, so a wall clock change cannot stretch the TTL.
		sent := time.Now()
		ctx, cancel := context.WithTimeout(c.ctx, interval)
		_, err := c.rpc.Heartbeat(c.auth(ctx), &pb.HeartbeatRequest{})
		cancel()
		switch err = fromStatus(err); {
		case err == nil:
			deadline.Reset(time.Until(c.localDeadline(sent)))
		case errors.Is(err, ErrSessionLost):
			c.cancel(ErrSessionLost)
			return
		}
		// Any other failure is retried at the next tick, until the deadline gives up.
	}
}
