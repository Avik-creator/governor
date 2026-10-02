package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/avikmukherjee/governor/internal/core"
)

// maxRemembered caps how many request ids are kept per session.
const maxRemembered = 4096

// call is the outcome of one request, shared by every retry of it.
type call struct {
	args  digest        // fingerprint of the request that first used the id
	done  chan struct{} // closed once resp, err and retry are set
	resp  any
	err   error
	retry bool // the outcome was not final, so a retry must run again
}

// history holds the calls of one session, oldest first in order.
type history struct {
	calls map[string]*call
	order []string
}

// dedup remembers request outcomes by session and request id.
type dedup struct {
	mu       sync.Mutex
	sessions map[core.SessionID]*history
}

// begin returns the call for a request id, and whether the caller must run it.
func (d *dedup) begin(sid core.SessionID, id string, args digest) (*call, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions == nil {
		d.sessions = make(map[core.SessionID]*history)
	}
	h := d.sessions[sid]
	if h == nil {
		h = &history{calls: make(map[string]*call)}
		d.sessions[sid] = h
	}
	if c := h.calls[id]; c != nil {
		return c, false
	}
	c := &call{args: args, done: make(chan struct{})}
	h.calls[id] = c
	h.order = append(h.order, id)
	if len(h.order) > maxRemembered {
		delete(h.calls, h.order[0])
		h.order = h.order[1:]
	}
	return c, true
}

// finish records a call's outcome and wakes the retries waiting for it.
func (d *dedup) finish(sid core.SessionID, id string, c *call, resp any, err error) {
	c.resp, c.err = resp, err
	// A cancelled call left nothing behind, so the id may be used again.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		c.retry = true
		d.mu.Lock()
		if h := d.sessions[sid]; h != nil && h.calls[id] == c {
			delete(h.calls, id)
		}
		d.mu.Unlock()
	}
	close(c.done)
}

// forget drops everything remembered for a session that has ended.
func (d *dedup) forget(sid core.SessionID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.sessions, sid)
}

// fingerprint hashes a request, so a reused id with new arguments is caught.
func fingerprint(req proto.Message) (digest, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return digest{}, fmt.Errorf("%w: %v", core.ErrInvalid, err)
	}
	h := sha256.New()
	h.Write([]byte(req.ProtoReflect().Descriptor().FullName()))
	h.Write(b)
	return digest(h.Sum(nil)), nil
}

// once runs fn for the first request with this id and replays its outcome to retries.
func once[T any](ctx context.Context, s *Server, id string, req proto.Message, fn func() (T, error)) (T, error) {
	var zero T
	if id == "" {
		return fn()
	}
	args, err := fingerprint(req)
	if err != nil {
		return zero, err
	}
	sid := sessionFrom(ctx)
	for {
		c, first := s.seen.begin(sid, id, args)
		if first {
			resp, err := fn()
			s.seen.finish(sid, id, c, resp, err)
			return resp, err
		}
		if c.args != args {
			return zero, fmt.Errorf("%w: request id %q was used with other arguments", core.ErrInvalid, id)
		}
		select {
		case <-c.done:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
		if !c.retry {
			resp, _ := c.resp.(T)
			return resp, c.err
		}
	}
}
