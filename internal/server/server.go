package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/avikmukherjee/governor/internal/core"
	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

var _ pb.GovernorServiceServer = (*Server)(nil)

// Server implements GovernorService; its interceptors must be installed with it.
type Server struct {
	pb.UnimplementedGovernorServiceServer

	engine *core.Engine
	commit Committer
	tokens TokenStore
	keys   map[digest]core.NodeID // API key to the node it confines a session to

	mu       sync.Mutex
	sessions map[digest]core.SessionID // token hash to the session it names
	seen     dedup                     // outcomes of requests that carried a request id

	closed    chan struct{} // stops the goroutines that wait for sessions to end
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// Config holds what a Server is built from; all three stores are required.
type Config struct {
	// Engine is the authority the server exposes.
	Engine *core.Engine

	// Committer says when a change is durable; NopCommitter runs without a store.
	Committer Committer

	// Tokens keeps session tokens across restarts; NopTokenStore keeps none.
	Tokens TokenStore

	// Keys maps each API key to the node its sessions are confined to.
	Keys map[string]core.NodeID
}

// New returns a Server that has taken over the sessions already in the engine.
func New(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Engine == nil || cfg.Committer == nil || cfg.Tokens == nil {
		return nil, errors.New("server: engine, committer and token store are required")
	}
	s := &Server{
		engine:   cfg.Engine,
		commit:   cfg.Committer,
		tokens:   cfg.Tokens,
		keys:     make(map[digest]core.NodeID, len(cfg.Keys)),
		sessions: make(map[digest]core.SessionID),
		closed:   make(chan struct{}),
	}
	for key, scope := range cfg.Keys {
		// An empty key would match a caller that sent no credentials at all.
		if key == "" {
			return nil, errors.New("server: empty API key")
		}
		s.keys[hash(key)] = scope
	}
	if err := s.adopt(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Close stops the server's background goroutines; the engine is left running.
func (s *Server) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
	s.wg.Wait()
}

// durable waits until the change that emitted seq is committed.
func (s *Server) durable(ctx context.Context, seq uint64) error {
	err := s.commit.WaitDurable(ctx, seq)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	default:
		return status.Errorf(codes.Unavailable, "governor: commit failed: %v", err)
	}
}

// CreateNode adds a child node under a parent.
func (s *Server) CreateNode(ctx context.Context, req *pb.CreateNodeRequest) (*pb.CreateNodeResponse, error) {
	spec, err := specFromProto(req.GetSpec())
	if err != nil {
		return nil, err
	}
	return once(ctx, s, req.GetRequestId(), req, func() (*pb.CreateNodeResponse, error) {
		id, seq, err := s.engine.CreateNode(sessionFrom(ctx), core.NodeID(req.GetParentId()), spec)
		if err != nil {
			return nil, err
		}
		// The node exists now, so the wait must not end with the caller's context.
		if err := s.durable(context.WithoutCancel(ctx), seq); err != nil {
			return nil, err
		}
		return &pb.CreateNodeResponse{NodeId: uint64(id)}, nil
	})
}

// Consume charges a quota to a node and its whole chain, or to nothing.
func (s *Server) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	node, resource := core.NodeID(req.GetNodeId()), core.Resource(req.GetResource())
	return once(ctx, s, req.GetRequestId(), req, func() (*pb.ConsumeResponse, error) {
		seq, err := s.engine.Consume(sessionFrom(ctx), node, resource, req.GetAmount())
		if err != nil {
			return nil, err
		}
		// The charge is applied now, so the wait must not end with the caller's context.
		if err := s.durable(context.WithoutCancel(ctx), seq); err != nil {
			return nil, err
		}
		return &pb.ConsumeResponse{}, nil
	})
}

// CancelNode ends a node and its subtree as cancelled.
func (s *Server) CancelNode(ctx context.Context, req *pb.CancelNodeRequest) (*pb.CancelNodeResponse, error) {
	seq, err := s.engine.Cancel(sessionFrom(ctx), core.NodeID(req.GetNodeId()))
	if err != nil {
		return nil, err
	}
	if err := s.durable(ctx, seq); err != nil {
		return nil, err
	}
	return &pb.CancelNodeResponse{}, nil
}

// CloseNode ends a node as done and cancels its active descendants.
func (s *Server) CloseNode(ctx context.Context, req *pb.CloseNodeRequest) (*pb.CloseNodeResponse, error) {
	seq, err := s.engine.Close(sessionFrom(ctx), core.NodeID(req.GetNodeId()))
	if err != nil {
		return nil, err
	}
	if err := s.durable(ctx, seq); err != nil {
		return nil, err
	}
	return &pb.CloseNodeResponse{}, nil
}

// WatchNode sends one message when the node ends, then closes the stream.
func (s *Server) WatchNode(req *pb.WatchNodeRequest, stream pb.GovernorService_WatchNodeServer) error {
	ctx := stream.Context()
	sid, id := sessionFrom(ctx), core.NodeID(req.GetNodeId())
	done, err := s.engine.Done(sid, id)
	if err != nil {
		return err
	}
	select {
	case <-done:
	case <-s.engine.SessionDone(sid):
		return core.ErrSessionExpired
	case <-ctx.Done():
		return ctx.Err()
	}
	state, err := s.engine.State(id)
	if err != nil {
		return err
	}
	return stream.Send(&pb.WatchNodeResponse{State: stateToProto(state)})
}

// OpenSession trades the bearer API key for a session confined to the key's node.
func (s *Server) OpenSession(ctx context.Context, req *pb.OpenSessionRequest) (*pb.OpenSessionResponse, error) {
	scope, ok := s.keys[hash(bearer(ctx))]
	if !ok {
		return nil, errBadKey
	}
	ttl, err := toDuration("ttl", req.GetTtl())
	if err != nil {
		return nil, err
	}
	sid, expires, seq, err := s.engine.OpenSession(scope, ttl)
	if err != nil {
		return nil, err
	}
	if err := s.durable(ctx, seq); err != nil {
		// The caller will never learn of this session, so end it.
		_, _ = s.engine.CloseSession(sid)
		return nil, err
	}
	// rand.Text carries at least 128 bits of randomness, so it cannot be guessed.
	token := rand.Text()
	if err := s.tokens.SaveToken(ctx, hash(token), sid); err != nil {
		_, _ = s.engine.CloseSession(sid)
		return nil, status.Errorf(codes.Unavailable, "governor: save token: %v", err)
	}
	s.issue(hash(token), sid)
	return &pb.OpenSessionResponse{
		SessionToken: token,
		ScopeId:      uint64(scope),
		ExpiresAt:    toTimestamp(expires),
	}, nil
}

// Heartbeat renews the caller's session and every lease it holds.
func (s *Server) Heartbeat(ctx context.Context, _ *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	expires, err := s.engine.Heartbeat(sessionFrom(ctx))
	if err != nil {
		return nil, err
	}
	return &pb.HeartbeatResponse{ExpiresAt: toTimestamp(expires)}, nil
}

// CloseSession ends the caller's session and releases every lease it holds.
func (s *Server) CloseSession(ctx context.Context, _ *pb.CloseSessionRequest) (*pb.CloseSessionResponse, error) {
	seq, err := s.engine.CloseSession(sessionFrom(ctx))
	if err != nil {
		return nil, err
	}
	if err := s.durable(ctx, seq); err != nil {
		return nil, err
	}
	return &pb.CloseSessionResponse{}, nil
}

// Acquire grants a lease, blocking while the node's chain is full.
func (s *Server) Acquire(ctx context.Context, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	hold, err := toDuration("max_hold", req.GetMaxHold())
	if err != nil {
		return nil, err
	}
	sid, node, class := sessionFrom(ctx), core.NodeID(req.GetNodeId()), core.Class(req.GetClass())
	return once(ctx, s, req.GetRequestId(), req, func() (*pb.AcquireResponse, error) {
		lease, seq, err := s.engine.Acquire(ctx, sid, node, class, core.AcquireOptions{MaxHold: hold})
		if err != nil {
			return nil, err
		}
		if err := s.durable(ctx, seq); err != nil {
			// The caller will never learn of this lease, so hand it back.
			_, _ = s.engine.Release(sid, lease, core.Report{})
			return nil, err
		}
		return &pb.AcquireResponse{LeaseId: uint64(lease)}, nil
	})
}

// Release returns a lease and reports how the guarded work went.
func (s *Server) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	latency, err := toDuration("latency", req.GetLatency())
	if err != nil {
		return nil, err
	}
	if latency < 0 {
		return nil, fmt.Errorf("%w: latency is negative", core.ErrInvalid)
	}
	report := core.Report{Latency: latency, Overloaded: req.GetOverloaded()}
	seq, err := s.engine.Release(sessionFrom(ctx), core.LeaseID(req.GetLeaseId()), report)
	if err != nil {
		return nil, err
	}
	if err := s.durable(ctx, seq); err != nil {
		return nil, err
	}
	return &pb.ReleaseResponse{}, nil
}

// Validate reports whether a lease is still held, as a fencing check.
func (s *Server) Validate(_ context.Context, req *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	if err := s.engine.Validate(core.LeaseID(req.GetLeaseId())); err != nil {
		return nil, err
	}
	return &pb.ValidateResponse{}, nil
}
