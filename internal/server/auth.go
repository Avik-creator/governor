package server

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// digest is the SHA-256 of a secret; secrets are only ever stored hashed.
type digest [sha256.Size]byte

func hash(secret string) digest {
	return sha256.Sum256([]byte(secret))
}

// public lists the methods that do not need a session token.
var public = map[string]bool{
	pb.GovernorService_OpenSession_FullMethodName: true,
	pb.GovernorService_Validate_FullMethodName:    true,
}

// oneShot lists the methods an API key may call directly, with no session of its own.
var oneShot = map[string]bool{
	pb.GovernorService_EnsureNode_FullMethodName: true,
	pb.GovernorService_Consume_FullMethodName:    true,
}

type sessionKey struct{}

// sessionFrom returns the session the interceptor resolved, or zero if none.
func sessionFrom(ctx context.Context) core.SessionID {
	sid, _ := ctx.Value(sessionKey{}).(core.SessionID)
	return sid
}

// bearer returns the secret from the "authorization: Bearer <secret>" metadata.
func bearer(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if secret, ok := strings.CutPrefix(v, "Bearer "); ok {
			return secret
		}
	}
	return ""
}

// authenticate puts the caller's session into ctx, unless the method is public.
func (s *Server) authenticate(ctx context.Context, method string) (context.Context, error) {
	if public[method] {
		return ctx, nil
	}
	secret := hash(bearer(ctx))
	s.mu.Lock()
	sid, ok := s.sessions[secret]
	s.mu.Unlock()
	// A short-lived caller such as a hook sends its API key and is served by the key's standing session.
	if !ok && oneShot[method] {
		sid, ok = s.standing[secret]
	}
	if !ok {
		return ctx, toStatus(core.ErrSessionExpired)
	}
	return context.WithValue(ctx, sessionKey{}, sid), nil
}

// finish converts a handler's error to a status, logging server faults.
func finish(ctx context.Context, method string, err error) error {
	if err == nil {
		return nil
	}
	st := toStatus(err)
	// These two are server faults, so their cause belongs in the log.
	if code := status.Code(st); code == codes.Internal || code == codes.Unavailable {
		slog.ErrorContext(ctx, "governor: call failed", "method", method, "code", code, "err", err)
	}
	return st
}

// UnaryInterceptor authenticates unary calls and maps their errors.
func (s *Server) UnaryInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	ctx, err := s.authenticate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	resp, err := handler(ctx, req)
	return resp, finish(ctx, info.FullMethod, err)
}

// StreamInterceptor authenticates streaming calls and maps their errors.
func (s *Server) StreamInterceptor(
	srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx, err := s.authenticate(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	err = handler(srv, &authedStream{ServerStream: ss, ctx: ctx})
	return finish(ctx, info.FullMethod, err)
}

// authedStream is a ServerStream whose context carries the caller's session.
type authedStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the authenticated context.
func (a *authedStream) Context() context.Context {
	return a.ctx
}
