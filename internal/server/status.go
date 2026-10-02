package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// errBadKey reports an API key that governord does not know.
var errBadKey = errors.New("governor: unknown API key")

// failures maps each known error to its gRPC code and wire reason.
var failures = []struct {
	err    error
	code   codes.Code
	reason pb.Reason
}{
	{core.ErrInvalid, codes.InvalidArgument, pb.Reason_REASON_INVALID},
	{core.ErrUnknownNode, codes.NotFound, pb.Reason_REASON_UNKNOWN_NODE},
	{core.ErrSessionExpired, codes.Unauthenticated, pb.Reason_REASON_SESSION_EXPIRED},
	{core.ErrUnknownLease, codes.NotFound, pb.Reason_REASON_UNKNOWN_LEASE},
	{core.ErrLeaseExpired, codes.FailedPrecondition, pb.Reason_REASON_LEASE_EXPIRED},
	{core.ErrLeaseRevoked, codes.FailedPrecondition, pb.Reason_REASON_LEASE_REVOKED},
	{core.ErrNotOwner, codes.PermissionDenied, pb.Reason_REASON_NOT_OWNER},
	{core.ErrForbidden, codes.PermissionDenied, pb.Reason_REASON_FORBIDDEN},
	{core.ErrDenied, codes.ResourceExhausted, pb.Reason_REASON_DENIED},
	{core.ErrClosed, codes.FailedPrecondition, pb.Reason_REASON_CLOSED},
	{errBadKey, codes.Unauthenticated, pb.Reason_REASON_BAD_API_KEY},
}

// toStatus converts an error into a gRPC status carrying an ErrorDetail.
func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	for _, f := range failures {
		if !errors.Is(err, f.err) {
			continue
		}
		detail := &pb.ErrorDetail{Reason: f.reason}
		if d, ok := errors.AsType[*core.DeniedError](err); ok {
			detail.Denial = denialToProto(d)
		}
		st := status.New(f.code, err.Error())
		if with, err := st.WithDetails(detail); err == nil {
			st = with
		}
		return st.Err()
	}
	// The cause is logged by the interceptor rather than sent to the client.
	return status.Error(codes.Internal, "governor: internal error")
}
