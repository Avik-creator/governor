package governor

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

// Errors returned by this package, matched with errors.Is.
var (
	// ErrDenied reports a consume that would exceed a quota; see DeniedError.
	ErrDenied = errors.New("governor: quota exceeded")

	// ErrTaskEnded reports work on a task that was closed, cancelled or timed out.
	ErrTaskEnded = errors.New("governor: task has ended")

	// ErrSessionLost reports that the client's session expired or was never renewed.
	ErrSessionLost = errors.New("governor: session lost")

	// ErrLeaseLost reports a lease that expired, was revoked or is not the caller's.
	ErrLeaseLost = errors.New("governor: lease lost")

	// ErrForbidden reports a node outside the tenant this client belongs to.
	ErrForbidden = errors.New("governor: forbidden")

	// ErrNotFound reports a node or lease that governord does not know.
	ErrNotFound = errors.New("governor: not found")

	// ErrInvalid reports an argument that governord rejected.
	ErrInvalid = errors.New("governor: invalid argument")

	// ErrBadAPIKey reports an API key that governord does not know.
	ErrBadAPIKey = errors.New("governor: unknown API key")

	// ErrUnavailable reports that governord could not be reached or could not commit.
	ErrUnavailable = errors.New("governor: unavailable")

	// ErrNoTask reports a context that carries no task to charge the work to.
	ErrNoTask = errors.New("governor: context carries no task")

	// ErrClientClosed reports use of a Client after Close.
	ErrClientClosed = errors.New("governor: client closed")
)

// DeniedError is the detail of a quota denial; it matches ErrDenied.
type DeniedError struct {
	// Node is the node whose cap was hit, and Name is its label.
	Node uint64
	Name string

	// Resource is the quota that was exhausted.
	Resource string

	// Used, Limit and Requested describe the cap at the time of the denial.
	Used      int64
	Limit     int64
	Requested int64

	// TopConsumer is the child of Node that has used the most, if any.
	TopConsumer     uint64
	TopConsumerName string
	TopConsumerUsed int64
}

// Error describes the denial in one line.
func (e *DeniedError) Error() string {
	msg := fmt.Sprintf("%v: %s at %q: used %d of %d, requested %d",
		ErrDenied, e.Resource, e.Name, e.Used, e.Limit, e.Requested)
	if e.TopConsumer != 0 {
		msg += fmt.Sprintf("; largest consumer is %q with %d", e.TopConsumerName, e.TopConsumerUsed)
	}
	return msg
}

// Unwrap makes errors.Is(err, ErrDenied) true.
func (e *DeniedError) Unwrap() error {
	return ErrDenied
}

// rpcError pairs one of this package's errors with the server's message.
type rpcError struct {
	kind error
	msg  string
}

func (e *rpcError) Error() string { return e.msg }
func (e *rpcError) Unwrap() error { return e.kind }

// reasons maps each wire reason to the error callers match against.
var reasons = map[pb.Reason]error{
	pb.Reason_REASON_INVALID:         ErrInvalid,
	pb.Reason_REASON_UNKNOWN_NODE:    ErrNotFound,
	pb.Reason_REASON_UNKNOWN_LEASE:   ErrNotFound,
	pb.Reason_REASON_SESSION_EXPIRED: ErrSessionLost,
	pb.Reason_REASON_LEASE_EXPIRED:   ErrLeaseLost,
	pb.Reason_REASON_LEASE_REVOKED:   ErrLeaseLost,
	pb.Reason_REASON_NOT_OWNER:       ErrLeaseLost,
	pb.Reason_REASON_FORBIDDEN:       ErrForbidden,
	pb.Reason_REASON_CLOSED:          ErrTaskEnded,
	pb.Reason_REASON_BAD_API_KEY:     ErrBadAPIKey,
}

// fromStatus converts an error returned by an RPC into one of this package's errors.
func fromStatus(err error) error {
	st, ok := status.FromError(err)
	if err == nil || !ok {
		return err
	}
	for _, d := range st.Details() {
		detail, ok := d.(*pb.ErrorDetail)
		if !ok {
			continue
		}
		if den := detail.GetDenial(); den != nil {
			return &DeniedError{
				Node:            den.GetNodeId(),
				Name:            den.GetName(),
				Resource:        den.GetResource(),
				Used:            den.GetUsed(),
				Limit:           den.GetLimit(),
				Requested:       den.GetRequested(),
				TopConsumer:     den.GetTopConsumerId(),
				TopConsumerName: den.GetTopConsumerName(),
				TopConsumerUsed: den.GetTopConsumerUsed(),
			}
		}
		if kind, ok := reasons[detail.GetReason()]; ok {
			return &rpcError{kind: kind, msg: st.Message()}
		}
	}
	switch st.Code() {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		// Anything without a reason means governord could not give an answer.
		return &rpcError{kind: ErrUnavailable, msg: "governor: unavailable: " + st.Message()}
	}
}
