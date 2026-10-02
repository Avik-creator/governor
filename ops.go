package governor

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

// Limits on how a call is repeated when governord cannot be reached.
const (
	callAttempts = 3
	callBackoff  = 50 * time.Millisecond
)

// newRequestID returns an id that makes a repeated request apply only once.
func newRequestID() string {
	return rand.Text()
}

// call runs an RPC, repeating it while governord is unreachable; the request must carry a request id.
func call[T any](ctx context.Context, c *Client, rpc func(context.Context) (T, error)) (T, error) {
	var (
		resp T
		err  error
	)
	for attempt := range callAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return resp, context.Cause(ctx)
			case <-time.After(callBackoff << (attempt - 1)):
			}
		}
		resp, err = rpc(c.auth(ctx))
		if status.Code(err) != codes.Unavailable {
			break
		}
	}
	return resp, fromStatus(err)
}

// Consume charges n units of a quota to the task ctx carries and to all its ancestors.
func Consume(ctx context.Context, resource string, n int64) error {
	t, err := live(ctx)
	if err != nil {
		return err
	}
	req := &pb.ConsumeRequest{RequestId: newRequestID(), NodeId: t.id, Resource: resource, Amount: n}
	_, err = call(ctx, t.client, func(ctx context.Context) (*pb.ConsumeResponse, error) {
		return t.client.rpc.Consume(ctx, req)
	})
	return err
}

// AcquireOption adjusts one Acquire.
type AcquireOption func(*pb.AcquireRequest)

// WithMaxHold makes the lease expire after d even if it is never released.
func WithMaxHold(d time.Duration) AcquireOption {
	return func(req *pb.AcquireRequest) { req.MaxHold = durationpb.New(d) }
}

// Report is what the holder of a lease says about the work it guarded.
type Report struct {
	// Latency is how long the work took; zero means the time the lease was held.
	Latency time.Duration

	// Overloaded says the downstream signalled overload, such as HTTP 429 or 503.
	Overloaded bool
}

// Lease is held capacity of one class; it must be released.
type Lease struct {
	client   *Client
	id       uint64
	acquired time.Time
	once     sync.Once
	err      error
}

// Acquire waits for a lease of class for the task ctx carries.
func Acquire(ctx context.Context, class string, opts ...AcquireOption) (*Lease, error) {
	t, err := live(ctx)
	if err != nil {
		return nil, err
	}
	req := &pb.AcquireRequest{RequestId: newRequestID(), NodeId: t.id, Class: class}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := call(ctx, t.client, func(ctx context.Context) (*pb.AcquireResponse, error) {
		return t.client.rpc.Acquire(ctx, req)
	})
	if err != nil {
		// A context that ended while waiting is reported by its cause, such as ErrTaskEnded.
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		return nil, err
	}
	return &Lease{client: t.client, id: resp.GetLeaseId(), acquired: time.Now()}, nil
}

// ID returns the lease's fencing token: a larger id is always a later grant.
func (l *Lease) ID() uint64 {
	return l.id
}

// Release returns the lease; only the first call has an effect.
func (l *Lease) Release(r Report) error {
	l.once.Do(func() {
		if r.Latency == 0 {
			r.Latency = time.Since(l.acquired)
		}
		// The caller's context may be done already, and the lease must still go back.
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		_, err := l.client.rpc.Release(l.client.auth(ctx), &pb.ReleaseRequest{
			LeaseId:    l.id,
			Latency:    durationpb.New(r.Latency),
			Overloaded: r.Overloaded,
		})
		l.err = fromStatus(err)
	})
	return l.err
}

// Validate asks governord whether the lease is still held, before irreversible work.
func (l *Lease) Validate(ctx context.Context) error {
	_, err := l.client.rpc.Validate(ctx, &pb.ValidateRequest{LeaseId: l.id})
	return fromStatus(err)
}

// Do runs fn while holding a lease of class, and reports how long it took.
func Do(ctx context.Context, class string, fn func(context.Context) error, opts ...AcquireOption) error {
	lease, err := Acquire(ctx, class, opts...)
	if err != nil {
		return err
	}
	err = fn(ctx)
	if releaseErr := lease.Release(Report{}); err == nil {
		err = releaseErr
	}
	return err
}
