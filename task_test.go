package governor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Avik-creator/governor/internal/core"
)

// ended waits for ctx to be done and returns its cause.
func ended(t *testing.T, ctx context.Context) error {
	t.Helper()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(5 * time.Second):
		t.Fatal("context is still live")
		return nil
	}
}

// usage reads how much of r a node has consumed, by provoking a denial at its quota.
func (b *backend) usage(id uint64, r core.Resource) int64 {
	b.t.Helper()
	_, err := b.engine.Consume(b.admin, core.NodeID(id), r, 1<<62)
	d, ok := errors.AsType[*core.DeniedError](err)
	if !ok {
		b.t.Fatalf("probe of node %d = %v, want a denial", id, err)
	}
	return d.Used
}

func mustTask(t *testing.T, c *Client, spec Spec) (context.Context, *Task) {
	t.Helper()
	ctx, task, err := c.NewTask(t.Context(), spec)
	if err != nil {
		t.Fatalf("NewTask: %v", err)
	}
	return ctx, task
}

func TestTasksFormATree(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{"http": 100}})
	c := b.mustDial()
	ctx, task := mustTask(t, c, Spec{Name: "crawl", Quotas: map[string]int64{"http": 10}})
	subCtx, sub, err := Child(ctx, Spec{Name: "page"})
	if err != nil {
		t.Fatalf("Child: %v", err)
	}
	if FromContext(ctx) != task || FromContext(subCtx) != sub || FromContext(t.Context()) != nil {
		t.Error("FromContext did not return the task each context carries")
	}

	// A charge to the subtask counts against the task and the tenant too.
	if err := Consume(subCtx, "http", 7); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := b.usage(uint64(b.tenant), "http"); got != 7 {
		t.Errorf("tenant has used %d, want 7", got)
	}
	err = Consume(subCtx, "http", 4)
	d, ok := errors.AsType[*DeniedError](err)
	if !ok || !errors.Is(err, ErrDenied) {
		t.Fatalf("Consume past the task's quota = %v, want a DeniedError", err)
	}
	if d.Node != task.ID() || d.Name != "crawl" || d.Used != 7 || d.Limit != 10 || d.Requested != 4 ||
		d.TopConsumer != sub.ID() || d.TopConsumerName != "page" {
		t.Errorf("denial = %+v", *d)
	}
}

func TestWorkNeedsALiveTask(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	c := b.mustDial()

	if err := Consume(t.Context(), "http", 1); !errors.Is(err, ErrNoTask) {
		t.Errorf("Consume without a task = %v, want ErrNoTask", err)
	}
	if _, err := Acquire(t.Context(), "db"); !errors.Is(err, ErrNoTask) {
		t.Errorf("Acquire without a task = %v, want ErrNoTask", err)
	}
	if _, _, err := Child(t.Context(), Spec{}); !errors.Is(err, ErrNoTask) {
		t.Errorf("Child without a task = %v, want ErrNoTask", err)
	}

	ctx, _ := mustTask(t, c, Spec{})
	if err := Consume(ctx, "http", 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("Consume of zero = %v, want ErrInvalid", err)
	}
	if _, _, err := Child(ctx, Spec{Quotas: map[string]int64{"http": -1}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("Child with a negative quota = %v, want ErrInvalid", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := c.NewTask(t.Context(), Spec{}); !errors.Is(err, ErrClientClosed) {
		t.Errorf("NewTask on a closed client = %v, want ErrClientClosed", err)
	}
	if err := Consume(ctx, "http", 1); !errors.Is(err, ErrClientClosed) {
		t.Errorf("Consume after Close = %v, want ErrClientClosed", err)
	}
}

func TestTaskEnds(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
		end  func(t *testing.T, b *backend, task *Task)
	}{
		{"closed by its owner", Spec{}, func(t *testing.T, _ *backend, task *Task) {
			if err := task.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}},
		{"cancelled by its owner", Spec{}, func(t *testing.T, _ *backend, task *Task) {
			if err := task.Cancel(); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
		}},
		{"cancelled on the server", Spec{}, func(t *testing.T, b *backend, task *Task) {
			if _, err := b.engine.Cancel(b.admin, core.NodeID(task.ID())); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
		}},
		{"deadline passed", Spec{Deadline: time.Now().Add(100 * time.Millisecond)}, func(*testing.T, *backend, *Task) {}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackend(t, core.Spec{}, core.Spec{})
			c := b.mustDial()
			ctx, task := mustTask(t, c, tc.spec)
			subCtx, _, err := Child(ctx, Spec{})
			if err != nil {
				t.Fatalf("Child: %v", err)
			}

			tc.end(t, b, task)
			// The subtask ends with its parent, and for the same reason.
			for _, ctx := range []context.Context{ctx, subCtx} {
				if err := ended(t, ctx); !errors.Is(err, ErrTaskEnded) {
					t.Errorf("cause = %v, want ErrTaskEnded", err)
				}
			}
			if err := Consume(subCtx, "http", 1); !errors.Is(err, ErrTaskEnded) {
				t.Errorf("Consume on an ended task = %v, want ErrTaskEnded", err)
			}
			if c.Err() != nil {
				t.Errorf("client Err = %v, want the session to outlive the task", c.Err())
			}
		})
	}
}

func TestSessionLossEndsTasks(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	c := b.mustDial(WithSessionTTL(300 * time.Millisecond))
	ctx, _ := mustTask(t, c, Spec{})
	b.grpc.Stop()
	if err := ended(t, ctx); !errors.Is(err, ErrSessionLost) {
		t.Errorf("cause = %v, want ErrSessionLost", err)
	}
	// With governord gone and the session lost, work is refused without a call.
	if err := Consume(ctx, "http", 1); !errors.Is(err, ErrSessionLost) {
		t.Errorf("Consume = %v, want ErrSessionLost", err)
	}
}

func TestLease(t *testing.T) {
	b := newBackend(t, core.Spec{Limits: map[core.Class]int{"db": 1}}, core.Spec{})
	c := b.mustDial()
	ctx, task := mustTask(t, c, Spec{})

	lease, err := Acquire(ctx, "db")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lease.Validate(ctx); err != nil {
		t.Errorf("Validate: %v", err)
	}

	// The pool has one slot, so a second acquire waits until the first is released.
	waiting := make(chan *Lease, 1)
	go func() {
		l, err := Acquire(ctx, "db")
		if err != nil {
			t.Errorf("queued Acquire: %v", err)
		}
		waiting <- l
	}()
	select {
	case <-waiting:
		t.Fatal("second Acquire did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	for range 2 {
		if err := lease.Release(Report{}); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}
	second := <-waiting
	if second.ID() <= lease.ID() {
		t.Errorf("lease %d is not greater than the earlier lease %d", second.ID(), lease.ID())
	}
	if err := lease.Validate(ctx); !errors.Is(err, ErrNotFound) {
		t.Errorf("Validate(released) = %v, want ErrNotFound", err)
	}

	// A caller that gives up while waiting gets its own context's error.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := Acquire(short, "db"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Acquire that timed out = %v, want DeadlineExceeded", err)
	}

	// Ending the task revokes the lease and fails whoever is waiting for one.
	failed := make(chan error, 1)
	go func() {
		_, err := Acquire(ctx, "db")
		failed <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := task.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-failed; !errors.Is(err, ErrTaskEnded) {
		t.Errorf("Acquire on a task that ended = %v, want ErrTaskEnded", err)
	}
	if err := second.Release(Report{}); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Release(revoked) = %v, want ErrLeaseLost", err)
	}
}

func TestDo(t *testing.T) {
	var (
		mu      sync.Mutex
		reports []core.Report
	)
	b := newBackend(t, core.Spec{Limits: map[core.Class]int{"db": 1}}, core.Spec{})
	b.observe(func(_ core.Class, r core.Report) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, r)
	})
	c := b.mustDial()
	ctx, _ := mustTask(t, c, Spec{})

	boom := errors.New("query failed")
	err := Do(ctx, "db", func(context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return boom
	})
	if !errors.Is(err, boom) {
		t.Errorf("Do = %v, want the function's error", err)
	}
	// The lease went back even though the function failed.
	if err := Do(ctx, "db", func(context.Context) error { return nil }, WithMaxHold(time.Minute)); err != nil {
		t.Errorf("second Do: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 2 || reports[0].Latency < 20*time.Millisecond || reports[0].Overloaded {
		t.Errorf("reports = %+v, want two with the first at least 20ms", reports)
	}
}

func TestLostReplyIsNotAppliedTwice(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{"http": 100}})
	var dropped atomic.Int32
	// The request reaches governord, but its first reply is lost on the way back.
	dropFirstReply := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if method == "/governor.v1.GovernorService/Consume" && dropped.Add(1) == 1 {
			return status.Error(codes.Unavailable, "connection reset")
		}
		return err
	}
	c := b.mustDial(WithDialOptions(grpc.WithUnaryInterceptor(dropFirstReply)))
	ctx, _ := mustTask(t, c, Spec{})

	if err := Consume(ctx, "http", 5); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if dropped.Load() != 2 {
		t.Fatalf("Consume was sent %d times, want 2", dropped.Load())
	}
	if got := b.usage(uint64(b.tenant), "http"); got != 5 {
		t.Errorf("tenant has used %d, want 5", got)
	}
}
