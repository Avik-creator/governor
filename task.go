package governor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

// watchRetry is how long a task waits before watching its node again after a failure.
const watchRetry = 200 * time.Millisecond

// Spec describes a task to create; the zero Spec has no limits of its own.
type Spec struct {
	// Name is a label shown in denials; it need not be unique.
	Name string

	// Quotas caps total consumption per resource by the task and its subtasks.
	Quotas map[string]int64

	// Limits caps leases held at once per class by the task and its subtasks.
	Limits map[string]int

	// Deadline is when the task ends; the zero time means none of its own.
	Deadline time.Time

	// Priority orders queued acquires within the tenant; higher goes first.
	Priority int
}

func (s Spec) proto() *pb.Spec {
	p := &pb.Spec{Name: s.Name, Quotas: s.Quotas, Priority: int64(s.Priority)}
	if !s.Deadline.IsZero() {
		p.Deadline = timestamppb.New(s.Deadline)
	}
	if len(s.Limits) > 0 {
		p.Limits = make(map[string]int64, len(s.Limits))
		for class, limit := range s.Limits {
			p.Limits[class] = int64(limit)
		}
	}
	return p
}

// Task is one node of the tree; the context returned with it carries it.
type Task struct {
	client *Client
	id     uint64
	ctx    context.Context // done once the task has ended, for any reason
	cancel context.CancelCauseFunc
}

type taskKey struct{}

// FromContext returns the task ctx carries, or nil if it carries none.
func FromContext(ctx context.Context) *Task {
	t, _ := ctx.Value(taskKey{}).(*Task)
	return t
}

// live returns the task ctx carries, or why no work may be done under ctx.
func live(ctx context.Context) (*Task, error) {
	t := FromContext(ctx)
	if t == nil {
		return nil, ErrNoTask
	}
	// A lost session ends its tasks, but their contexts learn of it a moment later.
	if err := t.client.Err(); err != nil {
		return nil, err
	}
	// A context that is done may not start work, whether the task or the caller ended it.
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	return t, nil
}

// NewTask creates a task directly under the client's tenant.
func (c *Client) NewTask(ctx context.Context, spec Spec) (context.Context, *Task, error) {
	return c.newTask(ctx, c.scope, spec)
}

// Child creates a subtask of the task ctx carries.
func Child(ctx context.Context, spec Spec) (context.Context, *Task, error) {
	parent, err := live(ctx)
	if err != nil {
		return nil, nil, err
	}
	return parent.client.newTask(ctx, parent.id, spec)
}

// newTask creates a node under parent and returns a context that ends with it.
func (c *Client) newTask(ctx context.Context, parent uint64, spec Spec) (context.Context, *Task, error) {
	if err := c.Err(); err != nil {
		return nil, nil, err
	}
	req := &pb.CreateNodeRequest{RequestId: newRequestID(), ParentId: parent, Spec: spec.proto()}
	resp, err := call(ctx, c, func(ctx context.Context) (*pb.CreateNodeResponse, error) {
		return c.rpc.CreateNode(ctx, req)
	})
	if err != nil {
		return nil, nil, err
	}

	t := &Task{client: c, id: resp.GetNodeId()}
	stopDeadline := func() {}
	if !spec.Deadline.IsZero() {
		// The deadline also runs locally, so it holds when governord cannot be reached.
		ctx, stopDeadline = context.WithDeadlineCause(ctx, spec.Deadline, ErrTaskEnded)
	}
	t.ctx, t.cancel = context.WithCancelCause(context.WithValue(ctx, taskKey{}, t))
	// Losing the session ends every task, since the server will end them too.
	stopSession := context.AfterFunc(c.ctx, func() { t.cancel(c.Err()) })
	context.AfterFunc(t.ctx, func() {
		stopDeadline()
		stopSession()
	})
	go t.watch()
	return t.ctx, t, nil
}

// ID returns the id of the task's node.
func (t *Task) ID() uint64 {
	return t.id
}

// Close ends the task as done and cancels its subtasks.
func (t *Task) Close() error {
	return t.end(func(ctx context.Context) error {
		_, err := t.client.rpc.CloseNode(ctx, &pb.CloseNodeRequest{NodeId: t.id})
		return err
	})
}

// Cancel ends the task and its subtasks as cancelled.
func (t *Task) Cancel() error {
	return t.end(func(ctx context.Context) error {
		_, err := t.client.rpc.CancelNode(ctx, &pb.CancelNodeRequest{NodeId: t.id})
		return err
	})
}

// end tells governord the task is over and cancels its context.
func (t *Task) end(rpc func(context.Context) error) error {
	// The task's own context may be done already, so the goodbye gets a fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	err := fromStatus(rpc(t.client.auth(ctx)))
	t.cancel(ErrTaskEnded)
	return err
}

// watch cancels the task's context when its node ends on the server.
func (t *Task) watch() {
	for {
		stream, err := t.client.rpc.WatchNode(t.client.auth(t.ctx), &pb.WatchNodeRequest{NodeId: t.id})
		if err == nil {
			var msg *pb.WatchNodeResponse
			if msg, err = stream.Recv(); err == nil {
				t.cancel(fmt.Errorf("%w: %s", ErrTaskEnded, msg.GetState()))
				return
			}
		}
		err = fromStatus(err)
		switch {
		case t.ctx.Err() != nil:
			return
		case errors.Is(err, ErrSessionLost), errors.Is(err, ErrNotFound), errors.Is(err, ErrForbidden):
			t.cancel(err)
			return
		}
		// governord may be restarting; the session's local deadline bounds this loop.
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(watchRetry):
		}
	}
}
