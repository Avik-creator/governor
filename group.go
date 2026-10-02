package governor

import (
	"context"
	"sync"
)

// ClassAgents is the class a fan-out is usually bounded by.
const ClassAgents = "agents"

// Group runs functions as subtasks, each waiting for a lease before it starts.
type Group struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	class  string
	wg     sync.WaitGroup
	once   sync.Once
	err    error
}

// NewGroup returns a Group whose subtasks each hold a lease of class; an empty class holds none.
func NewGroup(ctx context.Context, class string) (*Group, context.Context) {
	g := &Group{class: class}
	g.ctx, g.cancel = context.WithCancelCause(ctx)
	return g, g.ctx
}

// Go runs fn as a subtask; fn must not wait for more leases of the group's class while it holds one.
func (g *Group) Go(spec Spec, fn func(context.Context) error) {
	g.wg.Go(func() {
		if err := g.run(spec, fn); err != nil {
			g.fail(err)
		}
	})
}

// fail records the first error and stops the rest of the group, as errgroup does.
func (g *Group) fail(err error) {
	g.once.Do(func() {
		g.err = err
		g.cancel(err)
	})
}

// run creates the subtask, runs fn under the group's lease and ends the subtask.
func (g *Group) run(spec Spec, fn func(context.Context) error) error {
	ctx, task, err := Child(g.ctx, spec)
	if err != nil {
		return err
	}
	if err := g.hold(ctx, fn); err != nil {
		_ = task.Cancel()
		return err
	}
	return task.Close()
}

// hold runs fn while holding a lease of the group's class, if it has one.
func (g *Group) hold(ctx context.Context, fn func(context.Context) error) error {
	if g.class == "" {
		return fn(ctx)
	}
	lease, err := Acquire(ctx, g.class)
	if err != nil {
		return err
	}
	defer lease.Release(Report{})
	// A function whose turn comes after the group has failed does not start.
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	err = fn(ctx)
	if err != nil {
		// The group is failed before the lease goes back, so the next waiter sees it.
		g.fail(err)
	}
	return err
}

// Wait blocks until every function has returned, and returns the first error.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel(nil)
	return g.err
}
