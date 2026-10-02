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

// Go runs fn in a goroutine as a subtask described by spec.
func (g *Group) Go(spec Spec, fn func(context.Context) error) {
	g.wg.Go(func() {
		if err := g.run(spec, fn); err != nil {
			// The first failure stops the rest, as in errgroup.
			g.once.Do(func() {
				g.err = err
				g.cancel(err)
			})
		}
	})
}

// run creates the subtask, waits for its lease, runs fn and ends the subtask.
func (g *Group) run(spec Spec, fn func(context.Context) error) error {
	ctx, task, err := Child(g.ctx, spec)
	if err != nil {
		return err
	}
	if g.class == "" {
		err = fn(ctx)
	} else {
		err = Do(ctx, g.class, fn)
	}
	if err != nil {
		_ = task.Cancel()
		return err
	}
	return task.Close()
}

// Wait blocks until every function has returned, and returns the first error.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel(nil)
	return g.err
}
