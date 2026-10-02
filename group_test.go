package governor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

func TestGroup(t *testing.T) {
	b := newBackend(t, core.Spec{Limits: map[core.Class]int{ClassAgents: 3}}, core.Spec{})
	ctx, task := mustTask(t, b.mustDial(), Spec{})

	var running, peak, done atomic.Int32
	g, _ := NewGroup(ctx, ClassAgents)
	for range 20 {
		g.Go(Spec{Name: "agent"}, func(ctx context.Context) error {
			now := running.Add(1)
			for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
			}
			time.Sleep(5 * time.Millisecond)
			running.Add(-1)
			done.Add(1)
			// Each function runs as its own subtask of the group's task.
			if sub := FromContext(ctx); sub == nil || sub == task {
				t.Error("function did not get a subtask of its own")
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if done.Load() != 20 || peak.Load() > 3 || peak.Load() < 2 {
		t.Errorf("%d functions ran with at most %d at once, want 20 and no more than 3", done.Load(), peak.Load())
	}
	children, err := b.engine.Children(b.admin, core.NodeID(task.ID()))
	if err != nil {
		t.Fatalf("Children: %v", err)
	}
	for _, c := range children {
		if c.State != core.StateDone {
			t.Errorf("subtask %d is %s after Wait, want done", c.ID, c.State)
		}
	}
	if len(children) != 20 {
		t.Errorf("task has %d subtasks, want 20", len(children))
	}
	if ctx.Err() != nil {
		t.Errorf("the task's context ended with the group: %v", context.Cause(ctx))
	}
}

func TestGroupStopsOnFirstError(t *testing.T) {
	b := newBackend(t, core.Spec{Limits: map[core.Class]int{ClassAgents: 2}}, core.Spec{})
	ctx, _ := mustTask(t, b.mustDial(), Spec{})
	boom := errors.New("agent failed")

	var started atomic.Int32
	g, groupCtx := NewGroup(ctx, ClassAgents)
	// The failing function must hold a slot before the others compete for them.
	holding, fail := make(chan struct{}), make(chan struct{})
	g.Go(Spec{}, func(context.Context) error {
		started.Add(1)
		close(holding)
		<-fail
		return boom
	})
	<-holding
	for range 10 {
		g.Go(Spec{}, func(ctx context.Context) error {
			started.Add(1)
			<-ctx.Done()
			return context.Cause(ctx)
		})
	}
	close(fail)
	if err := g.Wait(); !errors.Is(err, boom) {
		t.Errorf("Wait = %v, want the first error", err)
	}
	if !errors.Is(context.Cause(groupCtx), boom) {
		t.Errorf("group context cause = %v, want the first error", context.Cause(groupCtx))
	}
	// Only the two that held a slot when the failure happened ever ran.
	if got := started.Load(); got > 2 {
		t.Errorf("%d functions started, want at most the two that held a slot", got)
	}
	if ctx.Err() != nil {
		t.Errorf("the task's context ended with the group: %v", context.Cause(ctx))
	}

	// With no class the group only creates subtasks and holds no lease.
	free, _ := NewGroup(ctx, "")
	free.Go(Spec{}, func(context.Context) error { return nil })
	if err := free.Wait(); err != nil {
		t.Errorf("Wait on a group without a class: %v", err)
	}
}
