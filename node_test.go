package governor

import (
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

func TestNodeAndCaps(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{Name: "tenant", Quotas: map[core.Resource]int64{"http": 100}})
	c := b.mustDial()
	ctx := t.Context()

	defaults := &Defaults{Quotas: map[string]int64{"http": 2}, Children: &Defaults{Quotas: map[string]int64{"http": 1}}}
	if err := c.SetDefaults(ctx, 0, defaults); err != nil {
		t.Fatalf("SetDefaults: %v", err)
	}
	deadline := time.Now().Add(time.Hour).Truncate(time.Second)
	work, task := mustTask(t, c, Spec{Name: "job", Deadline: deadline})
	if err := Consume(work, "http", 2); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := Consume(work, "http", 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("Consume over the default cap = %v, want ErrDenied", err)
	}

	tenant, children, err := c.Node(ctx, 0)
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if tenant.ID != c.Scope() || tenant.Name != "tenant" || tenant.State != StateActive || tenant.Children != 1 ||
		tenant.Quotas["http"] != 100 || tenant.Used["http"] != 2 ||
		tenant.Defaults == nil || tenant.Defaults.Quotas["http"] != 2 || tenant.Defaults.Children.Quotas["http"] != 1 {
		t.Errorf("tenant = %+v", tenant)
	}
	if len(children) != 1 {
		t.Fatalf("Node returned %d children, want 1", len(children))
	}
	if job := children[0]; job.ID != task.ID() || job.Parent != tenant.ID || job.Name != "job" ||
		!job.Deadline.Equal(deadline) || !job.EndedAt.IsZero() || !maps.Equal(job.Quotas, map[string]int64{"http": 2}) {
		t.Errorf("job = %+v", job)
	}

	// A raised cap lets the task carry on; a removed one leaves only the tenant's.
	if err := c.SetQuota(ctx, task.ID(), "http", 3); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if err := Consume(work, "http", 1); err != nil {
		t.Fatalf("Consume after the cap was raised: %v", err)
	}
	if err := c.SetQuota(ctx, task.ID(), "http", Unlimited); err != nil {
		t.Fatalf("SetQuota(Unlimited): %v", err)
	}
	if err := Consume(work, "http", 50); err != nil {
		t.Fatalf("Consume with the cap removed: %v", err)
	}

	if err := c.SetLimit(ctx, task.ID(), "db", 1); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	lease, err := Acquire(work, "db")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if job, _, err := c.Node(ctx, task.ID()); err != nil || job.Limits["db"] != 1 || job.Held["db"] != 1 {
		t.Errorf("Node(job) = %+v, %v, want one db lease held of one", job, err)
	}
	if err := lease.Release(Report{}); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// A client cannot loosen its own tenant, and an unknown node is reported as such.
	if err := c.SetQuota(ctx, c.Scope(), "http", 1000); !errors.Is(err, ErrForbidden) {
		t.Errorf("SetQuota on the client's own tenant = %v, want ErrForbidden", err)
	}
	if _, _, err := c.Node(ctx, 999); !errors.Is(err, ErrNotFound) {
		t.Errorf("Node(unknown) = %v, want ErrNotFound", err)
	}

	// Cancelling the node from outside ends the task's context.
	if err := c.CancelNode(ctx, task.ID()); err != nil {
		t.Fatalf("CancelNode: %v", err)
	}
	if err := ended(t, work); !errors.Is(err, ErrTaskEnded) {
		t.Errorf("task context ended with %v, want ErrTaskEnded", err)
	}
	if job, _, err := c.Node(ctx, task.ID()); err != nil || job.State != StateCancelled || job.EndedAt.IsZero() {
		t.Errorf("Node(job) after the cancel = %+v, %v", job, err)
	}
	if err := c.SetDefaults(ctx, 0, nil); err != nil {
		t.Fatalf("SetDefaults(nil): %v", err)
	}
	if tenant, _, _ := c.Node(ctx, 0); tenant.Defaults != nil {
		t.Errorf("defaults after clearing = %+v, want none", tenant.Defaults)
	}
}
