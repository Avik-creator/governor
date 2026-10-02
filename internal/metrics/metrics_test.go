package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Avik-creator/governor/internal/core"
)

// recorder is a Sink that keeps the kind of every event it is given.
type recorder struct {
	kinds []core.EventKind
}

func (r *recorder) Emit(ev core.Event) { r.kinds = append(r.kinds, ev.Kind) }

// newEngine returns an engine that emits through the metrics, and a session on its root.
func newEngine(t *testing.T, m *Metrics, next core.Sink, root core.Spec) (*core.Engine, core.SessionID) {
	t.Helper()
	e, err := core.New(core.Config{Sink: m.Sink(next), Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sid, _, _, err := e.OpenSession(core.RootID, 0)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return e, sid
}

func mustNode(t *testing.T, e *core.Engine, sid core.SessionID, parent core.NodeID, spec core.Spec) core.NodeID {
	t.Helper()
	id, _, err := e.CreateNode(sid, parent, spec)
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	return id
}

func TestTreeIsCollected(t *testing.T) {
	m := New()
	root := core.Spec{Quotas: map[core.Resource]int64{"http": 100}, Limits: map[core.Class]int{"db": 4}}
	e, sid := newEngine(t, m, nil, root)
	a := mustNode(t, e, sid, core.RootID, core.Spec{Name: "a", Quotas: map[core.Resource]int64{"http": 10}})
	b := mustNode(t, e, sid, core.RootID, core.Spec{Name: "b", Limits: map[core.Class]int{"db": 1}})
	// Two tenants with one name must not produce the same series twice.
	twin := mustNode(t, e, sid, core.RootID, core.Spec{Name: "b"})
	task := mustNode(t, e, sid, a, core.Spec{Name: "task"})
	for node, amount := range map[core.NodeID]int64{task: 3, b: 2, twin: 5} {
		if _, err := e.Consume(sid, node, "http", amount); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	if _, _, err := e.Acquire(t.Context(), sid, b, "db", core.AcquireOptions{}); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := m.Watch(e); err != nil {
		t.Fatalf("Watch: %v", err)
	}

	want := `
# HELP governor_nodes Nodes in the tree, ended ones included until they are removed.
# TYPE governor_nodes gauge
governor_nodes 5
# HELP governor_root_lease_limit The size of a shared pool: the root's cap on a class.
# TYPE governor_root_lease_limit gauge
governor_root_lease_limit{class="db"} 4
# HELP governor_root_leases_held Leases held in the whole tree, by class.
# TYPE governor_root_leases_held gauge
governor_root_leases_held{class="db"} 1
# HELP governor_root_quota_limit The root's cap on a resource.
# TYPE governor_root_quota_limit gauge
governor_root_quota_limit{resource="http"} 100
# HELP governor_root_quota_used_total Units consumed in the whole tree, by resource.
# TYPE governor_root_quota_used_total counter
governor_root_quota_used_total{resource="http"} 10
# HELP governor_sessions Open sessions.
# TYPE governor_sessions gauge
governor_sessions 1
# HELP governor_tenant_lease_limit A tenant's cap on a class.
# TYPE governor_tenant_lease_limit gauge
governor_tenant_lease_limit{class="db",tenant="b"} 1
# HELP governor_tenant_leases_held Leases a tenant holds, by class.
# TYPE governor_tenant_leases_held gauge
governor_tenant_leases_held{class="db",tenant="b"} 1
# HELP governor_tenant_quota_limit A tenant's cap on a resource.
# TYPE governor_tenant_quota_limit gauge
governor_tenant_quota_limit{resource="http",tenant="a"} 10
# HELP governor_tenant_quota_used_total Units a tenant has consumed, by resource.
# TYPE governor_tenant_quota_used_total counter
governor_tenant_quota_used_total{resource="http",tenant="a"} 3
governor_tenant_quota_used_total{resource="http",tenant="b"} 7
`
	names := []string{
		"governor_nodes", "governor_sessions", "governor_waiting_acquires",
		"governor_root_quota_used_total", "governor_root_quota_limit",
		"governor_root_leases_held", "governor_root_lease_limit",
		"governor_tenant_quota_used_total", "governor_tenant_quota_limit",
		"governor_tenant_leases_held", "governor_tenant_lease_limit",
	}
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(want), names...); err != nil {
		t.Error(err)
	}
	// Nothing in the output may name a task, or the series would grow with the work.
	if n, err := testutil.GatherAndCount(m.registry, names...); err != nil || n != 11 {
		t.Errorf("gathered %d series (%v), want 11", n, err)
	}
}

func TestWaitingIsCollected(t *testing.T) {
	m := New()
	e, sid := newEngine(t, m, nil, core.Spec{Limits: map[core.Class]int{"db": 1}})
	if err := m.Watch(e); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if _, _, err := e.Acquire(t.Context(), sid, core.RootID, "db", core.AcquireOptions{}); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = e.Acquire(ctx, sid, core.RootID, "db", core.AcquireOptions{})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().Waiting["db"] != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the second acquire never queued")
		}
		time.Sleep(time.Millisecond)
	}
	want := `
# HELP governor_waiting_acquires Acquires queued for a lease, by class.
# TYPE governor_waiting_acquires gauge
governor_waiting_acquires{class="db"} 1
`
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(want), "governor_waiting_acquires"); err != nil {
		t.Error(err)
	}
	cancel()
	<-done
}

func TestSinkCountsAndPassesOn(t *testing.T) {
	m := New()
	next := &recorder{}
	e, sid := newEngine(t, m, next, core.Spec{})
	if _, err := e.Consume(sid, core.RootID, "http", 1); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := e.Consume(sid, core.RootID, "http", 1); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	want := map[core.EventKind]float64{core.EventNodeCreated: 1, core.EventSessionOpened: 1, core.EventConsumed: 2}
	for kind, n := range want {
		if got := testutil.ToFloat64(m.events.WithLabelValues(string(kind))); got != n {
			t.Errorf("events of kind %s = %v, want %v", kind, got, n)
		}
	}
	if len(next.kinds) != 4 {
		t.Errorf("the next sink got %d events, want 4", len(next.kinds))
	}
}

func TestInterceptorsCountByMethodAndCode(t *testing.T) {
	m := New()
	denied := status.Error(codes.ResourceExhausted, "over budget")
	info := &grpc.UnaryServerInfo{FullMethod: "/governor.v1.GovernorService/Consume"}
	for _, err := range []error{nil, nil, denied} {
		_, got := m.UnaryInterceptor(t.Context(), nil, info, func(context.Context, any) (any, error) { return nil, err })
		if got != err {
			t.Errorf("the interceptor returned %v, want the handler's %v", got, err)
		}
	}
	stream := &grpc.StreamServerInfo{FullMethod: "/governor.v1.GovernorService/WatchNode"}
	if err := m.StreamInterceptor(nil, nil, stream, func(any, grpc.ServerStream) error { return nil }); err != nil {
		t.Errorf("StreamInterceptor: %v", err)
	}

	want := map[[2]string]float64{
		{"Consume", "OK"}:                2,
		{"Consume", "ResourceExhausted"}: 1,
		{"WatchNode", "OK"}:              1,
	}
	for labels, n := range want {
		if got := testutil.ToFloat64(m.requests.WithLabelValues(labels[0], labels[1])); got != n {
			t.Errorf("requests%v = %v, want %v", labels, got, n)
		}
	}
	// Only unary calls are timed: one histogram, for Consume.
	if n := testutil.CollectAndCount(m.duration); n != 1 {
		t.Errorf("timed %d methods, want 1", n)
	}
}

func TestObserveCommit(t *testing.T) {
	m := New()
	m.ObserveCommit(7, 3*time.Millisecond)
	m.ObserveCommit(1, time.Millisecond)
	want := `
# HELP governor_commit_batch_events How many events each transaction recorded.
# TYPE governor_commit_batch_events histogram
governor_commit_batch_events_bucket{le="1"} 1
governor_commit_batch_events_bucket{le="2"} 1
governor_commit_batch_events_bucket{le="4"} 1
governor_commit_batch_events_bucket{le="8"} 2
governor_commit_batch_events_bucket{le="16"} 2
governor_commit_batch_events_bucket{le="32"} 2
governor_commit_batch_events_bucket{le="64"} 2
governor_commit_batch_events_bucket{le="128"} 2
governor_commit_batch_events_bucket{le="256"} 2
governor_commit_batch_events_bucket{le="512"} 2
governor_commit_batch_events_bucket{le="1024"} 2
governor_commit_batch_events_bucket{le="+Inf"} 2
governor_commit_batch_events_sum 8
governor_commit_batch_events_count 2
`
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(want), "governor_commit_batch_events"); err != nil {
		t.Error(err)
	}
}
