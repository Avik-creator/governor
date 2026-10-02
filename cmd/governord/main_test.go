package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/avikmukherjee/governor/internal/config"
	"github.com/avikmukherjee/governor/internal/core"
	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

// eventCounter is a Sink that counts events by kind.
type eventCounter map[core.EventKind]int

func (c eventCounter) Emit(ev core.Event) { c[ev.Kind]++ }

func TestReconcile(t *testing.T) {
	events := eventCounter{}
	engine, err := core.New(core.Config{Sink: events})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg := &config.Config{
		AdminKey: "admin",
		Root:     config.Caps{Limits: map[core.Class]int{"db": 4}},
		Tenants: []config.Tenant{
			{Name: "a", APIKey: "key-a", Weight: 2, Caps: config.Caps{Limits: map[core.Class]int{"db": 1}}},
			{Name: "b", APIKey: "key-b"},
		},
	}

	first, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	a, b := first["key-a"], first["key-b"]
	if first["admin"] != core.RootID || a == 0 || b == 0 || a == b || len(first) != 3 {
		t.Fatalf("keys = %v, want the root and two distinct tenants", first)
	}
	if len(engine.Sessions()) != 0 {
		t.Errorf("reconcile left %d sessions open", len(engine.Sessions()))
	}

	// A second start with the same configuration finds everything in place.
	created, limits := events[core.EventNodeCreated], events[core.EventLimitChanged]
	second, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if second["key-a"] != a || second["key-b"] != b {
		t.Errorf("keys = %v after a restart, want the same nodes %d and %d", second, a, b)
	}
	if events[core.EventNodeCreated] != created || events[core.EventLimitChanged] != limits {
		t.Errorf("an unchanged configuration recorded new nodes or limits: %v", events)
	}

	// A changed limit is applied to the tenant that already exists.
	cfg.Tenants[0].Limits["db"] = 3
	if _, _, err := reconcile(engine, cfg); err != nil {
		t.Fatalf("reconcile with a new limit: %v", err)
	}
	if events[core.EventLimitChanged] != limits+1 || events[core.EventNodeCreated] != created {
		t.Errorf("a changed limit recorded %v", events)
	}

	// A tenant whose node has ended gets a fresh one.
	admin, _, _, _ := engine.OpenSession(core.RootID, 0)
	if _, err := engine.Cancel(admin, b); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := engine.CloseSession(admin); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	third, _, err := reconcile(engine, cfg)
	if err != nil {
		t.Fatalf("reconcile after a tenant ended: %v", err)
	}
	if third["key-b"] == b || third["key-a"] != a {
		t.Errorf("keys = %v, want a kept at %d and b moved off %d", third, a, b)
	}
}

// daemon is a governord running in this process until the test ends or stop is called.
type daemon struct {
	client pb.GovernorServiceClient
	stop   func()
}

// start runs governord with the given database and waits until it serves.
func start(t *testing.T, databaseURL string) *daemon {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	path := filepath.Join(t.TempDir(), "governor.yaml")
	file := fmt.Sprintf(`
listen: %s
database_url: %q
reap_interval: 10ms
admin_key: admin
root:
  limits: {db: 2}
tenants:
  - name: team-a
    api_key: key-a
    quotas: {http: 10}
  - name: team-b
    api_key: key-b
`, addr, databaseURL)
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- run(ctx, []string{"-config", path}, io.Discard) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-exited; err != nil {
			t.Errorf("run: %v", err)
		}
	}
	t.Cleanup(stop)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	d := &daemon{client: pb.NewGovernorServiceClient(conn), stop: stop}

	// Validate needs no credential, so it shows when the server is up.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := d.client.Validate(t.Context(), &pb.ValidateRequest{LeaseId: 1})
		if status.Code(err) == codes.NotFound {
			return d
		}
		select {
		case err := <-exited:
			t.Fatalf("run exited before serving: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("governord is not serving: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func as(t *testing.T, secret string) context.Context {
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+secret)
}

func TestRunServes(t *testing.T) {
	d := start(t, "")

	admin, err := d.client.OpenSession(as(t, "admin"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession(admin): %v", err)
	}
	if admin.GetScopeId() != uint64(core.RootID) {
		t.Errorf("admin scope = %d, want the root", admin.GetScopeId())
	}
	a, err := d.client.OpenSession(as(t, "key-a"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession(key-a): %v", err)
	}
	b, err := d.client.OpenSession(as(t, "key-b"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession(key-b): %v", err)
	}
	if a.GetScopeId() == b.GetScopeId() || a.GetScopeId() == uint64(core.RootID) {
		t.Errorf("tenant scopes = %d and %d, want two nodes under the root", a.GetScopeId(), b.GetScopeId())
	}

	// The configured quota of 10 binds, and tenant b cannot reach tenant a.
	charge := &pb.ConsumeRequest{NodeId: a.GetScopeId(), Resource: "http", Amount: 6}
	if _, err := d.client.Consume(as(t, a.GetSessionToken()), charge); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := d.client.Consume(as(t, a.GetSessionToken()), charge); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("Consume past the quota = %v, want ResourceExhausted", err)
	}
	if _, err := d.client.Consume(as(t, b.GetSessionToken()), charge); status.Code(err) != codes.PermissionDenied {
		t.Errorf("Consume on another tenant = %v, want PermissionDenied", err)
	}

	// The reaper runs, so a session that stops heartbeating loses its token.
	short, err := d.client.OpenSession(as(t, "key-a"), &pb.OpenSessionRequest{Ttl: durationpb.New(50 * time.Millisecond)})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	_, err = d.client.Heartbeat(as(t, short.GetSessionToken()), &pb.HeartbeatRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("Heartbeat after the ttl = %v, want Unauthenticated", err)
	}

	d.stop()
}

func TestRunSurvivesRestart(t *testing.T) {
	dsn := os.Getenv("GOVERNOR_TEST_DSN")
	if dsn == "" {
		t.Skip("GOVERNOR_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	// The tables may not exist yet on a database no store has opened.
	_, _ = db.ExecContext(t.Context(), "TRUNCATE events, session_tokens")

	d := start(t, dsn)
	session, err := d.client.OpenSession(as(t, "key-a"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	worker, tenant := as(t, session.GetSessionToken()), session.GetScopeId()
	if _, err := d.client.Consume(worker, &pb.ConsumeRequest{NodeId: tenant, Resource: "http", Amount: 7}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	lease, err := d.client.Acquire(worker, &pb.AcquireRequest{NodeId: tenant, Class: "db"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	d.stop()

	d = start(t, dsn)
	// The same token, the same tenant node, the same usage and the same lease.
	again, err := d.client.OpenSession(as(t, "key-a"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession after the restart: %v", err)
	}
	if again.GetScopeId() != tenant {
		t.Errorf("tenant node = %d after the restart, want %d", again.GetScopeId(), tenant)
	}
	_, err = d.client.Consume(worker, &pb.ConsumeRequest{NodeId: tenant, Resource: "http", Amount: 4})
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("Consume of 4 after 7 of 10 = %v, want ResourceExhausted", err)
	}
	if _, err := d.client.Validate(t.Context(), &pb.ValidateRequest{LeaseId: lease.GetLeaseId()}); err != nil {
		t.Errorf("Validate(lease from before the restart): %v", err)
	}
	if _, err := d.client.Release(worker, &pb.ReleaseRequest{LeaseId: lease.GetLeaseId()}); err != nil {
		t.Errorf("Release(lease from before the restart): %v", err)
	}
}

func TestRunRejects(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"-nope"}},
		{"missing file", []string{"-config", filepath.Join(t.TempDir(), "missing.yaml")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(t.Context(), tc.args, io.Discard); err == nil {
				t.Error("run succeeded")
			}
		})
	}
}
