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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
	"github.com/Avik-creator/governor/internal/store"
)

// instance is a governord running in this process until the test ends or stop is called.
type instance struct {
	client pb.GovernorServiceClient
	stop   func()
}

// start runs governord with the given database and waits until it serves.
func start(t *testing.T, databaseURL string) *instance {
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
snapshot_every: 4
admin_key: admin
root:
  limits: {db: 2, slow: 4}
adaptive:
  - class: slow
    target_p95: 10ms
    min_limit: 1
    max_limit: 4
    interval: 10ms
    min_samples: 1
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
	d := &instance{client: pb.NewGovernorServiceClient(conn), stop: stop}

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

	// The adaptive controller cuts the limit of a class whose leases report slow work.
	worker := as(t, a.GetSessionToken())
	hold := func(ctx context.Context) (*pb.AcquireResponse, error) {
		return d.client.Acquire(ctx, &pb.AcquireRequest{NodeId: a.GetScopeId(), Class: "slow"})
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		first, err := hold(worker)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		// With the limit down to one, a second lease cannot be had while the first is held.
		short, cancel := context.WithTimeout(worker, 30*time.Millisecond)
		second, err := hold(short)
		cancel()
		release := &pb.ReleaseRequest{LeaseId: first.GetLeaseId(), Latency: durationpb.New(time.Second)}
		if _, relErr := d.client.Release(worker, release); relErr != nil {
			t.Fatalf("Release: %v", relErr)
		}
		if status.Code(err) == codes.DeadlineExceeded {
			break
		}
		if err != nil {
			t.Fatalf("second Acquire: %v", err)
		}
		release = &pb.ReleaseRequest{LeaseId: second.GetLeaseId(), Latency: durationpb.New(time.Second)}
		if _, err := d.client.Release(worker, release); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the slow class still admits two leases at once")
		}
	}

	d.stop()
}

func TestRunSurvivesRestart(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "governor.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatalf("sql.Open: %v", err)
		}
		defer db.Close()
		survivesRestart(t, store.SQLiteScheme+path, db)
	})
	t.Run("postgres", func(t *testing.T) {
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
		for _, table := range []string{"events", "session_tokens", "snapshot"} {
			_, _ = db.ExecContext(t.Context(), "TRUNCATE "+table)
		}
		survivesRestart(t, dsn, db)
	})
}

// survivesRestart checks that a governord recording to dsn comes back as it was; db reads the same database.
func survivesRestart(t *testing.T, dsn string, db *sql.DB) {
	t.Helper()
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
	// Caps and defaults changed over the API are part of the record too.
	admin, err := d.client.OpenSession(as(t, "admin"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession as admin: %v", err)
	}
	owner := as(t, admin.GetSessionToken())
	defaults := &pb.Defaults{Quotas: map[string]int64{"tool_calls": 5}}
	if _, err := d.client.SetDefaults(owner, &pb.SetDefaultsRequest{NodeId: tenant, Defaults: defaults}); err != nil {
		t.Fatalf("SetDefaults: %v", err)
	}
	raise := &pb.SetQuotaRequest{NodeId: tenant, Resource: "http", Limit: proto.Int64(11)}
	if _, err := d.client.SetQuota(owner, raise); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	// The restart must come back from a snapshot plus the events recorded after it.
	deadline := time.Now().Add(5 * time.Second)
	for snapshotSeq := 0; snapshotSeq == 0; {
		_ = db.QueryRowContext(t.Context(), "SELECT seq FROM snapshot").Scan(&snapshotSeq)
		if time.Now().After(deadline) {
			t.Fatal("no snapshot was saved")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := d.client.Consume(worker, &pb.ConsumeRequest{NodeId: tenant, Resource: "http", Amount: 1}); err != nil {
		t.Fatalf("Consume after the snapshot: %v", err)
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
		t.Errorf("Consume of 4 after 8 of 11 = %v, want ResourceExhausted", err)
	}
	if _, err := d.client.Consume(worker, &pb.ConsumeRequest{NodeId: tenant, Resource: "http", Amount: 3}); err != nil {
		t.Errorf("Consume of 3 after 8 of 11: %v", err)
	}
	node, err := d.client.GetNode(owner, &pb.GetNodeRequest{NodeId: tenant})
	if err != nil || !proto.Equal(node.GetNode().GetDefaults(), defaults) || node.GetNode().GetUsed()["http"] != 11 {
		t.Errorf("GetNode after the restart = %v, %v, want the defaults and 11 used", node, err)
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
