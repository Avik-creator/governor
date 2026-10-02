package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/avikmukherjee/governor/internal/core"
	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
)

const (
	keyA = "key-tenant-a"
	keyB = "key-tenant-b"
)

// stubCommitter blocks while a gate is set and then returns its error.
type stubCommitter struct {
	mu   sync.Mutex
	err  error
	gate chan struct{}
}

func (c *stubCommitter) WaitDurable(ctx context.Context, _ uint64) error {
	c.mu.Lock()
	err, gate := c.err, c.gate
	c.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *stubCommitter) set(err error, gate chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err, c.gate = err, gate
}

// memTokens is a TokenStore held in memory, standing in for Postgres.
type memTokens struct {
	mu     sync.Mutex
	tokens map[[sha256.Size]byte]core.SessionID
	err    error // returned by SaveToken when set
}

func (m *memTokens) SaveToken(_ context.Context, hash [sha256.Size]byte, sid core.SessionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.tokens[hash] = sid
	return nil
}

func (m *memTokens) DeleteToken(_ context.Context, hash [sha256.Size]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tokens, hash)
	return nil
}

func (m *memTokens) LoadTokens(context.Context) (map[[sha256.Size]byte]core.SessionID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.tokens), nil
}

func (m *memTokens) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tokens)
}

// eventLog is a Sink that keeps every event, standing in for the store.
type eventLog struct {
	mu     sync.Mutex
	events []core.Event
}

func (l *eventLog) Emit(ev core.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

// replay yields the logged events the way the store would after a restart.
func (l *eventLog) replay() iter.Seq2[core.Event, error] {
	l.mu.Lock()
	events := slices.Clone(l.events)
	l.mu.Unlock()
	return func(yield func(core.Event, error) bool) {
		for _, ev := range events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// harness is a Server behind an in-memory gRPC connection, with two tenants.
type harness struct {
	t      *testing.T
	engine *core.Engine
	clock  *core.ManualClock
	commit *stubCommitter
	tokens *memTokens
	log    *eventLog
	server *Server
	client pb.GovernorServiceClient
	admin  core.SessionID
	a, b   core.NodeID
}

func newHarness(t *testing.T, root, tenant core.Spec) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  core.NewManualClock(time.Unix(1_700_000_000, 0)),
		commit: &stubCommitter{},
		tokens: &memTokens{tokens: make(map[[sha256.Size]byte]core.SessionID)},
		log:    &eventLog{},
	}
	var err error
	if h.engine, err = core.New(core.Config{Clock: h.clock, Sink: h.log, Root: root}); err != nil {
		t.Fatalf("core.New: %v", err)
	}
	// The server closes this session when it starts, as it holds no token for it.
	setup, _, _, err := h.engine.OpenSession(core.RootID, 0)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	for _, id := range []*core.NodeID{&h.a, &h.b} {
		if *id, _, err = h.engine.CreateNode(setup, core.RootID, tenant); err != nil {
			t.Fatalf("CreateNode: %v", err)
		}
	}
	h.serve()
	return h
}

// restart replaces the engine with one restored from the log, behind a new server.
func (h *harness) restart() {
	h.t.Helper()
	h.server.Close()
	engine, err := core.Restore(core.Config{Clock: h.clock, Sink: h.log}, h.log.replay())
	if err != nil {
		h.t.Fatalf("Restore: %v", err)
	}
	h.engine = engine
	h.serve()
}

// serve starts a Server for the harness's engine behind an in-memory connection.
func (h *harness) serve() {
	h.t.Helper()
	t := h.t
	var err error
	h.server, err = New(t.Context(), Config{
		Engine:    h.engine,
		Committer: h.commit,
		Tokens:    h.tokens,
		Keys:      map[string]core.NodeID{keyA: h.a, keyB: h.b},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// In-process sessions are opened after the server has adopted the engine.
	if h.admin, _, _, err = h.engine.OpenSession(core.RootID, 0); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(h.server.UnaryInterceptor),
		grpc.StreamInterceptor(h.server.StreamInterceptor),
	)
	pb.RegisterGovernorServiceServer(srv, h.server)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	server := h.server
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		server.Close()
	})
	h.client = pb.NewGovernorServiceClient(conn)
}

// as returns a context that carries secret as the bearer credential.
func (h *harness) as(secret string) context.Context {
	return metadata.AppendToOutgoingContext(h.t.Context(), "authorization", "Bearer "+secret)
}

// open opens a session with an API key and returns its token.
func (h *harness) open(key string, ttl time.Duration) string {
	h.t.Helper()
	resp, err := h.client.OpenSession(h.as(key), &pb.OpenSessionRequest{Ttl: durationpb.New(ttl)})
	if err != nil {
		h.t.Fatalf("OpenSession: %v", err)
	}
	return resp.GetSessionToken()
}

// node creates a child of parent as the session named by token.
func (h *harness) node(token string, parent core.NodeID) uint64 {
	h.t.Helper()
	resp, err := h.client.CreateNode(h.as(token), &pb.CreateNodeRequest{ParentId: uint64(parent)})
	if err != nil {
		h.t.Fatalf("CreateNode: %v", err)
	}
	return resp.GetNodeId()
}

// used reads a node's usage of r by provoking a denial; the node needs a quota for r.
func (h *harness) used(id core.NodeID, r core.Resource) int64 {
	h.t.Helper()
	_, err := h.engine.Consume(h.admin, id, r, math.MaxInt64)
	d, ok := errors.AsType[*core.DeniedError](err)
	if !ok {
		h.t.Fatalf("probe of node %d = %v, want a denial", id, err)
	}
	return d.Used
}

func (h *harness) live() int {
	h.server.mu.Lock()
	defer h.server.mu.Unlock()
	return len(h.server.sessions)
}

// eventually waits for cond, which becomes true on another goroutine.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// wantStatus checks the gRPC code and the reason carried in the ErrorDetail.
func wantStatus(t *testing.T, err error, code codes.Code, reason pb.Reason) *pb.ErrorDetail {
	t.Helper()
	st := status.Convert(err)
	detail := &pb.ErrorDetail{}
	for _, d := range st.Details() {
		if e, ok := d.(*pb.ErrorDetail); ok {
			detail = e
		}
	}
	if st.Code() != code || detail.GetReason() != reason {
		t.Errorf("got %s %s (%q), want %s %s", st.Code(), detail.GetReason(), st.Message(), code, reason)
	}
	return detail
}

func TestNewRejects(t *testing.T) {
	engine, err := core.New(core.Config{})
	if err != nil {
		t.Fatalf("core.New: %v", err)
	}
	full := Config{Engine: engine, Committer: NopCommitter{}, Tokens: NopTokenStore{}}
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"nil engine", func(c *Config) { c.Engine = nil }},
		{"nil committer", func(c *Config) { c.Committer = nil }},
		{"nil token store", func(c *Config) { c.Tokens = nil }},
		{"empty API key", func(c *Config) { c.Keys = map[string]core.NodeID{"": core.RootID} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := full
			tc.change(&cfg)
			if _, err := New(t.Context(), cfg); err == nil {
				t.Error("New accepted the config")
			}
		})
	}
}

func TestOpenSession(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})

	resp, err := h.client.OpenSession(h.as(keyA), &pb.OpenSessionRequest{Ttl: durationpb.New(time.Minute)})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if resp.GetScopeId() != uint64(h.a) {
		t.Errorf("scope = %d, want %d", resp.GetScopeId(), h.a)
	}
	if want := h.clock.Now().Add(time.Minute); !resp.GetExpiresAt().AsTime().Equal(want) {
		t.Errorf("expires_at = %v, want %v", resp.GetExpiresAt().AsTime(), want)
	}
	if len(resp.GetSessionToken()) < 26 {
		t.Errorf("token %q is shorter than 128 bits of base32", resp.GetSessionToken())
	}

	forever, err := h.client.OpenSession(h.as(keyA), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession without ttl: %v", err)
	}
	if forever.GetExpiresAt() != nil {
		t.Errorf("expires_at = %v for a session without a ttl, want unset", forever.GetExpiresAt())
	}
	if forever.GetSessionToken() == resp.GetSessionToken() {
		t.Error("two sessions share a token")
	}

	_, err = h.client.OpenSession(h.as("wrong"), &pb.OpenSessionRequest{})
	wantStatus(t, err, codes.Unauthenticated, pb.Reason_REASON_BAD_API_KEY)
	_, err = h.client.OpenSession(t.Context(), &pb.OpenSessionRequest{})
	wantStatus(t, err, codes.Unauthenticated, pb.Reason_REASON_BAD_API_KEY)
	// A session token is not an API key.
	_, err = h.client.OpenSession(h.as(resp.GetSessionToken()), &pb.OpenSessionRequest{})
	wantStatus(t, err, codes.Unauthenticated, pb.Reason_REASON_BAD_API_KEY)
	_, err = h.client.OpenSession(h.as(keyA), &pb.OpenSessionRequest{Ttl: durationpb.New(-time.Second)})
	wantStatus(t, err, codes.InvalidArgument, pb.Reason_REASON_INVALID)
}

// calls lists every RPC that needs a session, aimed at one node.
func calls(c pb.GovernorServiceClient, node uint64) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"CreateNode": func(ctx context.Context) error {
			_, err := c.CreateNode(ctx, &pb.CreateNodeRequest{ParentId: node})
			return err
		},
		"Consume": func(ctx context.Context) error {
			_, err := c.Consume(ctx, &pb.ConsumeRequest{NodeId: node, Resource: "http", Amount: 1})
			return err
		},
		"Acquire": func(ctx context.Context) error {
			_, err := c.Acquire(ctx, &pb.AcquireRequest{NodeId: node, Class: "db"})
			return err
		},
		"CancelNode": func(ctx context.Context) error {
			_, err := c.CancelNode(ctx, &pb.CancelNodeRequest{NodeId: node})
			return err
		},
		"CloseNode": func(ctx context.Context) error {
			_, err := c.CloseNode(ctx, &pb.CloseNodeRequest{NodeId: node})
			return err
		},
		"WatchNode": func(ctx context.Context) error {
			stream, err := c.WatchNode(ctx, &pb.WatchNodeRequest{NodeId: node})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
	}
}

func TestCallsNeedSession(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{"http": 100}})
	all := calls(h.client, uint64(h.a))
	all["Heartbeat"] = func(ctx context.Context) error {
		_, err := h.client.Heartbeat(ctx, &pb.HeartbeatRequest{})
		return err
	}
	all["CloseSession"] = func(ctx context.Context) error {
		_, err := h.client.CloseSession(ctx, &pb.CloseSessionRequest{})
		return err
	}
	all["Release"] = func(ctx context.Context) error {
		_, err := h.client.Release(ctx, &pb.ReleaseRequest{LeaseId: 1})
		return err
	}
	credentials := map[string]context.Context{
		"no credential": t.Context(),
		"guessed token": h.as("AAAAAAAAAAAAAAAAAAAAAAAAAA"),
		"an API key":    h.as(keyA),
	}
	for name, call := range all {
		for cred, ctx := range credentials {
			t.Run(name+" with "+cred, func(t *testing.T) {
				wantStatus(t, call(ctx), codes.Unauthenticated, pb.Reason_REASON_SESSION_EXPIRED)
			})
		}
	}
	if got := h.used(h.a, "http"); got != 0 {
		t.Errorf("used = %d after unauthenticated calls, want 0", got)
	}
}

func TestTenantIsolation(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{"http": 100}})
	tokenA, tokenB := h.open(keyA, 0), h.open(keyB, 0)
	taskB := h.node(tokenB, h.b)

	for _, target := range []uint64{uint64(h.b), taskB, uint64(core.RootID)} {
		for name, call := range calls(h.client, target) {
			t.Run(fmt.Sprintf("%s on node %d", name, target), func(t *testing.T) {
				wantStatus(t, call(h.as(tokenA)), codes.PermissionDenied, pb.Reason_REASON_FORBIDDEN)
			})
		}
	}

	// Tenant B is untouched and still works.
	if got := h.used(h.b, "http"); got != 0 {
		t.Errorf("used(b) = %d, want 0", got)
	}
	own := &pb.ConsumeRequest{NodeId: taskB, Resource: "http", Amount: 1}
	if _, err := h.client.Consume(h.as(tokenB), own); err != nil {
		t.Errorf("Consume by tenant b on its own task: %v", err)
	}

	// A lease can only be released by the session that holds it.
	lease, err := h.client.Acquire(h.as(tokenB), &pb.AcquireRequest{NodeId: taskB, Class: "db"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	_, err = h.client.Release(h.as(tokenA), &pb.ReleaseRequest{LeaseId: lease.GetLeaseId()})
	wantStatus(t, err, codes.PermissionDenied, pb.Reason_REASON_NOT_OWNER)
}

func TestConsumeDenialDetail(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{Name: "tenant", Quotas: map[core.Resource]int64{"http": 10}})
	token := h.open(keyA, 0)
	resp, err := h.client.CreateNode(h.as(token), &pb.CreateNodeRequest{
		ParentId: uint64(h.a),
		Spec:     &pb.Spec{Name: "crawler"},
	})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	task := resp.GetNodeId()
	if _, err := h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: task, Resource: "http", Amount: 8}); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	_, err = h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: task, Resource: "http", Amount: 5})
	detail := wantStatus(t, err, codes.ResourceExhausted, pb.Reason_REASON_DENIED)
	want := &pb.Denial{
		NodeId: uint64(h.a), Name: "tenant", Resource: "http",
		Used: 8, Limit: 10, Requested: 5,
		TopConsumerId: task, TopConsumerName: "crawler", TopConsumerUsed: 8,
	}
	if !proto.Equal(detail.GetDenial(), want) {
		t.Errorf("denial = %v, want %v", detail.GetDenial(), want)
	}
	if got := h.used(h.a, "http"); got != 8 {
		t.Errorf("used = %d after a denial, want 8", got)
	}
}

func TestInvalidArguments(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})
	token := h.open(keyA, 0)
	a := uint64(h.a)
	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{"zero amount", func(ctx context.Context) error {
			_, err := h.client.Consume(ctx, &pb.ConsumeRequest{NodeId: a, Resource: "http"})
			return err
		}},
		{"empty resource", func(ctx context.Context) error {
			_, err := h.client.Consume(ctx, &pb.ConsumeRequest{NodeId: a, Amount: 1})
			return err
		}},
		{"empty class", func(ctx context.Context) error {
			_, err := h.client.Acquire(ctx, &pb.AcquireRequest{NodeId: a})
			return err
		}},
		{"negative hold time", func(ctx context.Context) error {
			_, err := h.client.Acquire(ctx, &pb.AcquireRequest{NodeId: a, Class: "db", MaxHold: durationpb.New(-1)})
			return err
		}},
		{"negative latency", func(ctx context.Context) error {
			_, err := h.client.Release(ctx, &pb.ReleaseRequest{LeaseId: 1, Latency: durationpb.New(-1)})
			return err
		}},
		{"malformed duration", func(ctx context.Context) error {
			_, err := h.client.Release(ctx, &pb.ReleaseRequest{LeaseId: 1, Latency: &durationpb.Duration{Nanos: 2e9}})
			return err
		}},
		{"negative quota", func(ctx context.Context) error {
			spec := &pb.Spec{Quotas: map[string]int64{"http": -1}}
			_, err := h.client.CreateNode(ctx, &pb.CreateNodeRequest{ParentId: a, Spec: spec})
			return err
		}},
		{"malformed deadline", func(ctx context.Context) error {
			spec := &pb.Spec{Deadline: &timestamppb.Timestamp{Nanos: -1}}
			_, err := h.client.CreateNode(ctx, &pb.CreateNodeRequest{ParentId: a, Spec: spec})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantStatus(t, tc.call(h.as(token)), codes.InvalidArgument, pb.Reason_REASON_INVALID)
		})
	}

	_, err := h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: 9999, Resource: "http", Amount: 1})
	wantStatus(t, err, codes.NotFound, pb.Reason_REASON_UNKNOWN_NODE)
}

func TestSpecIsConverted(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})
	token := h.open(keyA, 0)
	resp, err := h.client.CreateNode(h.as(token), &pb.CreateNodeRequest{
		ParentId: uint64(h.a),
		Spec: &pb.Spec{
			Quotas:   map[string]int64{"http": 2},
			Limits:   map[string]int64{"db": 0},
			Deadline: timestamppb.New(h.clock.Now().Add(time.Minute)),
		},
	})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	node := resp.GetNodeId()

	_, err = h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: node, Resource: "http", Amount: 3})
	wantStatus(t, err, codes.ResourceExhausted, pb.Reason_REASON_DENIED)

	// A limit of zero means the acquire can only queue.
	ctx, cancel := context.WithTimeout(h.as(token), 50*time.Millisecond)
	defer cancel()
	_, err = h.client.Acquire(ctx, &pb.AcquireRequest{NodeId: node, Class: "db"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("Acquire against a limit of zero = %v, want DeadlineExceeded", err)
	}

	h.clock.Advance(time.Minute)
	_, err = h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: node, Resource: "http", Amount: 1})
	wantStatus(t, err, codes.FailedPrecondition, pb.Reason_REASON_CLOSED)
}

func TestLeaseLifecycle(t *testing.T) {
	h := newHarness(t, core.Spec{Limits: map[core.Class]int{"db": 1}}, core.Spec{})
	token := h.open(keyA, 0)
	task := h.node(token, h.a)

	lease, err := h.client.Acquire(h.as(token), &pb.AcquireRequest{NodeId: task, Class: "db"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Validate is a fencing check, so it needs no credential.
	if _, err := h.client.Validate(t.Context(), &pb.ValidateRequest{LeaseId: lease.GetLeaseId()}); err != nil {
		t.Errorf("Validate: %v", err)
	}

	// A queued acquire that gives up must not keep the slot.
	ctx, cancel := context.WithTimeout(h.as(token), 50*time.Millisecond)
	defer cancel()
	_, err = h.client.Acquire(ctx, &pb.AcquireRequest{NodeId: task, Class: "db"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("queued Acquire = %v, want DeadlineExceeded", err)
	}

	release := &pb.ReleaseRequest{LeaseId: lease.GetLeaseId(), Latency: durationpb.New(time.Millisecond)}
	for range 2 {
		if _, err := h.client.Release(h.as(token), release); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}
	_, err = h.client.Validate(t.Context(), &pb.ValidateRequest{LeaseId: lease.GetLeaseId()})
	wantStatus(t, err, codes.NotFound, pb.Reason_REASON_UNKNOWN_LEASE)

	second, err := h.client.Acquire(h.as(token), &pb.AcquireRequest{NodeId: task, Class: "db"})
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	if second.GetLeaseId() <= lease.GetLeaseId() {
		t.Errorf("lease %d is not greater than the earlier lease %d", second.GetLeaseId(), lease.GetLeaseId())
	}

	if _, err := h.client.CancelNode(h.as(token), &pb.CancelNodeRequest{NodeId: task}); err != nil {
		t.Fatalf("CancelNode: %v", err)
	}
	_, err = h.client.Release(h.as(token), &pb.ReleaseRequest{LeaseId: second.GetLeaseId()})
	wantStatus(t, err, codes.FailedPrecondition, pb.Reason_REASON_LEASE_REVOKED)
	_, err = h.client.Validate(t.Context(), &pb.ValidateRequest{LeaseId: second.GetLeaseId()})
	wantStatus(t, err, codes.FailedPrecondition, pb.Reason_REASON_LEASE_REVOKED)
}

func TestWatchNode(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})
	token := h.open(keyA, 0)
	closed, cancelled := h.node(token, h.a), h.node(token, h.a)

	watch := func(token string, node uint64) pb.GovernorService_WatchNodeClient {
		t.Helper()
		stream, err := h.client.WatchNode(h.as(token), &pb.WatchNodeRequest{NodeId: node})
		if err != nil {
			t.Fatalf("WatchNode: %v", err)
		}
		return stream
	}
	onClosed, onCancelled := watch(token, closed), watch(token, cancelled)

	if _, err := h.client.CloseNode(h.as(token), &pb.CloseNodeRequest{NodeId: closed}); err != nil {
		t.Fatalf("CloseNode: %v", err)
	}
	if _, err := h.client.CancelNode(h.as(token), &pb.CancelNodeRequest{NodeId: cancelled}); err != nil {
		t.Fatalf("CancelNode: %v", err)
	}
	for _, tc := range []struct {
		stream pb.GovernorService_WatchNodeClient
		want   pb.State
	}{
		{onClosed, pb.State_STATE_DONE},
		{onCancelled, pb.State_STATE_CANCELLED},
		// Watching a node that has already ended answers at once.
		{watch(token, closed), pb.State_STATE_DONE},
	} {
		msg, err := tc.stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if msg.GetState() != tc.want {
			t.Errorf("state = %s, want %s", msg.GetState(), tc.want)
		}
	}

	// The stream ends when its session does, even if the node lives on.
	dying := h.open(keyA, 10*time.Second)
	stream := watch(dying, uint64(h.a))
	h.clock.Advance(10 * time.Second)
	h.engine.Reap()
	_, err := stream.Recv()
	wantStatus(t, err, codes.Unauthenticated, pb.Reason_REASON_SESSION_EXPIRED)
}

func TestSessionEndForgetsToken(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})
	dying, closing, forever := h.open(keyA, 10*time.Second), h.open(keyA, time.Minute), h.open(keyA, 0)
	if got := h.live(); got != 3 {
		t.Fatalf("tokens = %d, want 3", got)
	}

	// The dying worker never calls again; only the reaper notices.
	h.clock.Advance(10 * time.Second)
	h.engine.Reap()
	eventually(t, "the expired session's token to be forgotten", func() bool { return h.live() == 2 })
	_, err := h.client.Heartbeat(h.as(dying), &pb.HeartbeatRequest{})
	wantStatus(t, err, codes.Unauthenticated, pb.Reason_REASON_SESSION_EXPIRED)

	resp, err := h.client.Heartbeat(h.as(closing), &pb.HeartbeatRequest{})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if want := h.clock.Now().Add(time.Minute); !resp.GetExpiresAt().AsTime().Equal(want) {
		t.Errorf("expires_at = %v, want %v", resp.GetExpiresAt().AsTime(), want)
	}
	if _, err := h.client.CloseSession(h.as(closing), &pb.CloseSessionRequest{}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	eventually(t, "the closed session's token to be forgotten", func() bool { return h.live() == 1 })

	if _, err := h.client.Heartbeat(h.as(forever), &pb.HeartbeatRequest{}); err != nil {
		t.Errorf("Heartbeat on a session without a ttl: %v", err)
	}
}

func TestSessionsSurviveRestart(t *testing.T) {
	h := newHarness(t, core.Spec{Limits: map[core.Class]int{"db": 1}}, core.Spec{})
	kept, closed := h.open(keyA, time.Minute), h.open(keyA, time.Minute)
	lease, err := h.client.Acquire(h.as(kept), &pb.AcquireRequest{NodeId: uint64(h.a), Class: "db"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := h.client.CloseSession(h.as(closed), &pb.CloseSessionRequest{}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	eventually(t, "the closed session's token to leave the store", func() bool { return h.tokens.len() == 1 })
	// A token whose session never made it into the record must not survive either.
	if err := h.tokens.SaveToken(t.Context(), hash("stale"), 9999); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}
	orphan := h.admin

	h.restart()

	// The worker carries on with the token and the lease it had before.
	if _, err := h.client.Heartbeat(h.as(kept), &pb.HeartbeatRequest{}); err != nil {
		t.Errorf("Heartbeat with a token from before the restart: %v", err)
	}
	if _, err := h.client.Release(h.as(kept), &pb.ReleaseRequest{LeaseId: lease.GetLeaseId()}); err != nil {
		t.Errorf("Release of a lease from before the restart: %v", err)
	}
	for name, token := range map[string]string{"closed": closed, "stale": "stale"} {
		_, err := h.client.Heartbeat(h.as(token), &pb.HeartbeatRequest{})
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("Heartbeat with the %s token = %v, want Unauthenticated", name, err)
		}
	}
	// A restored session that nobody holds a token for is closed at start.
	if _, err := h.engine.Heartbeat(orphan); !errors.Is(err, core.ErrSessionExpired) {
		t.Errorf("Heartbeat(session without a token) = %v, want ErrSessionExpired", err)
	}
	eventually(t, "the stale token to leave the store", func() bool { return h.tokens.len() == 1 })
}

func TestTokenStoreFailure(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{})
	before := len(h.engine.Sessions())
	h.tokens.mu.Lock()
	h.tokens.err = errors.New("postgres is down")
	h.tokens.mu.Unlock()

	_, err := h.client.OpenSession(h.as(keyA), &pb.OpenSessionRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("OpenSession = %v, want Unavailable", err)
	}
	// A session whose token could not be saved would be unreachable after a restart.
	if got := len(h.engine.Sessions()); got != before {
		t.Errorf("sessions = %d after a failed OpenSession, want %d", got, before)
	}
}

func TestReplyWaitsForCommit(t *testing.T) {
	h := newHarness(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{"http": 10}})
	token := h.open(keyA, 0)
	gate := make(chan struct{})
	h.commit.set(nil, gate)

	replied := make(chan error, 1)
	go func() {
		_, err := h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: uint64(h.a), Resource: "http", Amount: 4})
		replied <- err
	}()
	eventually(t, "the consume to reach the engine", func() bool { return h.used(h.a, "http") == 4 })
	select {
	case err := <-replied:
		t.Fatalf("Consume replied before the commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	if err := <-replied; err != nil {
		t.Errorf("Consume after the commit: %v", err)
	}
}

func TestCommitFailure(t *testing.T) {
	h := newHarness(t, core.Spec{Limits: map[core.Class]int{"db": 1}}, core.Spec{})
	token := h.open(keyA, 0)
	h.commit.set(errors.New("postgres is down"), nil)

	_, err := h.client.Consume(h.as(token), &pb.ConsumeRequest{NodeId: uint64(h.a), Resource: "http", Amount: 1})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Consume = %v, want Unavailable", err)
	}
	_, err = h.client.OpenSession(h.as(keyA), &pb.OpenSessionRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("OpenSession = %v, want Unavailable", err)
	}
	if got := h.live(); got != 1 {
		t.Errorf("tokens = %d after a failed OpenSession, want 1", got)
	}

	// A lease whose grant could not be committed is handed back.
	_, err = h.client.Acquire(h.as(token), &pb.AcquireRequest{NodeId: uint64(h.a), Class: "db"})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Acquire = %v, want Unavailable", err)
	}
	h.commit.set(nil, nil)
	if _, err := h.client.Acquire(h.as(token), &pb.AcquireRequest{NodeId: uint64(h.a), Class: "db"}); err != nil {
		t.Errorf("Acquire of the only slot after a failed grant: %v", err)
	}
}

func TestRequestIDs(t *testing.T) {
	h := newHarness(t, core.Spec{Limits: map[core.Class]int{"db": 1}},
		core.Spec{Quotas: map[core.Resource]int64{"http": 10}})
	first, second := h.open(keyA, 0), h.open(keyA, 0)
	a := uint64(h.a)

	t.Run("CreateNode returns the same node", func(t *testing.T) {
		req := &pb.CreateNodeRequest{RequestId: "create", ParentId: a}
		one, err := h.client.CreateNode(h.as(first), req)
		if err != nil {
			t.Fatalf("CreateNode: %v", err)
		}
		two, err := h.client.CreateNode(h.as(first), req)
		if err != nil {
			t.Fatalf("CreateNode retry: %v", err)
		}
		if one.GetNodeId() != two.GetNodeId() {
			t.Errorf("retry created node %d, want %d", two.GetNodeId(), one.GetNodeId())
		}
		// Request ids belong to a session, so another session's id is unrelated.
		other, err := h.client.CreateNode(h.as(second), req)
		if err != nil {
			t.Fatalf("CreateNode by another session: %v", err)
		}
		if other.GetNodeId() == one.GetNodeId() {
			t.Error("two sessions shared a request id")
		}
	})

	t.Run("concurrent Consume retries charge once", func(t *testing.T) {
		req := &pb.ConsumeRequest{RequestId: "charge", NodeId: a, Resource: "http", Amount: 3}
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if _, err := h.client.Consume(h.as(first), req); err != nil {
					t.Errorf("Consume: %v", err)
				}
			})
		}
		wg.Wait()
		if got := h.used(h.a, "http"); got != 3 {
			t.Errorf("used = %d, want 3", got)
		}
	})

	t.Run("an id reused with other arguments is rejected", func(t *testing.T) {
		req := &pb.ConsumeRequest{RequestId: "charge", NodeId: a, Resource: "http", Amount: 4}
		_, err := h.client.Consume(h.as(first), req)
		wantStatus(t, err, codes.InvalidArgument, pb.Reason_REASON_INVALID)
	})

	t.Run("without an id every call is applied", func(t *testing.T) {
		req := &pb.ConsumeRequest{NodeId: a, Resource: "http", Amount: 2}
		for range 2 {
			if _, err := h.client.Consume(h.as(first), req); err != nil {
				t.Fatalf("Consume: %v", err)
			}
		}
		if got := h.used(h.a, "http"); got != 7 {
			t.Errorf("used = %d, want 7", got)
		}
	})

	t.Run("a denial is replayed", func(t *testing.T) {
		req := &pb.ConsumeRequest{RequestId: "too much", NodeId: a, Resource: "http", Amount: 4}
		for range 2 {
			_, err := h.client.Consume(h.as(first), req)
			wantStatus(t, err, codes.ResourceExhausted, pb.Reason_REASON_DENIED)
		}
	})

	t.Run("Acquire returns the same lease", func(t *testing.T) {
		req := &pb.AcquireRequest{RequestId: "hold", NodeId: a, Class: "db"}
		one, err := h.client.Acquire(h.as(first), req)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		two, err := h.client.Acquire(h.as(first), req)
		if err != nil {
			t.Fatalf("Acquire retry: %v", err)
		}
		if one.GetLeaseId() != two.GetLeaseId() {
			t.Errorf("retry got lease %d, want %d", two.GetLeaseId(), one.GetLeaseId())
		}

		// A queued acquire that timed out is not remembered, so its retry queues again.
		queued := &pb.AcquireRequest{RequestId: "wait", NodeId: a, Class: "db"}
		ctx, cancel := context.WithTimeout(h.as(first), 50*time.Millisecond)
		defer cancel()
		if _, err := h.client.Acquire(ctx, queued); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("queued Acquire = %v, want DeadlineExceeded", err)
		}
		granted := make(chan uint64, 2)
		for range 2 {
			go func() {
				resp, err := h.client.Acquire(h.as(first), queued)
				if err != nil {
					t.Errorf("Acquire retry: %v", err)
				}
				granted <- resp.GetLeaseId()
			}()
		}
		if _, err := h.client.Release(h.as(first), &pb.ReleaseRequest{LeaseId: one.GetLeaseId()}); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if x, y := <-granted, <-granted; x != y || x == one.GetLeaseId() {
			t.Errorf("retries got leases %d and %d, want one new lease", x, y)
		}
	})

	t.Run("a session's ids are forgotten when it ends", func(t *testing.T) {
		for _, token := range []string{first, second} {
			if _, err := h.client.CloseSession(h.as(token), &pb.CloseSessionRequest{}); err != nil {
				t.Fatalf("CloseSession: %v", err)
			}
		}
		eventually(t, "the request ids to be forgotten", func() bool {
			h.server.seen.mu.Lock()
			defer h.server.seen.mu.Unlock()
			return len(h.server.seen.sessions) == 0
		})
	})
}

func TestRequestIDsAreCapped(t *testing.T) {
	var d dedup
	first, _ := d.begin(1, "0", digest{})
	for i := 1; i <= maxRemembered; i++ {
		d.begin(1, fmt.Sprint(i), digest{})
	}
	if got := len(d.sessions[1].calls); got != maxRemembered {
		t.Errorf("remembered %d ids, want %d", got, maxRemembered)
	}
	if again, fresh := d.begin(1, "0", digest{}); !fresh || again == first {
		t.Error("the oldest id was not evicted")
	}
}

func TestToStatus(t *testing.T) {
	tests := []struct {
		err    error
		code   codes.Code
		reason pb.Reason
	}{
		{core.ErrInvalid, codes.InvalidArgument, pb.Reason_REASON_INVALID},
		{core.ErrUnknownNode, codes.NotFound, pb.Reason_REASON_UNKNOWN_NODE},
		{core.ErrSessionExpired, codes.Unauthenticated, pb.Reason_REASON_SESSION_EXPIRED},
		{core.ErrUnknownLease, codes.NotFound, pb.Reason_REASON_UNKNOWN_LEASE},
		{core.ErrLeaseExpired, codes.FailedPrecondition, pb.Reason_REASON_LEASE_EXPIRED},
		{core.ErrLeaseRevoked, codes.FailedPrecondition, pb.Reason_REASON_LEASE_REVOKED},
		{core.ErrNotOwner, codes.PermissionDenied, pb.Reason_REASON_NOT_OWNER},
		{core.ErrForbidden, codes.PermissionDenied, pb.Reason_REASON_FORBIDDEN},
		{&core.DeniedError{}, codes.ResourceExhausted, pb.Reason_REASON_DENIED},
		{core.ErrClosed, codes.FailedPrecondition, pb.Reason_REASON_CLOSED},
		{errBadKey, codes.Unauthenticated, pb.Reason_REASON_BAD_API_KEY},
		{fmt.Errorf("wrapped: %w", core.ErrClosed), codes.FailedPrecondition, pb.Reason_REASON_CLOSED},
		{context.Canceled, codes.Canceled, pb.Reason_REASON_UNSPECIFIED},
		{context.DeadlineExceeded, codes.DeadlineExceeded, pb.Reason_REASON_UNSPECIFIED},
		{status.Error(codes.Unavailable, "down"), codes.Unavailable, pb.Reason_REASON_UNSPECIFIED},
		{errors.New("a secret detail"), codes.Internal, pb.Reason_REASON_UNSPECIFIED},
	}
	for _, tc := range tests {
		t.Run(tc.err.Error(), func(t *testing.T) {
			wantStatus(t, toStatus(tc.err), tc.code, tc.reason)
		})
	}
	// An unknown error must not reach the client with its text.
	if msg := status.Convert(toStatus(errors.New("a secret detail"))).Message(); msg != "governor: internal error" {
		t.Errorf("internal error message = %q", msg)
	}
}
