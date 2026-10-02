package governor

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/avikmukherjee/governor/internal/config"
	"github.com/avikmukherjee/governor/internal/core"
	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
	"github.com/avikmukherjee/governor/internal/server"
)

const testKey = "key-tenant"

// backend is a governord running in this process behind an in-memory listener.
type backend struct {
	t      *testing.T
	engine *core.Engine
	admin  core.SessionID
	tenant core.NodeID
	grpc   *grpc.Server
	lis    *bufconn.Listener

	mu       sync.Mutex
	observer func(core.Class, core.Report)
}

// observe sets the function that sees the report of every released lease.
func (b *backend) observe(fn func(core.Class, core.Report)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.observer = fn
}

func (b *backend) report(class core.Class, r core.Report) {
	b.mu.Lock()
	fn := b.observer
	b.mu.Unlock()
	if fn != nil {
		fn(class, r)
	}
}

// newBackend starts a server with one tenant whose key is testKey.
func newBackend(t *testing.T, root, tenant core.Spec) *backend {
	t.Helper()
	b := &backend{t: t, lis: bufconn.Listen(1 << 20)}
	var err error
	if b.engine, err = core.New(core.Config{Root: root, Observer: b.report}); err != nil {
		t.Fatalf("core.New: %v", err)
	}
	setup, _, _, err := b.engine.OpenSession(core.RootID, 0)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if b.tenant, _, err = b.engine.CreateNode(setup, core.RootID, tenant); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	srv, err := server.New(t.Context(), server.Config{
		Engine:    b.engine,
		Committer: server.NopCommitter{},
		Tokens:    server.NopTokenStore{},
		Keys:      map[string]core.NodeID{testKey: b.tenant},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if b.admin, _, _, err = b.engine.OpenSession(core.RootID, 0); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	b.grpc = grpc.NewServer(grpc.UnaryInterceptor(srv.UnaryInterceptor), grpc.StreamInterceptor(srv.StreamInterceptor))
	pb.RegisterGovernorServiceServer(b.grpc, srv)
	go func() { _ = b.grpc.Serve(b.lis) }()
	reaping, stop := context.WithCancel(context.Background())
	go b.engine.Run(reaping, 5*time.Millisecond)
	t.Cleanup(func() {
		stop()
		b.grpc.Stop()
		srv.Close()
	})
	return b
}

// dial connects a Client to the backend; extra options come last, so they win.
func (b *backend) dial(opts ...Option) (*Client, error) {
	base := []Option{
		WithAddr("passthrough:///backend"),
		WithAPIKey(testKey),
		WithDialOptions(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return b.lis.Dial()
		})),
	}
	return Dial(b.t.Context(), append(base, opts...)...)
}

func (b *backend) mustDial(opts ...Option) *Client {
	b.t.Helper()
	c, err := b.dial(opts...)
	if err != nil {
		b.t.Fatalf("Dial: %v", err)
	}
	b.t.Cleanup(func() { _ = c.Close() })
	return c
}

// sessions counts the sessions clients hold, leaving out the backend's own.
func (b *backend) sessions() int {
	return len(b.engine.Sessions()) - 1
}

// waitDone waits for the client to give up its session and returns why.
func waitDone(t *testing.T, c *Client, within time.Duration) error {
	t.Helper()
	select {
	case <-c.Done():
		return c.Err()
	case <-time.After(within):
		t.Fatalf("client is still live after %v", within)
		return nil
	}
}

func TestDefaultsMatchTheServer(t *testing.T) {
	if DefaultAddr != config.DefaultListen {
		t.Errorf("DefaultAddr = %q, but governord listens on %q by default", DefaultAddr, config.DefaultListen)
	}
}

func TestDial(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	c := b.mustDial()
	if c.Scope() != uint64(b.tenant) {
		t.Errorf("Scope = %d, want the tenant node %d", c.Scope(), b.tenant)
	}
	if c.Err() != nil {
		t.Errorf("Err = %v on a live client, want nil", c.Err())
	}
	if got := b.sessions(); got != 1 {
		t.Errorf("server holds %d sessions, want 1", got)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !errors.Is(c.Err(), ErrClientClosed) {
		t.Errorf("Err after Close = %v, want ErrClientClosed", c.Err())
	}
	if got := b.sessions(); got != 0 {
		t.Errorf("server holds %d sessions after Close, want 0", got)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestDialReadsEnvironment(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	dialer := WithDialOptions(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return b.lis.Dial()
	}))
	t.Setenv(EnvAddr, "passthrough:///from-env")
	t.Setenv(EnvAPIKey, testKey)
	t.Setenv(EnvSessionTTL, "45s")

	c, err := Dial(t.Context(), dialer)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	if c.ttl != 45*time.Second || c.conn.Target() != "passthrough:///from-env" {
		t.Errorf("ttl, target = %v, %q, want the values from the environment", c.ttl, c.conn.Target())
	}

	// An option beats the environment.
	t.Setenv(EnvAPIKey, "wrong")
	if _, err := Dial(t.Context(), dialer); !errors.Is(err, ErrBadAPIKey) {
		t.Errorf("Dial with a wrong key in the environment = %v, want ErrBadAPIKey", err)
	}
	c2, err := Dial(t.Context(), dialer, WithAPIKey(testKey))
	if err != nil {
		t.Fatalf("Dial with WithAPIKey: %v", err)
	}
	_ = c2.Close()
}

func TestDialRejects(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	t.Setenv(EnvAPIKey, "")
	tests := []struct {
		name string
		opts []Option
		env  string // value of GOVERNOR_SESSION_TTL
		want error
	}{
		{"no API key", []Option{WithAPIKey("")}, "", ErrInvalid},
		{"unknown API key", []Option{WithAPIKey("nope")}, "", ErrBadAPIKey},
		{"negative TTL", []Option{WithSessionTTL(-time.Second)}, "", ErrInvalid},
		{"malformed TTL in the environment", nil, "soon", ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvSessionTTL, tc.env)
			if _, err := b.dial(tc.opts...); !errors.Is(err, tc.want) {
				t.Errorf("Dial = %v, want %v", err, tc.want)
			}
		})
	}
	if got := b.sessions(); got != 0 {
		t.Errorf("failed dials left %d sessions", got)
	}

	t.Run("server unreachable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err := Dial(ctx, WithAddr("127.0.0.1:1"), WithAPIKey(testKey))
		if !errors.Is(err, ErrUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Dial = %v, want ErrUnavailable or DeadlineExceeded", err)
		}
	})
}

func TestHeartbeatKeepsSession(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	c := b.mustDial(WithSessionTTL(150 * time.Millisecond))
	// Several TTLs pass; only heartbeats can have kept the session.
	time.Sleep(600 * time.Millisecond)
	if c.Err() != nil {
		t.Fatalf("Err = %v, want a live client", c.Err())
	}
	if got := b.sessions(); got != 1 {
		t.Errorf("server holds %d sessions, want 1", got)
	}
}

func TestSessionWithoutTTL(t *testing.T) {
	b := newBackend(t, core.Spec{}, core.Spec{})
	c := b.mustDial(WithSessionTTL(0))
	time.Sleep(50 * time.Millisecond)
	if c.Err() != nil || b.sessions() != 1 {
		t.Errorf("Err = %v with %d sessions, want a live session", c.Err(), b.sessions())
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestSessionLost(t *testing.T) {
	const ttl = 300 * time.Millisecond

	t.Run("server ends the session", func(t *testing.T) {
		b := newBackend(t, core.Spec{}, core.Spec{})
		c := b.mustDial(WithSessionTTL(ttl))
		for _, sid := range b.engine.Sessions() {
			if sid != b.admin {
				if _, err := b.engine.CloseSession(sid); err != nil {
					t.Fatalf("CloseSession: %v", err)
				}
			}
		}
		// The next heartbeat is refused, so the client learns before its deadline.
		if err := waitDone(t, c, ttl); !errors.Is(err, ErrSessionLost) {
			t.Errorf("Err = %v, want ErrSessionLost", err)
		}
		if err := c.Close(); err != nil {
			t.Errorf("Close after a lost session: %v", err)
		}
	})

	t.Run("server is unreachable", func(t *testing.T) {
		b := newBackend(t, core.Spec{}, core.Spec{})
		c := b.mustDial(WithSessionTTL(ttl))
		start := time.Now()
		b.grpc.Stop()
		if err := waitDone(t, c, 2*ttl); !errors.Is(err, ErrSessionLost) {
			t.Errorf("Err = %v, want ErrSessionLost", err)
		}
		// The client must give up before the server could have expired the session.
		if waited := time.Since(start); waited >= ttl {
			t.Errorf("client gave up after %v, want less than the TTL of %v", waited, ttl)
		}
	})
}
