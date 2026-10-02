// Package governor is the client of governord: it keeps a tree of tasks inside its budgets.
package governor

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// Environment variables that Dial reads unless an option overrides them.
const (
	EnvAddr       = "GOVERNOR_ADDR"
	EnvAPIKey     = "GOVERNOR_API_KEY"
	EnvSessionTTL = "GOVERNOR_SESSION_TTL"
)

// Defaults used when neither an option nor the environment gives a value.
const (
	DefaultAddr       = "127.0.0.1:7600"
	DefaultSessionTTL = 15 * time.Second
)

// closeTimeout bounds the goodbye that Close sends to governord.
const closeTimeout = 2 * time.Second

// options are the settings of one Client.
type options struct {
	addr   string
	apiKey string
	ttl    time.Duration
	dial   []grpc.DialOption
}

// Option changes how Dial connects.
type Option func(*options)

// WithAddr sets the address of governord instead of reading GOVERNOR_ADDR.
func WithAddr(addr string) Option {
	return func(o *options) { o.addr = addr }
}

// WithAPIKey sets the tenant's API key instead of reading GOVERNOR_API_KEY.
func WithAPIKey(key string) Option {
	return func(o *options) { o.apiKey = key }
}

// WithSessionTTL sets how long the session survives without a heartbeat; zero never expires.
func WithSessionTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

// WithDialOptions adds gRPC dial options, such as transport credentials.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dial = append(o.dial, opts...) }
}

// fromEnv returns the options given by the environment and the defaults.
func fromEnv() (options, error) {
	o := options{addr: DefaultAddr, apiKey: os.Getenv(EnvAPIKey), ttl: DefaultSessionTTL}
	if addr := os.Getenv(EnvAddr); addr != "" {
		o.addr = addr
	}
	if raw := os.Getenv(EnvSessionTTL); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil {
			return o, fmt.Errorf("%w: %s: %v", ErrInvalid, EnvSessionTTL, err)
		}
		o.ttl = ttl
	}
	return o, nil
}

// Client is one session with governord; it is safe for concurrent use.
type Client struct {
	conn  *grpc.ClientConn
	rpc   pb.GovernorServiceClient
	token string
	scope uint64
	ttl   time.Duration

	ctx    context.Context // done once the session is lost or the client is closed
	cancel context.CancelCauseFunc
	beats  chan struct{} // closed when the heartbeat loop has exited

	closeOnce sync.Once
	closeErr  error
}

// Dial opens a session with governord and keeps it alive until Close.
func Dial(ctx context.Context, opts ...Option) (*Client, error) {
	o, err := fromEnv()
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.apiKey == "":
		return nil, fmt.Errorf("%w: no API key; set %s", ErrInvalid, EnvAPIKey)
	case o.ttl < 0:
		return nil, fmt.Errorf("%w: session TTL is negative", ErrInvalid)
	}

	dial := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, o.dial...)
	conn, err := grpc.NewClient(o.addr, dial...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c := &Client{conn: conn, rpc: pb.NewGovernorServiceClient(conn), ttl: o.ttl, beats: make(chan struct{})}

	// The session can only expire later than a TTL counted from before the request.
	opened := time.Now()
	resp, err := c.rpc.OpenSession(bearer(ctx, o.apiKey), &pb.OpenSessionRequest{Ttl: durationpb.New(o.ttl)})
	if err != nil {
		_ = conn.Close()
		return nil, fromStatus(err)
	}
	c.token, c.scope = resp.GetSessionToken(), resp.GetScopeId()
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	go c.heartbeat(opened)
	return c, nil
}

// bearer returns ctx carrying secret as the credential of the call.
func bearer(ctx context.Context, secret string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+secret)
}

// auth returns ctx carrying the session token.
func (c *Client) auth(ctx context.Context) context.Context {
	return bearer(ctx, c.token)
}

// Scope returns the id of the tenant node this client's work lives under.
func (c *Client) Scope() uint64 {
	return c.scope
}

// Done is closed once the session is lost or the client is closed.
func (c *Client) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Err says why Done is closed: ErrSessionLost or ErrClientClosed; nil while it is open.
func (c *Client) Err() error {
	return context.Cause(c.ctx)
}

// Close ends the session, which releases every lease it still holds.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		lost := c.Err() != nil
		c.cancel(ErrClientClosed)
		<-c.beats
		// A lost session is already gone on the server, or will expire there.
		if !lost {
			ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			_, err := c.rpc.CloseSession(c.auth(ctx), &pb.CloseSessionRequest{})
			cancel()
			c.closeErr = fromStatus(err)
		}
		if err := c.conn.Close(); err != nil && c.closeErr == nil {
			c.closeErr = err
		}
	})
	return c.closeErr
}
