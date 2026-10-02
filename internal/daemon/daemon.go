// Package daemon runs one governord: it restores the engine, applies the configuration and serves.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
	"github.com/Avik-creator/governor/internal/metrics"
	"github.com/Avik-creator/governor/internal/server"
	"github.com/Avik-creator/governor/internal/store"
	"github.com/Avik-creator/governor/internal/transport"
)

// Daemon is a governord that is serving; it stops when the context given to Start is done.
type Daemon struct {
	addr    string
	metrics string        // address the metrics are served on; empty if they are not
	done    chan struct{} // closed once the daemon has stopped and released everything
	err     error
}

// Addr returns the address the daemon listens on.
func (d *Daemon) Addr() string {
	return d.addr
}

// MetricsAddr returns the address the metrics are served on, or "" if they are not.
func (d *Daemon) MetricsAddr() string {
	return d.metrics
}

// Wait blocks until the daemon has stopped; it returns nil if the context ended it.
func (d *Daemon) Wait() error {
	<-d.done
	return d.err
}

// Start brings up governord as cfg describes and returns once it is serving.
func Start(ctx context.Context, cfg *config.Config) (*Daemon, error) {
	// Everything opened so far is closed again, newest first, if a later step fails.
	var opened []func()
	closeAll := func() {
		for i := len(opened) - 1; i >= 0; i-- {
			opened[i]()
		}
	}
	fail := func(err error) (*Daemon, error) {
		closeAll()
		return nil, err
	}

	// A certificate that cannot be loaded stops governord before it opens anything.
	creds := insecure.NewCredentials()
	if cfg.TLS != nil {
		var err error
		if creds, err = transport.Server(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
	}
	meter := metrics.New()
	// Without a database nothing is recorded, so nothing survives a restart.
	var (
		sink     core.Sink
		snapshot *core.Snapshot
		save     func(context.Context, *core.Snapshot) error
		commit   server.Committer             = server.NopCommitter{}
		tokens   server.TokenStore            = server.NopTokenStore{}
		events   iter.Seq2[core.Event, error] = func(func(core.Event, error) bool) {}
		failed   <-chan struct{}
	)
	if cfg.DatabaseURL != "" {
		st, err := store.Open(ctx, cfg.DatabaseURL, store.WithCommitObserver(meter.ObserveCommit))
		if err != nil {
			return nil, err
		}
		opened = append(opened, func() {
			if err := st.Close(); err != nil {
				slog.Error("governord: close store", "err", err)
			}
		})
		sink, commit, tokens, events, failed = st, st, st, st.Events(ctx), st.Failed()
		if snapshot, err = st.LoadSnapshot(ctx); err != nil {
			return fail(err)
		}
		save = st.SaveSnapshot
	}

	tuned, err := controllers(cfg)
	if err != nil {
		return fail(err)
	}
	// Each released lease reports to the controller of its class, if it has one.
	observer := func(class core.Class, r core.Report) {
		if c := tuned[class]; c != nil {
			c.Observe(r)
		}
	}
	engine, err := core.Restore(core.Config{
		Sink:          meter.Sink(sink),
		Observer:      observer,
		NodeRetention: cfg.NodeRetention,
		Root:          cfg.Root.Spec(),
	}, snapshot, events)
	if err != nil {
		return fail(fmt.Errorf("restore: %w", err))
	}
	keys, seq, err := reconcile(engine, cfg)
	if err != nil {
		return fail(fmt.Errorf("apply configuration: %w", err))
	}
	if err := commit.WaitDurable(ctx, seq); err != nil {
		return fail(fmt.Errorf("record configuration: %w", err))
	}
	srv, err := server.New(ctx, server.Config{Engine: engine, Committer: commit, Tokens: tokens, Keys: keys})
	if err != nil {
		return fail(err)
	}
	opened = append(opened, srv.Close)

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fail(fmt.Errorf("listen: %w", err))
	}
	opened = append(opened, func() { _ = lis.Close() })
	metricsAddr, err := serveMetrics(cfg.MetricsListen, meter, engine, &opened)
	if err != nil {
		return fail(err)
	}
	// The metrics go first, so they see the status each call ends with.
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.ChainUnaryInterceptor(meter.UnaryInterceptor, srv.UnaryInterceptor),
		grpc.ChainStreamInterceptor(meter.StreamInterceptor, srv.StreamInterceptor),
	)
	pb.RegisterGovernorServiceServer(grpcServer, srv)

	background, stopBackground := context.WithCancel(context.Background())
	opened = append(opened, stopBackground)
	go engine.Run(background, cfg.ReapInterval)
	for _, c := range tuned {
		go c.Run(background, engine)
	}
	if save != nil {
		go snapshots(background, engine, save, cfg, snapshot)
	}
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(lis) }()
	slog.Info("governord: serving", "addr", lis.Addr().String(), "tenants", len(cfg.Tenants),
		"durable", cfg.DatabaseURL != "", "tls", cfg.TLS != nil, "adaptive", len(tuned), "metrics", metricsAddr)

	d := &Daemon{addr: lis.Addr().String(), metrics: metricsAddr, done: make(chan struct{})}
	go func() {
		defer close(d.done)
		defer closeAll()
		select {
		case <-ctx.Done():
			slog.Info("governord: shutting down")
			drain(grpcServer, cfg.DrainTimeout)
		case <-failed:
			// The engine is ahead of the record, so stop at once and restart from it.
			grpcServer.Stop()
			d.err = errors.New("the store failed to commit; restart to recover from the record")
		case err := <-served:
			d.err = fmt.Errorf("serve: %w", err)
		}
	}()
	return d, nil
}

// metricsTimeout bounds reading a scrape's request and finishing the scrapes in flight at shutdown.
const metricsTimeout = 5 * time.Second

// serveMetrics serves the metrics over HTTP at addr and returns where; an empty addr serves nothing.
func serveMetrics(addr string, meter *metrics.Metrics, engine *core.Engine, opened *[]func()) (string, error) {
	if addr == "" {
		return "", nil
	}
	if err := meter.Watch(engine); err != nil {
		return "", err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen for metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", meter.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: metricsTimeout}
	go func() {
		if err := srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("governord: serve metrics", "err", err)
		}
	}()
	*opened = append(*opened, func() {
		ctx, cancel := context.WithTimeout(context.Background(), metricsTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("governord: stop metrics", "err", err)
		}
	})
	return lis.Addr().String(), nil
}

// snapshots saves the engine's state whenever enough events have accumulated since the last snapshot.
func snapshots(
	ctx context.Context, engine *core.Engine, save func(context.Context, *core.Snapshot) error,
	cfg *config.Config, loaded *core.Snapshot,
) {
	var last uint64
	if loaded != nil {
		last = loaded.Seq
	}
	ticker := time.NewTicker(cfg.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if engine.Seq()-last < cfg.SnapshotEvery {
			continue
		}
		snap := engine.Snapshot()
		if err := save(ctx, snap); err != nil {
			// The events are still on record, so a failed snapshot only costs replay time.
			if ctx.Err() == nil {
				slog.ErrorContext(ctx, "governord: save snapshot", "err", err)
			}
			continue
		}
		last = snap.Seq
	}
}

// drain lets in-flight calls finish, then cuts off whatever is still open.
func drain(s *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		// Streams such as WatchNode never finish by themselves.
		s.Stop()
	}
}
