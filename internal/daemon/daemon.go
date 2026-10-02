// Package daemon runs one governord: it restores the engine, applies the configuration and serves.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
	"github.com/Avik-creator/governor/internal/server"
	"github.com/Avik-creator/governor/internal/store"
)

// Daemon is a governord that is serving; it stops when the context given to Start is done.
type Daemon struct {
	addr string
	done chan struct{} // closed once the daemon has stopped and released everything
	err  error
}

// Addr returns the address the daemon listens on.
func (d *Daemon) Addr() string {
	return d.addr
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

	// Without a database nothing is recorded, so nothing survives a restart.
	var (
		sink   core.Sink
		commit server.Committer             = server.NopCommitter{}
		tokens server.TokenStore            = server.NopTokenStore{}
		events iter.Seq2[core.Event, error] = func(func(core.Event, error) bool) {}
		failed <-chan struct{}
	)
	if cfg.DatabaseURL != "" {
		st, err := store.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, err
		}
		opened = append(opened, func() {
			if err := st.Close(); err != nil {
				slog.Error("governord: close store", "err", err)
			}
		})
		sink, commit, tokens, events, failed = st, st, st, st.Events(ctx), st.Failed()
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
		Sink:          sink,
		Observer:      observer,
		NodeRetention: cfg.NodeRetention,
		Root:          cfg.Root.Spec(),
	}, events)
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
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor),
		grpc.StreamInterceptor(srv.StreamInterceptor),
	)
	pb.RegisterGovernorServiceServer(grpcServer, srv)

	background, stopBackground := context.WithCancel(context.Background())
	opened = append(opened, stopBackground)
	go engine.Run(background, cfg.ReapInterval)
	for _, c := range tuned {
		go c.Run(background, engine)
	}
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(lis) }()
	slog.Info("governord: serving", "addr", lis.Addr().String(), "tenants", len(cfg.Tenants),
		"durable", cfg.DatabaseURL != "", "adaptive", len(tuned))

	d := &Daemon{addr: lis.Addr().String(), done: make(chan struct{})}
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
