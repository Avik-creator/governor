// Command governord is the Governor daemon: one engine served over gRPC.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/avikmukherjee/governor/internal/config"
	"github.com/avikmukherjee/governor/internal/core"
	pb "github.com/avikmukherjee/governor/internal/gen/governor/v1"
	"github.com/avikmukherjee/governor/internal/server"
	"github.com/avikmukherjee/governor/internal/store"
)

// drainTimeout is how long in-flight calls get to finish on shutdown.
const drainTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.Error("governord: " + err.Error())
		os.Exit(1)
	}
}

// run starts governord and blocks until ctx is done or the store fails.
func run(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("governord", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "governor.yaml", "path to the configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}

	// Without a database nothing is recorded, so nothing survives a restart.
	var (
		sink    core.Sink
		commit  server.Committer             = server.NopCommitter{}
		tokens  server.TokenStore            = server.NopTokenStore{}
		events  iter.Seq2[core.Event, error] = func(func(core.Event, error) bool) {}
		failed  <-chan struct{}
		closeDB = func() error { return nil }
	)
	if cfg.DatabaseURL != "" {
		st, err := store.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		sink, commit, tokens, events, failed, closeDB = st, st, st, st.Events(ctx), st.Failed(), st.Close
	}
	defer func() {
		if err := closeDB(); err != nil {
			slog.Error("governord: close store", "err", err)
		}
	}()

	engine, err := core.Restore(core.Config{Sink: sink, Root: cfg.Root.Spec()}, events)
	if err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	keys, seq, err := reconcile(engine, cfg)
	if err != nil {
		return fmt.Errorf("apply configuration: %w", err)
	}
	if err := commit.WaitDurable(ctx, seq); err != nil {
		return fmt.Errorf("record configuration: %w", err)
	}
	srv, err := server.New(ctx, server.Config{Engine: engine, Committer: commit, Tokens: tokens, Keys: keys})
	if err != nil {
		return err
	}
	defer srv.Close()

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(srv.UnaryInterceptor),
		grpc.StreamInterceptor(srv.StreamInterceptor),
	)
	pb.RegisterGovernorServiceServer(grpcServer, srv)

	reaping, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	go engine.Run(reaping, cfg.ReapInterval)

	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(lis) }()
	slog.Info("governord: serving", "addr", lis.Addr().String(), "tenants", len(cfg.Tenants),
		"durable", cfg.DatabaseURL != "")

	select {
	case <-ctx.Done():
		slog.Info("governord: shutting down")
		drain(grpcServer)
		return nil
	case <-failed:
		// The engine is ahead of the record, so stop at once and restart from it.
		grpcServer.Stop()
		return errors.New("the store failed to commit; restart to recover from the record")
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	}
}

// drain lets in-flight calls finish, then cuts off whatever is still open.
func drain(s *grpc.Server) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(drainTimeout):
		// Streams such as WatchNode never finish by themselves.
		s.Stop()
	}
}
