// Command governord is the Governor daemon: one engine served over gRPC.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/daemon"
)

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
	d, err := daemon.Start(ctx, cfg)
	if err != nil {
		return err
	}
	return d.Wait()
}
