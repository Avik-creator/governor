package bench

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/daemon"
)

// Keys of the tenants every scenario may use.
const (
	KeyA = "bench-tenant-a"
	KeyB = "bench-tenant-b"
)

// Options describes the governord a scenario runs against.
type Options struct {
	// Root holds the shared pools, and Tenant the caps each of the two tenants gets.
	Root   config.Caps
	Tenant config.Caps

	// Adaptive lists the root limits tuned by a controller.
	Adaptive []config.Adaptive

	// DatabaseURL is the Postgres to record to; empty keeps everything in memory.
	DatabaseURL string
}

// Harness is a real governord running in this process.
type Harness struct {
	cfg    *config.Config
	daemon *daemon.Daemon
	stop   context.CancelFunc
}

// Start runs governord on a free local port.
func Start(o Options) (*Harness, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("bench: find a port: %w", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		return nil, err
	}
	h := &Harness{cfg: &config.Config{
		Listen:       addr,
		DatabaseURL:  o.DatabaseURL,
		ReapInterval: 10 * time.Millisecond,
		DrainTimeout: 100 * time.Millisecond,
		Root:         o.Root,
		Adaptive:     o.Adaptive,
		Tenants: []config.Tenant{
			{Name: "tenant-a", APIKey: KeyA, Caps: o.Tenant},
			{Name: "tenant-b", APIKey: KeyB, Caps: o.Tenant},
		},
	}}
	return h, h.start()
}

func (h *Harness) start() error {
	ctx, stop := context.WithCancel(context.Background())
	d, err := daemon.Start(ctx, h.cfg)
	if err != nil {
		stop()
		return fmt.Errorf("bench: start governord: %w", err)
	}
	h.daemon, h.stop = d, stop
	return nil
}

// Addr returns the address governord listens on.
func (h *Harness) Addr() string {
	return h.cfg.Listen
}

// Dial opens a client session for the tenant with the given key.
func (h *Harness) Dial(ctx context.Context, key string, opts ...governor.Option) (*governor.Client, error) {
	base := []governor.Option{governor.WithAddr(h.Addr()), governor.WithAPIKey(key)}
	return governor.Dial(ctx, append(base, opts...)...)
}

// Stop shuts governord down and waits until it has released everything.
func (h *Harness) Stop() error {
	h.stop()
	return h.daemon.Wait()
}

// Restart stops governord and starts it again on the same address and database.
func (h *Harness) Restart() error {
	if err := h.Stop(); err != nil {
		return err
	}
	return h.start()
}
