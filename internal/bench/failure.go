package bench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// CrashParams sizes the worker crash scenario.
type CrashParams struct {
	// Slots is the size of the pool the dead worker was holding all of.
	Slots int

	// TTL is how long the dead worker's session lasts without a heartbeat.
	TTL time.Duration
}

// DefaultCrash kills a worker that holds all 4 slots of a pool, on a one second session.
var DefaultCrash = CrashParams{Slots: 4, TTL: time.Second}

// Crash kills a worker that holds a whole pool, and times how long until another worker can use it.
func Crash(ctx context.Context, p CrashParams) (Report, error) {
	report := Report{
		Scenario: "Worker crash",
		Note: fmt.Sprintf("A worker takes all %d slots of a pool and dies without releasing them. "+
			"Its session lasts %v without a heartbeat.", p.Slots, p.TTL),
		Columns: []string{"measured"},
	}
	h, err := Start(Options{Root: config.Caps{Limits: map[core.Class]int{class: p.Slots}}})
	if err != nil {
		return report, err
	}
	defer h.Stop()

	// The doomed worker speaks the protocol directly, so that nothing heartbeats for it.
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return report, err
	}
	defer conn.Close()
	rpc := pb.NewGovernorServiceClient(conn)
	as := func(secret string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+secret)
	}
	session, err := rpc.OpenSession(as(KeyA), &pb.OpenSessionRequest{Ttl: durationpb.New(p.TTL)})
	if err != nil {
		return report, err
	}
	dead := as(session.GetSessionToken())
	leases := make([]uint64, p.Slots)
	for i := range leases {
		l, err := rpc.Acquire(dead, &pb.AcquireRequest{NodeId: session.GetScopeId(), Class: class})
		if err != nil {
			return report, err
		}
		leases[i] = l.GetLeaseId()
	}
	died := time.Now()

	// A healthy worker of another tenant now needs the pool.
	survivor, err := h.Dial(ctx, KeyB)
	if err != nil {
		return report, err
	}
	defer survivor.Close()
	run, _, err := survivor.NewTask(ctx, governor.Spec{Name: "survivor"})
	if err != nil {
		return report, err
	}
	lease, err := governor.Acquire(run, class)
	if err != nil {
		return report, err
	}
	reclaimed := time.Since(died)
	defer lease.Release(governor.Report{})

	// The dead worker wakes up and tries to use what it thinks it still holds.
	refused := 0
	for _, id := range leases {
		if _, err := rpc.Release(dead, &pb.ReleaseRequest{LeaseId: id}); err != nil {
			refused++
		}
	}
	report.add("session TTL (ms)", ms(p.TTL))
	report.add("pool usable by another worker after (ms)", ms(reclaimed))
	report.add("leases the dead worker held", float64(p.Slots))
	report.add("late releases by the dead worker that were refused", float64(refused))
	report.add("survivor's lease id is newer than every stale one (1 = yes)", newer(lease.ID(), leases))
	return report, nil
}

// newer reports 1 if id is greater than every stale id, which is what a fencing check relies on.
func newer(id uint64, stale []uint64) float64 {
	for _, s := range stale {
		if id <= s {
			return 0
		}
	}
	return 1
}

// RestartParams sizes the daemon restart scenario.
type RestartParams struct {
	// Workers charge one unit at a time for Duration; governord is restarted a third of the way in.
	Workers  int
	Duration time.Duration

	// DatabaseURL is a throwaway Postgres; the scenario empties Governor's tables in it.
	DatabaseURL string
}

// DefaultRestart has 4 workers charge units for 3 seconds across a restart.
var DefaultRestart = RestartParams{Workers: 4, Duration: 3 * time.Second}

// ErrNoDatabase reports a scenario that needs Postgres and was given none.
var ErrNoDatabase = errors.New("bench: this scenario needs a database")

// wipe empties Governor's tables, which exist only once a store has opened the database.
func wipe(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	// A table is only there once a store has opened the database with that migration.
	for _, table := range []string{"events", "session_tokens", "snapshot"} {
		var exists bool
		query := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
		if err := db.QueryRowContext(ctx, query, table).Scan(&exists); err != nil {
			return fmt.Errorf("bench: inspect database: %w", err)
		}
		if !exists {
			continue
		}
		if _, err := db.ExecContext(ctx, "TRUNCATE "+table); err != nil {
			return fmt.Errorf("bench: empty %s: %w", table, err)
		}
	}
	return nil
}

// Restart stops governord while workers are charging against it, starts it again and checks what survived.
func Restart(ctx context.Context, p RestartParams) (Report, error) {
	report := Report{
		Scenario: "governord restart",
		Note: fmt.Sprintf("%d workers charge one unit at a time for %v. A third of the way in, governord is "+
			"stopped and started again from Postgres.", p.Workers, p.Duration),
		Columns: []string{"measured"},
	}
	if p.DatabaseURL == "" {
		return report, ErrNoDatabase
	}
	if err := wipe(ctx, p.DatabaseURL); err != nil {
		return report, err
	}
	const budget = 1 << 40
	h, err := Start(Options{
		Root:        config.Caps{Limits: map[core.Class]int{class: 1}},
		Tenant:      config.Caps{Quotas: map[core.Resource]int64{"units": budget}},
		DatabaseURL: p.DatabaseURL,
	})
	if err != nil {
		return report, err
	}
	defer func() { _ = h.Stop() }()
	c, err := h.Dial(ctx, KeyA)
	if err != nil {
		return report, err
	}
	defer c.Close()
	run, _, err := c.NewTask(ctx, governor.Spec{Name: "across-restart"})
	if err != nil {
		return report, err
	}
	lease, err := governor.Acquire(run, class)
	if err != nil {
		return report, err
	}

	var acknowledged, refused atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for range p.Workers {
		wg.Go(func() {
			for time.Since(start) < p.Duration {
				if governor.Consume(run, "units", 1) == nil {
					acknowledged.Add(1)
				} else {
					refused.Add(1)
				}
			}
		})
	}
	time.Sleep(p.Duration / 3)
	before := acknowledged.Load()
	down := time.Now()
	if err := h.Restart(); err != nil {
		return report, err
	}
	downtime := time.Since(down)
	wg.Wait()

	// The same client, with the session and lease it had before, asks what the record says.
	survived := 0.0
	if lease.Validate(ctx) == nil && c.Err() == nil {
		survived = 1
	}
	recorded := -1.0
	if d, ok := errors.AsType[*governor.DeniedError](governor.Consume(run, "units", budget)); ok {
		recorded = float64(d.Used)
	}
	report.add("downtime (ms)", ms(downtime))
	report.add("charges acknowledged before the restart", float64(before))
	report.add("charges acknowledged in total", float64(acknowledged.Load()))
	report.add("charges on record after the restart", recorded)
	report.add("calls refused while governord was down", float64(refused.Load()))
	report.add("session and lease still valid after the restart (1 = yes)", survived)
	return report, nil
}
