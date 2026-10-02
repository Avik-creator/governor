package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/avikmukherjee/governor/internal/core"
)

// dsnEnv names the variable holding a Postgres these tests may wipe.
const dsnEnv = "GOVERNOR_TEST_DSN"

// openStore opens the test database; fresh also empties its events.
func openStore(t *testing.T, fresh bool) *Store {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set", dsnEnv)
	}
	s, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if fresh {
		if _, err := s.db.Truncate(eventsTable).Executor().ExecContext(t.Context()); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		s.mu.Lock()
		s.durable = 0
		s.mu.Unlock()
	}
	return s
}

// durable waits for seq, failing the test if the store cannot commit it.
func durable(t *testing.T, s *Store, seq uint64) {
	t.Helper()
	if err := s.WaitDurable(t.Context(), seq); err != nil {
		t.Fatalf("WaitDurable(%d): %v", seq, err)
	}
}

// count returns how many events are stored and in how many transactions.
func count(t *testing.T, s *Store) (events, transactions int) {
	t.Helper()
	row := s.sqlDB.QueryRowContext(t.Context(), "SELECT count(*), count(DISTINCT xmin::text) FROM events")
	if err := row.Scan(&events, &transactions); err != nil {
		t.Fatalf("count: %v", err)
	}
	return events, transactions
}

func TestRestartRestoresEngine(t *testing.T) {
	s := openStore(t, true)
	clock := core.NewManualClock(time.Unix(1_700_000_000, 0))
	root := core.Spec{Limits: map[core.Class]int{"db": 1}}
	e, err := core.New(core.Config{Clock: clock, Sink: s, Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin, _, _, _ := e.OpenSession(core.RootID, 0)
	tenant, _, _ := e.CreateNode(admin, core.RootID, core.Spec{
		Name:     "tenant",
		Quotas:   map[core.Resource]int64{"http": 10},
		Deadline: clock.Now().Add(time.Hour),
	})
	worker, _, _, _ := e.OpenSession(tenant, time.Minute)
	if _, err := e.Consume(worker, tenant, "http", 7); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	lease, seq, err := e.Acquire(t.Context(), worker, tenant, "db", core.AcquireOptions{})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	durable(t, s, seq)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s = openStore(t, false)
	if got := s.LastSeq(); got != seq {
		t.Fatalf("LastSeq after reopening = %d, want %d", got, seq)
	}
	r, err := core.Restore(core.Config{Clock: clock, Sink: s}, s.Events(t.Context()))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	_, err = r.Consume(worker, tenant, "http", 4)
	if d, ok := errors.AsType[*core.DeniedError](err); !ok || d.Used != 7 || d.Limit != 10 || d.Name != "tenant" {
		t.Errorf("Consume = %v, want a denial at 7 of 10 on tenant", err)
	}
	if err := r.Validate(lease); err != nil {
		t.Errorf("Validate(lease held before the restart): %v", err)
	}
	// The restored engine carries on from the stored seq, so its writes are accepted.
	next, err := r.Release(worker, lease, core.Report{})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if next != seq+1 {
		t.Errorf("first seq after the restart = %d, want %d", next, seq+1)
	}
	durable(t, s, next)

	clock.Advance(time.Hour)
	if state, _ := r.State(tenant); state != core.StateDeadlineExceeded {
		t.Errorf("state an hour later = %s, want %s", state, core.StateDeadlineExceeded)
	}
}

func TestGroupCommit(t *testing.T) {
	s := openStore(t, true)
	root := core.Spec{Quotas: map[core.Resource]int64{"http": 1 << 40}}
	e, err := core.New(core.Config{Sink: s, Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	admin, _, _, _ := e.OpenSession(core.RootID, 0)

	const workers, each = 32, 50
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				seq, err := e.Consume(admin, core.RootID, "http", 1)
				if err != nil {
					t.Errorf("Consume: %v", err)
					return
				}
				if err := s.WaitDurable(t.Context(), seq); err != nil {
					t.Errorf("WaitDurable: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()

	events, transactions := count(t, s)
	if want := workers*each + 2; events != want {
		t.Errorf("stored %d events, want %d", events, want)
	}
	// Every caller waited for its own event, yet commits were shared between them.
	if transactions >= events/2 {
		t.Errorf("%d events took %d transactions, want batching", events, transactions)
	}
	t.Logf("%d events in %d transactions", events, transactions)
}

func TestWriteFailureIsFatal(t *testing.T) {
	s := openStore(t, true)
	s.Emit(core.Event{Seq: 1, Kind: core.EventConsumed, Time: time.Now()})
	durable(t, s, 1)

	// A second event with the same seq cannot be stored.
	s.Emit(core.Event{Seq: 1, Kind: core.EventConsumed, Time: time.Now()})
	select {
	case <-s.Failed():
	case <-time.After(5 * time.Second):
		t.Fatal("Failed is still open after a write failed")
	}
	if err := s.WaitDurable(t.Context(), 2); err == nil || errors.Is(err, ErrClosed) {
		t.Errorf("WaitDurable after a failure = %v, want the write error", err)
	}
	// What was committed before the failure stays durable.
	if err := s.WaitDurable(t.Context(), 1); err != nil {
		t.Errorf("WaitDurable(1) = %v, want nil", err)
	}
}

func TestWaitDurable(t *testing.T) {
	s := openStore(t, true)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := s.WaitDurable(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitDurable for an event never emitted = %v, want DeadlineExceeded", err)
	}

	// Close writes what is pending before it stops.
	s.Emit(core.Event{Seq: 1, Kind: core.EventConsumed, Time: time.Now()})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.WaitDurable(t.Context(), 1); err != nil {
		t.Errorf("WaitDurable(1) after Close = %v, want nil", err)
	}
	if err := s.WaitDurable(t.Context(), 2); !errors.Is(err, ErrClosed) {
		t.Errorf("WaitDurable(2) after Close = %v, want ErrClosed", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
