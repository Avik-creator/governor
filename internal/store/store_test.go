package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

// dsnEnv names the variable holding a Postgres these tests may wipe.
const dsnEnv = "GOVERNOR_TEST_DSN"

// opener opens one test database; fresh also empties its tables.
type opener func(t *testing.T, fresh bool) *Store

// eachBackend runs a test against SQLite, and against Postgres too if the variable names one.
func eachBackend(t *testing.T, test func(t *testing.T, open opener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		test(t, openerFor(SQLiteScheme+filepath.Join(t.TempDir(), "governor.db")))
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv(dsnEnv)
		if dsn == "" {
			t.Skipf("%s is not set", dsnEnv)
		}
		test(t, openerFor(dsn))
	})
}

func openerFor(dsn string) opener {
	return func(t *testing.T, fresh bool) *Store {
		t.Helper()
		s, err := Open(t.Context(), dsn)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if !fresh {
			return s
		}
		for _, table := range []string{eventsTable, tokensTable, snapshotTable} {
			if _, err := s.db.Delete(table).Executor().ExecContext(t.Context()); err != nil {
				t.Fatalf("empty %s: %v", table, err)
			}
		}
		s.mu.Lock()
		s.durable = 0
		s.mu.Unlock()
		return s
	}
}

// durable waits for seq, failing the test if the store cannot commit it.
func durable(t *testing.T, s *Store, seq uint64) {
	t.Helper()
	if err := s.WaitDurable(t.Context(), seq); err != nil {
		t.Fatalf("WaitDurable(%d): %v", seq, err)
	}
}

// count returns how many events are stored.
func count(t *testing.T, s *Store) int {
	t.Helper()
	n, err := s.db.From(eventsTable).CountContext(t.Context())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return int(n)
}

// transactions returns how many transactions wrote the stored events; only Postgres can tell.
func transactions(t *testing.T, s *Store) (int, bool) {
	t.Helper()
	if s.dialect != postgres.goose {
		return 0, false
	}
	var n int
	row := s.sqlDB.QueryRowContext(t.Context(), "SELECT count(DISTINCT xmin::text) FROM events")
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	return n, true
}

func TestRestartRestoresEngine(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
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
		r, err := core.Restore(core.Config{Clock: clock, Sink: s}, nil, s.Events(t.Context()))
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
	})
}

func TestGroupCommit(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
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

		events := count(t, s)
		if want := workers*each + 2; events != want {
			t.Errorf("stored %d events, want %d", events, want)
		}
		// Every caller waited for its own event, yet commits were shared between them.
		if n, ok := transactions(t, s); ok && n >= events/2 {
			t.Errorf("%d events took %d transactions, want batching", events, n)
		}
	})
}

func TestWriteFailureIsFatal(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
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
	})
}

func TestWaitDurable(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
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
	})
}

func TestTokens(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
		s := openStore(t, true)
		ctx := t.Context()
		first, second := sha256.Sum256([]byte("first")), sha256.Sum256([]byte("second"))
		if err := s.SaveToken(ctx, first, 7); err != nil {
			t.Fatalf("SaveToken: %v", err)
		}
		if err := s.SaveToken(ctx, second, 9); err != nil {
			t.Fatalf("SaveToken: %v", err)
		}
		if err := s.SaveToken(ctx, first, 8); err == nil {
			t.Error("SaveToken accepted a hash that is already stored")
		}

		// Tokens are read by a later process, so reopen before loading them.
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		s = openStore(t, false)
		got, err := s.LoadTokens(ctx)
		if err != nil {
			t.Fatalf("LoadTokens: %v", err)
		}
		want := map[[sha256.Size]byte]core.SessionID{first: 7, second: 9}
		if !maps.Equal(got, want) {
			t.Errorf("LoadTokens = %v, want %v", got, want)
		}

		for range 2 {
			if err := s.DeleteToken(ctx, first); err != nil {
				t.Fatalf("DeleteToken: %v", err)
			}
		}
		if got, _ := s.LoadTokens(ctx); len(got) != 1 || got[second] != 9 {
			t.Errorf("LoadTokens after a delete = %v, want only the second token", got)
		}
	})
}

func TestSnapshot(t *testing.T) {
	eachBackend(t, func(t *testing.T, openStore opener) {
		s := openStore(t, true)
		ctx := t.Context()
		if snap, err := s.LoadSnapshot(ctx); err != nil || snap != nil {
			t.Fatalf("LoadSnapshot on an empty database = %v, %v, want nil", snap, err)
		}
		clock := core.NewManualClock(time.Unix(1_700_000_000, 0))
		e, err := core.New(core.Config{Clock: clock, Sink: s, Root: core.Spec{Limits: map[core.Class]int{"db": 1}}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		admin, _, _, _ := e.OpenSession(core.RootID, 0)
		tenant, _, _ := e.CreateNode(admin, core.RootID, core.Spec{Name: "tenant", Quotas: map[core.Resource]int64{"http": 10}})
		if _, err := e.Consume(admin, tenant, "http", 6); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		lease, _, err := e.Acquire(ctx, admin, tenant, "db", core.AcquireOptions{})
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}

		// The snapshot replaces every event up to it, and later events are kept.
		snap := e.Snapshot()
		if err := s.SaveSnapshot(ctx, snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if events := count(t, s); events != 0 {
			t.Errorf("%d events remain after the snapshot, want 0", events)
		}
		seq, err := e.Consume(admin, tenant, "http", 1)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		durable(t, s, seq)
		if events := count(t, s); events != 1 {
			t.Errorf("%d events stored after the snapshot, want 1", events)
		}
		// A second snapshot replaces the first.
		if err := s.SaveSnapshot(ctx, e.Snapshot()); err != nil {
			t.Fatalf("second SaveSnapshot: %v", err)
		}
		seq, err = e.Consume(admin, tenant, "http", 2)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		durable(t, s, seq)
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		s = openStore(t, false)
		if got := s.LastSeq(); got != seq {
			t.Fatalf("LastSeq after reopening = %d, want %d", got, seq)
		}
		loaded, err := s.LoadSnapshot(ctx)
		if err != nil || loaded == nil || loaded.Seq != seq-1 {
			t.Fatalf("LoadSnapshot = %+v, %v, want the snapshot at event %d", loaded, err, seq-1)
		}
		r, err := core.Restore(core.Config{Clock: clock, Sink: s}, loaded, s.Events(ctx))
		if err != nil {
			t.Fatalf("Restore: %v", err)
		}
		_, err = r.Consume(admin, tenant, "http", 2)
		if d, ok := errors.AsType[*core.DeniedError](err); !ok || d.Used != 9 {
			t.Errorf("Consume = %v, want a denial at 9 used", err)
		}
		if err := r.Validate(lease); err != nil {
			t.Errorf("Validate(lease from before the snapshot): %v", err)
		}
		// With nothing but a snapshot on record, the store still knows where the record ends.
		if err := s.SaveSnapshot(ctx, r.Snapshot()); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if s = openStore(t, false); s.LastSeq() != seq {
			t.Errorf("LastSeq with only a snapshot = %d, want %d", s.LastSeq(), seq)
		}
	})
}

func TestSQLiteIsOpenedDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir with spaces", "governor.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Both spellings of the URL name the same file.
	for _, dsn := range []string{SQLiteScheme + path, SQLiteScheme + "//" + path} {
		s, err := Open(t.Context(), dsn)
		if err != nil {
			t.Fatalf("Open(%q): %v", dsn, err)
		}
		var mode string
		var sync int
		if err := s.sqlDB.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatalf("journal_mode: %v", err)
		}
		if err := s.sqlDB.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&sync); err != nil {
			t.Fatalf("synchronous: %v", err)
		}
		// 2 is FULL: a commit is on disk before it is reported.
		if mode != "wal" || sync != 2 {
			t.Errorf("journal_mode, synchronous = %s, %d, want wal, 2", mode, sync)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the database file was not created at the given path: %v", err)
	}
}
