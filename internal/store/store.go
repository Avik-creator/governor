// Package store keeps the engine's events in Postgres as the durable record.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres" // registers the goqu dialect
	_ "github.com/jackc/pgx/v5/stdlib"                  // registers the "pgx" driver
	"github.com/pressly/goose/v3"

	"github.com/Avik-creator/governor/internal/core"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// eventsTable is the table that holds one row per engine event.
	eventsTable = "events"

	// maxRows caps the rows of one INSERT, keeping it under the parameter limit.
	maxRows = 1000

	// flushTimeout bounds one transaction, so a hung database counts as a failure.
	flushTimeout = 10 * time.Second
)

// ErrClosed reports a wait on a store that was closed before the event was written.
var ErrClosed = errors.New("store: closed")

// Store writes events in batches; it is a core.Sink and a server.Committer.
type Store struct {
	sqlDB *sql.DB
	db    *goqu.Database

	mu      sync.Mutex
	pending []core.Event  // emitted but not yet taken by the writer
	durable uint64        // seq of the last committed event
	err     error         // set once, when a write fails or the store closes
	changed chan struct{} // closed and replaced whenever durable or err changes

	wake   chan struct{} // tells the writer that pending is not empty
	stop   chan struct{} // tells the writer to flush and exit
	done   chan struct{} // closed when the writer has exited
	failed chan struct{} // closed when a write has failed
	once   sync.Once
}

// Open connects to Postgres, applies the migrations and starts the writer.
func Open(ctx context.Context, dsn string) (*Store, error) {
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	s := &Store{
		sqlDB:   sqlDB,
		db:      goqu.New("postgres", sqlDB),
		changed: make(chan struct{}),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		failed:  make(chan struct{}),
	}
	if err := s.init(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

// init applies the migrations and reads how far the stored events go.
func (s *Store) init(ctx context.Context) error {
	dir, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("store: migrations: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, s.sqlDB, dir)
	if err != nil {
		return fmt.Errorf("store: migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	var last sql.NullInt64
	query := s.db.From(eventsTable).Select(goqu.MAX("seq"))
	if _, err := query.ScanValContext(ctx, &last); err != nil {
		return fmt.Errorf("store: read last seq: %w", err)
	}
	s.durable = uint64(last.Int64)
	return nil
}

// LastSeq returns the seq of the last event known to be committed.
func (s *Store) LastSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}

// Events yields every stored event in order, for core.Restore.
func (s *Store) Events(ctx context.Context) iter.Seq2[core.Event, error] {
	return func(yield func(core.Event, error) bool) {
		query, args, err := s.db.From(eventsTable).Select("data").Order(goqu.C("seq").Asc()).ToSQL()
		if err != nil {
			yield(core.Event{}, fmt.Errorf("store: build query: %w", err))
			return
		}
		rows, err := s.sqlDB.QueryContext(ctx, query, args...)
		if err != nil {
			yield(core.Event{}, fmt.Errorf("store: read events: %w", err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			var data []byte
			var ev core.Event
			if err := rows.Scan(&data); err != nil {
				yield(core.Event{}, fmt.Errorf("store: read events: %w", err))
				return
			}
			if err := json.Unmarshal(data, &ev); err != nil {
				yield(core.Event{}, fmt.Errorf("store: decode event: %w", err))
				return
			}
			if !yield(ev, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(core.Event{}, fmt.Errorf("store: read events: %w", err))
		}
	}
}

// Failed returns a channel that is closed once a write has failed.
func (s *Store) Failed() <-chan struct{} {
	return s.failed
}

// Emit queues an event for the writer; it never blocks on the database.
func (s *Store) Emit(ev core.Event) {
	s.mu.Lock()
	s.pending = append(s.pending, ev)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// WaitDurable blocks until every event up to seq is committed, or fails.
func (s *Store) WaitDurable(ctx context.Context, seq uint64) error {
	for {
		s.mu.Lock()
		durable, err, changed := s.durable, s.err, s.changed
		s.mu.Unlock()
		switch {
		case durable >= seq:
			return nil
		case err != nil:
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close writes what is still pending, stops the writer and closes the database.
func (s *Store) Close() error {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	return s.sqlDB.Close()
}

// run is the writer: it commits everything pending as one transaction at a time.
func (s *Store) run() {
	defer close(s.done)
	for {
		stopping := false
		select {
		case <-s.wake:
		case <-s.stop:
			stopping = true
		}
		s.mu.Lock()
		batch := s.pending
		s.pending = nil
		s.mu.Unlock()

		err := s.write(batch)
		s.mu.Lock()
		switch {
		case err != nil:
			s.err = err
		case len(batch) > 0:
			s.durable = batch[len(batch)-1].Seq
		}
		if err == nil && stopping {
			s.err = ErrClosed
		}
		close(s.changed)
		s.changed = make(chan struct{})
		s.mu.Unlock()

		switch {
		case stopping:
			return
		case err != nil:
			// The engine is now ahead of the record, so the process must restart.
			close(s.failed)
			return
		}
	}
}

// write commits a batch of events in one transaction.
func (s *Store) write(batch []core.Event) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	return tx.Wrap(func() error {
		for len(batch) > 0 {
			n := min(len(batch), maxRows)
			rows := make([]any, n)
			for i, ev := range batch[:n] {
				data, err := json.Marshal(ev)
				if err != nil {
					return fmt.Errorf("store: encode event %d: %w", ev.Seq, err)
				}
				rows[i] = goqu.Record{"seq": ev.Seq, "kind": string(ev.Kind), "at": ev.Time, "data": string(data)}
			}
			insert := tx.Insert(eventsTable).Prepared(true).Rows(rows...)
			if _, err := insert.Executor().ExecContext(ctx); err != nil {
				return fmt.Errorf("store: insert events: %w", err)
			}
			batch = batch[n:]
		}
		return nil
	})
}
