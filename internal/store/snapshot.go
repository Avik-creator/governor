package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/doug-martin/goqu/v9"

	"github.com/Avik-creator/governor/internal/core"
)

// snapshotTable holds at most one row: the newest snapshot.
const snapshotTable = "snapshot"

// SaveSnapshot stores a snapshot and deletes the events it makes unnecessary, in one transaction.
func (s *Store) SaveSnapshot(ctx context.Context, snap *core.Snapshot) error {
	// The snapshot may only replace events that are themselves on record.
	if err := s.WaitDurable(ctx, snap.Seq); err != nil {
		return err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("store: encode snapshot: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	return tx.Wrap(func() error {
		row := goqu.Record{"singleton": true, "seq": snap.Seq, "data": string(data)}
		upsert := tx.Insert(snapshotTable).Prepared(true).Rows(row).
			OnConflict(goqu.DoUpdate("singleton", goqu.Record{"seq": snap.Seq, "data": string(data)}))
		if _, err := upsert.Executor().ExecContext(ctx); err != nil {
			return fmt.Errorf("store: save snapshot: %w", err)
		}
		drop := tx.Delete(eventsTable).Prepared(true).Where(goqu.C("seq").Lte(snap.Seq))
		if _, err := drop.Executor().ExecContext(ctx); err != nil {
			return fmt.Errorf("store: delete events: %w", err)
		}
		return nil
	})
}

// LoadSnapshot returns the stored snapshot, or nil if there is none.
func (s *Store) LoadSnapshot(ctx context.Context) (*core.Snapshot, error) {
	var data []byte
	query, args, err := s.db.From(snapshotTable).Select("data").ToSQL()
	if err != nil {
		return nil, fmt.Errorf("store: build query: %w", err)
	}
	switch err := s.sqlDB.QueryRowContext(ctx, query, args...).Scan(&data); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("store: load snapshot: %w", err)
	}
	snap := &core.Snapshot{}
	if err := json.Unmarshal(data, snap); err != nil {
		return nil, fmt.Errorf("store: decode snapshot: %w", err)
	}
	return snap, nil
}
