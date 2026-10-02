package governor

import (
	"context"
	"database/sql"

	"github.com/doug-martin/goqu/v9"
)

// Names a transaction charges and holds; configure caps under these names.
const (
	ResourceSQL = "sql"
	ClassDB     = "db"
)

// DB is a database whose only way in is a governed transaction.
type DB struct {
	db *goqu.Database
}

// NewDB wraps db for the named goqu dialect, such as "postgres".
func NewDB(dialect string, db *sql.DB) *DB {
	return &DB{db: goqu.New(dialect, db)}
}

// Tx runs fn in a transaction that costs one sql unit and holds one db lease throughout.
func (d *DB) Tx(ctx context.Context, fn func(tx *goqu.TxDatabase) error) error {
	if err := Consume(ctx, ResourceSQL, 1); err != nil {
		return err
	}
	return Do(ctx, ClassDB, func(ctx context.Context) error {
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		// Wrap commits when fn returns nil and rolls back on an error or a panic.
		return tx.Wrap(func() error { return fn(tx) })
	}, WithMaxHold(maxHold))
}
