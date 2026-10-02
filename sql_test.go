package governor

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres" // registers the goqu dialect
	_ "github.com/jackc/pgx/v5/stdlib"                  // registers the "pgx" driver

	"github.com/Avik-creator/governor/internal/core"
)

// sqlTable is the scratch table the transaction tests write to.
const sqlTable = "governor_sdk_test"

func TestDBRefusesBeforeTouchingTheDatabase(t *testing.T) {
	// Nothing listens here, so reaching the database would fail in its own way.
	raw, err := sql.Open("pgx", "postgres://nobody@127.0.0.1:1/none")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	db := NewDB("postgres", raw)
	ran := false
	fn := func(*goqu.TxDatabase) error {
		ran = true
		return nil
	}

	if err := db.Tx(t.Context(), fn); !errors.Is(err, ErrNoTask) {
		t.Errorf("Tx without a task = %v, want ErrNoTask", err)
	}
	b := newBackend(t, core.Spec{}, core.Spec{Quotas: map[core.Resource]int64{ResourceSQL: 0}})
	ctx, _ := mustTask(t, b.mustDial(), Spec{})
	if err := db.Tx(ctx, fn); !errors.Is(err, ErrDenied) {
		t.Errorf("Tx with no sql budget = %v, want ErrDenied", err)
	}
	if ran {
		t.Error("a refused transaction ran its function")
	}
}

func TestDBTx(t *testing.T) {
	dsn := os.Getenv("GOVERNOR_TEST_DSN")
	if dsn == "" {
		t.Skip("GOVERNOR_TEST_DSN is not set")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlTable); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), "CREATE TABLE "+sqlTable+" (name TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = raw.Exec("DROP TABLE IF EXISTS " + sqlTable) })

	var (
		mu      sync.Mutex
		reports []core.Report
	)
	b := newBackend(t,
		core.Spec{Limits: map[core.Class]int{ClassDB: 1}},
		core.Spec{Quotas: map[core.Resource]int64{ResourceSQL: 100}})
	b.observe(func(_ core.Class, r core.Report) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, r)
	})
	ctx, _ := mustTask(t, b.mustDial(), Spec{})
	db := NewDB("postgres", raw)
	insert := func(tx *goqu.TxDatabase, name string) error {
		_, err := tx.Insert(sqlTable).Rows(goqu.Record{"name": name}).Executor().ExecContext(ctx)
		return err
	}
	names := func() []string {
		t.Helper()
		var got []string
		err := db.Tx(ctx, func(tx *goqu.TxDatabase) error {
			return tx.From(sqlTable).Select("name").Order(goqu.C("name").Asc()).ScanValsContext(ctx, &got)
		})
		if err != nil {
			t.Fatalf("read names: %v", err)
		}
		return got
	}

	// Two statements commit together, on one lease that is held for the whole transaction.
	err = db.Tx(ctx, func(tx *goqu.TxDatabase) error {
		if err := insert(tx, "a"); err != nil {
			return err
		}
		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if _, err := Acquire(short, ClassDB); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Acquire during a transaction = %v, want DeadlineExceeded", err)
		}
		return insert(tx, "b")
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}

	// A failed transaction leaves nothing behind and still returns its lease.
	boom := errors.New("changed my mind")
	err = db.Tx(ctx, func(tx *goqu.TxDatabase) error {
		if err := insert(tx, "c"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Errorf("Tx = %v, want the function's error", err)
	}
	if got := names(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("rows = %v, want a and b", got)
	}

	// Three transactions ran, so three sql units and three leases were used.
	if got := b.usage(uint64(b.tenant), ResourceSQL); got != 3 {
		t.Errorf("sql used = %d, want 3", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 3 || reports[0].Latency < 50*time.Millisecond {
		t.Errorf("reports = %+v, want three with the first at least 50ms", reports)
	}
}
