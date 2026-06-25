//go:build docker

package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/committeddb/committed/e2e/cdc/dataset"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// postgresEngine is the Postgres backing of Engine. It borrows the harness's
// source connection (the harness still owns the container + connection lifecycle
// in this slice) and owns the per-table replication-slot names. It delegates
// config generation to the existing postIngestable / postSink* helpers and
// implements the source-side operations the framework needs: readiness gating,
// sink reads, bulk load, and mutation execution (mutation.Execer).
type postgresEngine struct {
	ctx       context.Context
	conn      *pgx.Conn
	connStr   string
	slotNames map[string]string // table → replication slot name
}

func newPostgresEngine(ctx context.Context, conn *pgx.Conn, connStr string) *postgresEngine {
	return &postgresEngine{ctx: ctx, conn: conn, connStr: connStr, slotNames: map[string]string{}}
}

func (*postgresEngine) Dialect() string { return "postgres" }

func (e *postgresEngine) PostIngestable(t *testing.T, table string) {
	t.Helper()
	slot := fmt.Sprintf("slot_%s", table)
	pub := fmt.Sprintf("pub_%s", table)
	e.slotNames[table] = slot
	postIngestable(t, table, e.connStr, slot, pub)
}

func (e *postgresEngine) PostSinkDatabase(t *testing.T) {
	t.Helper()
	postSinkDatabase(t, e.connStr)
}

func (*postgresEngine) PostSyncable(t *testing.T, table string) {
	t.Helper()
	postSyncable(t, table)
}

func (e *postgresEngine) SlotName(table string) string { return e.slotNames[table] }

// WaitReady polls Postgres until the table's replication slot is streaming.
// pg_stat_replication.state == 'streaming' is the right gate for "snapshot done
// AND streaming live"; the slot's `active` flag flips true when the dialect
// connects (before snapshot), so racing on it would let test mutations slip into
// the snapshot batches and double-count. (Moved verbatim from the harness.)
func (e *postgresEngine) WaitReady(t *testing.T, table string) {
	t.Helper()
	slot := e.slotNames[table]
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		err := e.conn.QueryRow(e.ctx, `
			SELECT s.state
			FROM pg_stat_replication s
			JOIN pg_replication_slots r ON r.active_pid = s.pid
			WHERE r.slot_name = $1`, slot,
		).Scan(&state)
		if err == nil && state == "streaming" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Diagnostic: dump full slot state on timeout.
	var exists bool
	_ = e.conn.QueryRow(e.ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot,
	).Scan(&exists)
	if !exists {
		t.Fatalf("ingestable slot %q was never created — supervisor likely never spawned the dialect", slot)
	}
	var state string
	_ = e.conn.QueryRow(e.ctx, `
		SELECT s.state FROM pg_stat_replication s
		JOIN pg_replication_slots r ON r.active_pid = s.pid
		WHERE r.slot_name = $1`, slot).Scan(&state)
	t.Fatalf("ingestable slot %q never reached state=streaming (last observed state=%q)", slot, state)
}

// WaitReadyCtx is the context-bounded readiness gate used by source-restart
// paths. (Moved verbatim from the harness.)
func (e *postgresEngine) WaitReadyCtx(ctx context.Context, table string, timeout time.Duration) {
	slot := e.slotNames[table]
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var active bool
		err := e.conn.QueryRow(ctx,
			"SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot,
		).Scan(&active)
		if err == nil && active {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// SinkValue reads column col of the sink row keyed by pk, and whether it exists.
// (Moved verbatim from syncable.go.)
func (e *postgresEngine) SinkValue(table, pk, col string) (string, bool) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		col, SinkTable(table), dataset.PrimaryKey(table))
	var v string
	if err := e.conn.QueryRow(e.ctx, q, pk).Scan(&v); err != nil {
		return "", false
	}
	return v, true
}

// SinkCount returns the row count of a topic's sink table, 0 if absent.
// (Moved verbatim from syncable.go.)
func (e *postgresEngine) SinkCount(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := e.conn.QueryRow(e.ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", SinkTable(table))).Scan(&n); err != nil {
		return 0
	}
	return n
}

// Load bulk-inserts the dataset into Postgres (CopyFrom per table).
func (e *postgresEngine) Load(ctx context.Context, ds dataset.Dataset) error {
	return dataset.Load(ctx, e.conn, ds)
}

// Txn implements mutation.Execer: it runs fn inside one pgx transaction,
// committing on a nil return and rolling back otherwise.
func (e *postgresEngine) Txn(ctx context.Context, fn func(q mutation.Querier) error) error {
	tx, err := e.conn.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(pgQuerier{tx: tx}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// pgQuerier adapts a pgx.Tx to mutation.Querier.
type pgQuerier struct{ tx pgx.Tx }

func (q pgQuerier) Exec(ctx context.Context, query string, args ...any) error {
	_, err := q.tx.Exec(ctx, query, args...)
	return err
}

func (pgQuerier) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }
