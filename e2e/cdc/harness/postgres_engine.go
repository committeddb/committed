//go:build docker

package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/committeddb/committed/e2e/cdc/dataset"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// postgresEngine is the Postgres backing of Engine. It owns the source-database
// lifecycle (container, connection, schema) and the per-table replication-slot
// names, delegates config generation to the existing postIngestable / postSink*
// helpers, and implements the source-side operations the framework needs:
// readiness gating, sink reads, bulk load, and mutation execution
// (mutation.Execer).
type postgresEngine struct {
	container *tcpostgres.PostgresContainer
	ctx       context.Context
	conn      *pgx.Conn
	connStr   string
	slotNames map[string]string // table → replication slot name
}

func newPostgresEngine() *postgresEngine {
	return &postgresEngine{slotNames: map[string]string{}}
}

func (*postgresEngine) Dialect() string { return "postgres" }

// Start brings up the Postgres container, opens the source connection, and
// applies the schema (DDL) before committed boots.
func (e *postgresEngine) Start(ctx context.Context, t *testing.T) {
	t.Helper()
	e.ctx = ctx
	e.container, e.connStr = startPostgres(t)
	conn, err := pgx.Connect(ctx, e.connStr)
	require.NoError(t, err, "connect pgx")
	e.conn = conn
	require.NoError(t, applySchema(ctx, conn, dataset.Statements()), "apply schema")
}

// Close closes the source connection and terminates the container. Idempotent.
func (e *postgresEngine) Close() {
	if e.conn != nil {
		_ = e.conn.Close(context.Background())
		e.conn = nil
	}
	if e.container != nil {
		_ = e.container.Terminate(context.Background())
		e.container = nil
	}
}

// RestartContainer stops and restarts the Postgres container and reconnects.
// Used by source-restart tests (slot-resume correctness). Committed's ingestable
// reconnects on its own; callers re-gate readiness via WaitReadyCtx afterward.
func (e *postgresEngine) RestartContainer(ctx context.Context) error {
	if e.conn != nil {
		_ = e.conn.Close(ctx)
		e.conn = nil
	}
	if err := e.container.Stop(ctx, nil); err != nil {
		return fmt.Errorf("stop postgres: %w", err)
	}
	if err := e.container.Start(ctx); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	// The connection string includes a dynamic port; re-read it authoritatively.
	connStr, err := e.container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres connection string after restart: %w", err)
	}
	e.connStr = connStr
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("reconnect pgx after restart: %w", err)
	}
	e.conn = conn
	return nil
}

// ConnString exposes the source connection string (preflight tests query it).
func (e *postgresEngine) ConnString() string { return e.connStr }

// Conn exposes the raw pgx connection — a Postgres-only escape hatch for the
// preflight tests and the hand-rolled-transaction scenarios. Deliberately NOT on
// the Engine interface; reached via a type assertion on the harness side.
func (e *postgresEngine) Conn() *pgx.Conn { return e.conn }

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
