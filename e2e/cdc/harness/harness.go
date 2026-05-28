//go:build docker

// Package harness wires together a Postgres container, a committed
// child process, per-table ingestables, and proposal stream capture
// into a single per-test fixture. Tests construct a Harness via
// New(t), apply their dataset and mutation script, then call
// Capture(t) to get back the actual proposal stream for the oracle.
package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/philborlin/committed/e2e/cdc/dataset"
)

// Harness owns the per-test fixture lifecycle. One Harness per test;
// t.Cleanup tears everything down so the next test starts fresh.
type Harness struct {
	pg        *tcpostgres.PostgresContainer
	pgConnStr string
	pgConn    *pgx.Conn
	committed *committedProcess
	topics    []string
	slotNames map[string]string // topic → slot name
	ctx       context.Context
	cancel    context.CancelFunc
	// baseline is the per-topic proposal count captured AFTER dataset
	// load and ingestable startup. Capture() trims away everything up
	// to this count so each test only sees its mutation script output.
	baseline map[string]int
}

// Options configures a Harness. Currently just the table set — tests
// that don't touch all 8 TPC-H tables can opt in to a subset to keep
// per-test setup time down (each table = one ingestable = one slot).
type Options struct {
	Tables []string // defaults to dataset.Tables (all 8)
}

// New brings up Postgres + committed + ingestables and returns a ready
// fixture. The dataset is NOT loaded here — call Load() if needed.
// The harness records baseline proposal counts BEFORE you load (since
// pre-load there are zero CDC events), so Capture() correctly subtracts
// both the empty baseline AND the dataset-load events from any
// subsequent mutation capture (if you load via Load() it bumps the
// baseline again after the load completes).
func New(t *testing.T, opts ...Options) *Harness {
	t.Helper()

	o := Options{Tables: dataset.Tables}
	if len(opts) > 0 && len(opts[0].Tables) > 0 {
		o.Tables = opts[0].Tables
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &Harness{
		topics:    o.Tables,
		slotNames: make(map[string]string, len(o.Tables)),
		ctx:       ctx,
		cancel:    cancel,
	}

	// 1. Postgres.
	h.pg, h.pgConnStr = startPostgres(t)

	pgConn, err := pgx.Connect(ctx, h.pgConnStr)
	require.NoError(t, err, "connect pgx")
	h.pgConn = pgConn

	// 2. Schema (DDL) — applied before committed boots so the publication
	// created later finds the tables it references.
	require.NoError(t, applySchema(ctx, pgConn, dataset.Statements()), "apply schema")

	// 3. committed.
	h.committed = startCommitted(t)

	// 4. One Type per table.
	for _, table := range o.Tables {
		postType(t, table)
	}

	// 5. One ingestable per table.
	for _, table := range o.Tables {
		slot := fmt.Sprintf("slot_%s", table)
		pub := fmt.Sprintf("pub_%s", table)
		h.slotNames[table] = slot
		postIngestable(t, table, h.pgConnStr, slot, pub)
	}

	// 6. Wait for every slot to be active.
	for _, table := range o.Tables {
		h.waitForIngestableReady(t, h.slotNames[table])
	}

	// 7. Establish baseline. At this point no CDC events have flowed
	// (tables are empty); baseline should be 0 per topic. Recorded
	// anyway so Load() and the test logic share one rebaseline path.
	h.baseline = h.snapshotCounts(t)

	t.Cleanup(func() { h.Close() })
	return h
}

// Load bulk-inserts the dataset into Postgres via COPY. After load
// returns, the harness waits for each touched topic to see exactly one
// new proposal (because COPY runs in one txn per table = one proposal
// per table), then rebaselines.
func (h *Harness) Load(t *testing.T, ds dataset.Dataset) {
	t.Helper()
	// Per-table expected proposal counts. Only the tables the test
	// asked the harness to track receive a COPY here, but every
	// configured table has a publication so even zero-row tables
	// produce one COPY (empty) — except COPY of zero rows actually
	// produces zero WAL events. Build the expected map by checking
	// each table's row count in the dataset.
	expected := make(map[string]int, len(h.topics))
	for _, table := range h.topics {
		if rowCount(ds, table) > 0 {
			expected[table] = 1
		}
	}
	require.NoError(t, dataset.Load(h.ctx, h.pgConn, ds), "load dataset")
	h.waitForCounts(t, expected, 60*time.Second)
	h.baseline = h.snapshotCounts(t)
}

// rowCount returns the number of rows in ds for the named table.
// Centralized so Load doesn't sprawl into a per-table switch in two
// places (here and dataset.Load).
func rowCount(ds dataset.Dataset, table string) int {
	switch table {
	case "region":
		return len(ds.Region)
	case "nation":
		return len(ds.Nation)
	case "supplier":
		return len(ds.Supplier)
	case "customer":
		return len(ds.Customer)
	case "part":
		return len(ds.Part)
	case "partsupp":
		return len(ds.PartSupp)
	case "orders":
		return len(ds.Orders)
	case "lineitem":
		return len(ds.LineItem)
	}
	return 0
}

// Conn returns the pgx connection bound to the test Postgres. Tests
// use this to issue their mutation SQL.
func (h *Harness) Conn() *pgx.Conn {
	return h.pgConn
}

// Topics returns the topic IDs this harness is watching, one per
// configured table.
func (h *Harness) Topics() []string {
	return h.topics
}

// Capture waits for at least expected[topic] new proposals on each
// topic, then returns all new proposals per topic in commit order AND
// advances the baseline. Use `h.Capture(t, script.ExpectedCounts())`.
//
// Why proposal-count gating instead of LSN catch-up: when a slot has
// no streaming traffic, its confirmed_flush_lsn doesn't advance (the
// dialect's clientXLogPos only moves on XLogData receipt). For tests
// that drive few events, an LSN gate can stall well past sensible
// timeouts. Proposal-count gating matches what the oracle actually
// checks — and any surplus proposals beyond the expected count are
// surfaced to the oracle, not silently dropped.
//
// Pass nil expected to skip the wait entirely (drain-only flush).
func (h *Harness) Capture(t *testing.T, expected map[string]int) map[string][]CapturedProposal {
	t.Helper()
	if expected != nil {
		h.waitForCounts(t, expected, 30*time.Second)
	}

	out := make(map[string][]CapturedProposal, len(h.topics))
	for _, topic := range h.topics {
		all := fetchProposals(t, topic)
		out[topic] = all[h.baseline[topic]:]
		h.baseline[topic] = len(all)
	}
	return out
}

// Rebaseline advances the baseline to the current per-topic proposal
// counts without waiting for anything. Useful after a seed phase whose
// proposal counts the test doesn't want to compute up front.
func (h *Harness) Rebaseline(t *testing.T) {
	t.Helper()
	for _, topic := range h.topics {
		h.baseline[topic] = len(fetchProposals(t, topic))
	}
}

// waitForCounts polls each topic until the proposal count reaches
// baseline+expected, or timeout fires.
func (h *Harness) waitForCounts(t *testing.T, expected map[string]int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for topic, n := range expected {
		target := h.baseline[topic] + n
		for {
			got := len(fetchProposals(t, topic))
			if got >= target {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %q: timed out waiting for %d proposals (got %d, baseline %d, expected %d new)",
					topic, target, got, h.baseline[topic], n)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Close releases all resources owned by the harness. Idempotent.
func (h *Harness) Close() {
	if h.pgConn != nil {
		_ = h.pgConn.Close(context.Background())
		h.pgConn = nil
	}
	if h.committed != nil {
		h.committed.Stop()
	}
	if h.pg != nil {
		_ = h.pg.Terminate(context.Background())
		h.pg = nil
	}
	if h.cancel != nil {
		h.cancel()
	}
}

// ConnString exposes the Postgres connection string. Used by the
// preflight tests that query Postgres directly.
func (h *Harness) ConnString() string {
	return h.pgConnStr
}

// SlotName returns the replication slot name for a given table. Used
// by preflight tests and restart-resume verification.
func (h *Harness) SlotName(table string) string {
	return h.slotNames[table]
}

// RestartCommitted stops the committed child process and starts a
// fresh one against the same data dir. Postgres is untouched, so the
// replication slot keeps its position. After restart, committed
// replays its WAL, re-loads ingestable configs, and reads back the
// persisted IngestablePosition via Storage.Position so the ingestable
// resumes from the last acked LSN rather than re-snapshotting.
//
// This is the test path for the resume-from-checkpoint feature added
// in this PR — it exercises Storage.Position end-to-end without
// needing a Postgres restart (which testcontainers can't do cleanly
// because Stop+Start reassigns the host port).
func (h *Harness) RestartCommitted(t *testing.T) {
	t.Helper()
	dataDir := h.committed.dataDir
	h.committed.Stop()
	h.committed = startCommittedAt(t, dataDir)

	// Wait for each ingestable's slot to be in streaming state again.
	// On restart, committed re-applies the ingestable config, the
	// supervisor spawns the dialect, and the dialect reconnects to
	// the existing slot from the persisted position.
	for _, table := range h.topics {
		h.waitForIngestableReady(t, h.slotNames[table])
	}
}

// RestartPostgres stops and restarts the Postgres container, then
// reconnects the harness's pgx connection. Committed's ingestable
// reconnects on its own (see postgres.go:135 "postgres replication
// stream exited, will reconnect"). Used by TestRestartResume to verify
// slot-resume correctness.
func (h *Harness) RestartPostgres(ctx context.Context) error {
	if h.pgConn != nil {
		_ = h.pgConn.Close(ctx)
		h.pgConn = nil
	}
	if err := h.pg.Stop(ctx, nil); err != nil {
		return fmt.Errorf("stop postgres: %w", err)
	}
	if err := h.pg.Start(ctx); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	// The connection string includes a dynamic port. testcontainers-go
	// can reuse the same port across Stop/Start, but ConnectionString
	// is the authoritative way to find out — re-read it.
	connStr, err := h.pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres connection string after restart: %w", err)
	}
	h.pgConnStr = connStr
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("reconnect pgx after restart: %w", err)
	}
	h.pgConn = conn

	// Wait for the ingestable's dialect to reconnect — it polls and
	// retries on its own, but a few hundred ms gives it room before
	// the test runs more mutations.
	for _, table := range h.topics {
		h.waitForIngestableReadyContext(ctx, h.slotNames[table], 30*time.Second)
	}
	return nil
}

// waitForIngestableReadyContext is the context-aware counterpart of
// waitForIngestableReady, used by RestartPostgres where we don't have
// a *testing.T at the deepest call site (Close uses bg ctx too).
func (h *Harness) waitForIngestableReadyContext(ctx context.Context, slot string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var active bool
		err := h.pgConn.QueryRow(ctx,
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
