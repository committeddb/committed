//go:build docker

// Package harness wires together a Postgres container, a committed
// child process, per-table ingestables, and proposal stream capture
// into a single per-test fixture. Tests construct a Harness via
// New(t), apply their dataset and mutation script, then call
// Capture(t) to get back the actual proposal stream for the oracle.
package harness

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/dataset"
)

// Harness owns the per-test fixture lifecycle. One Harness per test;
// t.Cleanup tears everything down so the next test starts fresh.
type Harness struct {
	committed *committedProcess
	collector *collector
	topics    []string
	engine    Engine
	ctx       context.Context
	cancel    context.CancelFunc
	// baseline is the per-topic proposal count captured AFTER dataset
	// load and ingestable startup. Capture() trims away everything up
	// to this count so each test only sees its mutation script output.
	baseline map[string]int
}

// Options configures a Harness. Tests that don't touch all 8 TPC-H tables
// can opt in to a subset to keep per-test setup time down (each table = one
// ingestable = one slot).
type Options struct {
	Tables []string // defaults to dataset.Tables (all 8)
	// Syncable, when set, also wires the OUTPUT side: a sink database
	// (the harness's own Postgres) plus one syncable per table that
	// projects topic <table> into a <table>_sink table. Used to exercise
	// the syncable path end-to-end (e.g. restart-resume). Off by default
	// so the ingestable-only tests pay nothing for it.
	Syncable bool
}

// New brings up Postgres + committed + ingestables and returns a ready
// fixture. The dataset is NOT loaded here — call Load() if needed.
// The harness records baseline proposal counts BEFORE you load (since
// pre-load there are zero CDC events), so Capture() correctly subtracts
// both the empty baseline AND the dataset-load events from any
// subsequent mutation capture (if you load via Load() it bumps the
// baseline again after the load completes).
func New(t *testing.T, opts ...Options) *Harness {
	return NewWith(t, PostgresEngine(), opts...)
}

// NewWith is New backed by the given source engine — PostgresEngine() (what New
// uses) or MySQLEngine(). The committed/collector/capture wiring is identical;
// only the source-database specifics differ behind the Engine seam.
func NewWith(t *testing.T, engine Engine, opts ...Options) *Harness {
	t.Helper()

	o := Options{Tables: dataset.Tables}
	if len(opts) > 0 {
		if len(opts[0].Tables) > 0 {
			o.Tables = opts[0].Tables
		}
		o.Syncable = opts[0].Syncable
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &Harness{
		topics: o.Tables,
		engine: engine,
		ctx:    ctx,
		cancel: cancel,
	}

	// 1. Source database (engine-owned: container, connection, and schema —
	// the DDL is applied before committed boots so the publication created later
	// finds the tables it references).
	h.engine.Start(ctx, t)

	// 3. committed.
	h.committed = startCommitted(t)

	// 3b. The collector: an HTTP endpoint the per-topic webhook syncables
	// (registered below) POST every committed Actual to. This is how the
	// harness observes the log — through a syncable, the way a real consumer
	// does — since the log is write-only over HTTP (no GET /proposal).
	h.collector = newCollector()

	// 4. One Type per table.
	for _, table := range o.Tables {
		postType(t, table)
	}

	// 5. One ingestable per table (the engine emits its dialect's config).
	for _, table := range o.Tables {
		h.engine.PostIngestable(t, table)
	}

	// 6. Wait for every ingestable to reach streaming.
	for _, table := range o.Tables {
		h.engine.WaitReady(t, table)
	}

	// 6a. One webhook syncable per topic, POSTing every committed Actual to
	// the collector. A new syncable reads its topic from the start, so the
	// collector accumulates the full stream; the baseline below trims away
	// everything up to the dataset-load point.
	for _, table := range o.Tables {
		postCollectSyncable(t, table, h.collector.urlFor(table))
	}

	// 6b. OUTPUT side (opt-in): a sink database plus one syncable per table
	// that projects topic <table> into <table>_sink. Posted after the
	// ingestables so the topic the syncable reads already has a producer.
	// The syncable's Init runs CREATE TABLE IF NOT EXISTS on apply, so the
	// sink tables exist (empty) once these POSTs return.
	if o.Syncable {
		h.engine.PostSinkDatabase(t)
		for _, table := range o.Tables {
			h.engine.PostSyncable(t, table)
		}
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
	require.NoError(t, h.engine.Load(h.ctx, ds), "load dataset")
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

// Conn returns the pgx connection bound to the test Postgres — a Postgres-only
// escape hatch for the preflight tests and the hand-rolled-transaction scenarios.
func (h *Harness) Conn() *pgx.Conn {
	return h.engine.(*postgresEngine).Conn()
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
		h.waitForCounts(t, expected, captureTimeout(expected))
	}

	out := make(map[string][]CapturedProposal, len(h.topics))
	for _, topic := range h.topics {
		all := h.collector.proposals(topic)
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
		h.baseline[topic] = len(h.collector.proposals(topic))
	}
}

// captureTimeout scales the capture wait with the number of proposals
// expected. Unlike the old direct GET /proposal read (which saw the committed
// log instantly), the harness now observes through the real delivery path:
// every proposal costs a webhook POST plus an fsync-bound SyncableIndex bump,
// and on a single node those fsyncs serialize with the ingestable's own
// writes. High-volume captures (e.g. hot-key churn) therefore need headroom
// proportional to the work; small captures reach their count in well under
// the floor and never feel it.
func captureTimeout(expected map[string]int) time.Duration {
	total := 0
	for _, n := range expected {
		total += n
	}
	return 30*time.Second + time.Duration(total)*250*time.Millisecond
}

// waitForCounts polls each topic until the proposal count reaches
// baseline+expected, or timeout fires.
func (h *Harness) waitForCounts(t *testing.T, expected map[string]int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for topic, n := range expected {
		target := h.baseline[topic] + n
		for {
			got := len(h.collector.proposals(topic))
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
	if h.committed != nil {
		h.committed.Stop()
	}
	if h.collector != nil {
		h.collector.close()
	}
	if h.engine != nil {
		h.engine.Close()
	}
	if h.cancel != nil {
		h.cancel()
	}
}

// ConnString exposes the Postgres connection string. Used by the
// preflight tests that query Postgres directly.
func (h *Harness) ConnString() string {
	return h.engine.ConnString()
}

// SlotName returns the replication slot name for a given table. Used
// by preflight tests and restart-resume verification.
func (h *Harness) SlotName(table string) string {
	return h.engine.SlotName(table)
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
		h.engine.WaitReady(t, table)
	}
}

// RestartPostgres stops and restarts the Postgres container, then
// reconnects the harness's pgx connection. Committed's ingestable
// reconnects on its own (see postgres.go:135 "postgres replication
// stream exited, will reconnect"). Used by TestRestartResume to verify
// slot-resume correctness.
func (h *Harness) RestartPostgres(ctx context.Context) error {
	if err := h.engine.RestartContainer(ctx); err != nil {
		return err
	}
	// Wait for the ingestable's dialect to reconnect — it polls and retries on
	// its own, but re-gating gives it room before the test runs more mutations.
	for _, table := range h.topics {
		h.engine.WaitReadyCtx(ctx, table, 30*time.Second)
	}
	return nil
}
