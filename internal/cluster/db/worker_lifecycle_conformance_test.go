package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// syncStuckGauge reads the committed.sync.stuck gauge for a syncable id (its
// attribute key is syncable_id, not id).
func syncStuckGauge(t *testing.T, reader *sdkmetric.ManualReader, id string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		return -1
	}
	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			if met.Name != "committed.sync.stuck" {
				continue
			}
			g, ok := met.Data.(metricdata.Gauge[float64])
			if !ok {
				return -1
			}
			for _, dp := range g.DataPoints {
				for _, a := range dp.Attributes.ToSlice() {
					if string(a.Key) == "syncable_id" && a.Value.AsString() == id {
						return dp.Value
					}
				}
			}
		}
	}
	return -1
}

// Worker-lifecycle conformance: the executable guards for the control-plane
// contracts the round-8 fixes restore. Each asserts a recovery/derived state
// converges on its source of truth across a lifecycle transition. They exist
// because the round-8 regressions all slipped past mechanism-level unit tests;
// these are workflow-level.

// TestSupervisorConformance_RePOSTGrantsFreshBudget pins round-8 #2: an operator
// re-POST (which replaces the worker — a new worker generation) must grant the
// fresh worker its full restart budget, not inherit a prior give-up. The budget
// is pruned on the replace, so even a byte-identical re-POST (which does NOT
// bump the config version) recovers — this is the "raise the cap and re-POST"
// workflow.
func TestSupervisorConformance_RePOSTGrantsFreshBudget(t *testing.T) {
	d, _ := newWalDB(t)
	id := "repost"

	// Install a worker so the second Ingest is a REPLACE (which prunes).
	require.NoError(t, d.Ingest(context.Background(), id, reconcileFakeIngestable{}))
	require.Eventually(t, func() bool { return d.HasIngestWorkerForTest(id) },
		2*time.Second, 5*time.Millisecond)

	// Simulate a give-up run accumulating on this id.
	d.RecordFreezeAndNextBackoffForTest(id, cluster.Position("poison"))
	require.True(t, d.SupervisorStateExistsForTest(id), "a freeze must record supervisor state")

	// Re-POST: the replace prunes the give-up state → the fresh worker's budget
	// starts clean.
	require.NoError(t, d.Ingest(context.Background(), id, reconcileFakeIngestable{}))
	require.False(t, d.SupervisorStateExistsForTest(id),
		"a re-POST (worker replace) must prune the give-up state so the fresh worker gets a full budget")
}

// TestSupervisorConformance_ConfigDeletePrunesGiveupState pins round-8 #2's other
// half: deleting the config drops the supervisor give-up state, so a recreated
// id starts fresh and the state map stays bounded.
func TestSupervisorConformance_ConfigDeletePrunesGiveupState(t *testing.T) {
	d, _ := newIngestFailFastDBWith(t)
	t.Cleanup(func() { _ = d.Close() })

	id := "prune-me"
	d.RecordFreezeAndNextBackoffForTest(id, cluster.Position("p"))
	require.True(t, d.SupervisorStateExistsForTest(id), "a freeze must record supervisor state")

	d.CancelIngestWorkerForTest(id) // the delete / reconcile-absent-cancel path
	require.False(t, d.SupervisorStateExistsForTest(id),
		"a config delete must prune the supervisor give-up state")
}

// TestStuckRecordConformance_StillStuckReplacementKeepsRecord pins round-8 #3: a
// replacement worker that is STILL stuck (adopts the record and re-wedges on the
// same poison) must keep the replicated stuck record CONTINUOUSLY — the record
// is deleted only on genuine progress, not on the worker's startup. Before the
// fix the leadership-gain "starting sync" branch cleared the adopted record, so
// a still-stuck syncable flapped (delete → re-wedge → re-publish after the
// debounce), 409ing the operator's skip/dead-letter lever in between.
func TestStuckRecordConformance_StillStuckReplacementKeepsRecord(t *testing.T) {
	d, s := newWalDBStuck(t)
	id := "still-stuck"
	seedSyncableConfig(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"poison"})

	// Worker A wedges on poison and publishes the replicated stuck record.
	require.NoError(t, d.Sync(context.Background(), id,
		&transientSyncable{stuck: map[string]bool{"poison": true}, transientErr: fmt.Errorf("boom")}))
	require.Eventually(t, func() bool { _, ok, _ := d.SyncableStuck(id); return ok },
		10*time.Second, 10*time.Millisecond, "worker A must publish the stuck record")

	// Replacement B is ALSO stuck on poison: it adopts the record. The record
	// must never disappear (no delete-on-startup flap).
	require.NoError(t, d.Sync(context.Background(), id,
		&transientSyncable{stuck: map[string]bool{"poison": true}, transientErr: fmt.Errorf("boom")}))
	require.Never(t, func() bool { _, ok, _ := d.SyncableStuck(id); return !ok },
		1*time.Second, 20*time.Millisecond,
		"a still-stuck replacement must keep the adopted record continuously (no delete-on-startup flap)")
}

// TestStuckGaugeConformance_DerivedFromAppliedRecord pins round-8 #4: the
// committed.sync.stuck gauge is DERIVED from the applied SyncableStuck record on
// the apply path (handleSyncableStuck) — set when the record applies, cleared
// when the delete applies — not toggled imperatively by the worker. Because
// every node runs the apply path, the gauge converges cluster-wide (a follower
// no longer latches it at 1). This test exercises the derivation single-node
// (publish→apply→1, delete→apply→0); the multi-node convergence follows from the
// derivation being on the apply path every node runs.
func TestStuckGaugeConformance_DerivedFromAppliedRecord(t *testing.T) {
	// Metrics must be wired to the WAL (so the apply path can emit) AND the DB,
	// exactly as cmd does; a reader collects the gauge.
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync(), wal.WithMetrics(m))
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithMetrics(m),
		db.WithSyncStuckThreshold(50*time.Millisecond))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	id := "gauge-derive"
	seedSyncableConfig(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"poison", "ok"})

	// Worker wedges → publishes the record → the apply derives the gauge to 1.
	require.NoError(t, d.Sync(context.Background(), id,
		&transientSyncable{stuck: map[string]bool{"poison": true}, transientErr: fmt.Errorf("boom")}))
	require.Eventually(t, func() bool { return syncStuckGauge(t, reader, id) == 1.0 },
		10*time.Second, 10*time.Millisecond,
		"the stuck gauge must derive to 1 from the applied SyncableStuck record")

	// Healthy replacement clears on progress → the delete applies → gauge to 0.
	require.NoError(t, d.Sync(context.Background(), id, &transientSyncable{stuck: map[string]bool{}}))
	require.Eventually(t, func() bool { return syncStuckGauge(t, reader, id) == 0.0 },
		10*time.Second, 10*time.Millisecond,
		"the stuck gauge must derive to 0 when the record's delete applies")
}
