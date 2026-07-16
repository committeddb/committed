//go:build docker

package cdc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// TestRefreshEpochReconcilesStrandedSinkRow_Postgres and _MySQL are the
// end-to-end guards for the refresh-epoch reset fix (A1), one per dialect the
// fix touches. Both drive the exact production path the fix repairs, all the way
// from a source database through committed to an external SQL sink:
//
//  1. three rows are ingested and projected to the sink at generation 1;
//  2. the ingestable is DELETED (its checkpoint — and the epoch inside it — is
//     wiped);
//  3. one row is deleted at the SOURCE while nothing is watching, so no delete
//     event is ever ingested and the sink keeps the now-stale row;
//  4. the ingestable is RECREATED on the same topic.
//
// The recreate's fresh full snapshot re-emits the two live rows at a refresh
// epoch bumped strictly above the generation already on the sink (2 > 1), and
// its closing refresh-boundary marker sweeps every sink row still below that
// epoch — reconciling the source-deleted row the upsert-only enumeration cannot
// re-emit (in the real hazard, an RTBF-erased subject).
//
// Before the fix the recreate reset the epoch to 1, so the marker's
// "committed_generation >= 1 AND committed_generation < 1" sweep was vacuous and
// the stale row survived forever — both tests then fail at WaitForSinkAbsent
// with region 2 still present and SinkCount stuck at 3. (Verified by reverting
// the epoch bump to the pre-A1 max(*epoch, 1).)

func TestRefreshEpochReconcilesStrandedSinkRow_Postgres(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})
	runRefreshEpochReconcile(t, h, refreshReconcileHooks{
		// Postgres: only a genuinely NEW replication slot drives the
		// fresh-snapshot branch that emits a refresh-boundary marker. Recreating
		// while the old slot still exists would resume from LSN 0 with no snapshot
		// and no marker — no sweep would run. Wait for teardown to drop it.
		afterDeleteIngestable: func(t *testing.T, h *harness.Harness) {
			waitForSlotAbsent(t, h, "region", 30*time.Second)
		},
		// Postgres can read the BIGINT generation column via the (text-protocol)
		// pgx connection, so it additionally pins the exact generations: streamed
		// rows start at 1, survivors are re-stamped at the bumped epoch 2.
		afterPhase1: func(t *testing.T, h *harness.Harness) {
			require.Equal(t, int64(1), pgSinkGeneration(t, h, "2"),
				"a row streamed under the first ingestable lands at generation 1")
		},
		afterRecreate: func(t *testing.T, h *harness.Harness) {
			require.Equal(t, int64(2), pgSinkGeneration(t, h, "1"),
				"the recreate re-stamped the surviving row at the bumped refresh epoch")
			require.Equal(t, int64(2), pgSinkGeneration(t, h, "3"),
				"the recreate re-stamped the surviving row at the bumped refresh epoch")
		},
	})
}

func TestRefreshEpochReconcilesStrandedSinkRow_MySQL(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(),
		harness.Options{Tables: []string{"region"}, Syncable: true})
	// MySQL has no replication slot: a recreate with a cleared position always
	// fresh-snapshots and emits the marker (mysql.go: lastPos == nil), so no
	// afterDeleteIngestable gate is needed. Its sink BIGINT generation column
	// can't be read through the string-based SinkValue (binary-protocol int64),
	// so the exact-generation pins are Postgres-only; row absence + count prove
	// the sweep just as decisively.
	runRefreshEpochReconcile(t, h, refreshReconcileHooks{})
}

// refreshReconcileHooks carries the dialect-specific assertions/gates the shared
// driver calls at fixed points. All are optional (nil = skip); MySQL uses none.
type refreshReconcileHooks struct {
	afterDeleteIngestable func(t *testing.T, h *harness.Harness)
	afterPhase1           func(t *testing.T, h *harness.Harness)
	afterRecreate         func(t *testing.T, h *harness.Harness)
}

// runRefreshEpochReconcile is the dialect-agnostic scenario body: the four
// phases above, with the decisive assertions (the stale row is swept, only the
// live rows remain) shared and the dialect-specific pins/gates injected via hooks.
func runRefreshEpochReconcile(t *testing.T, h *harness.Harness, hooks refreshReconcileHooks) {
	t.Helper()
	ctx := context.Background()

	// Phase 1: three rows flow source → ingestable → topic → syncable → sink.
	// They stream in after the (empty) initial snapshot, so they land carrying
	// the initial-snapshot epoch: committed_generation = 1.
	ins := mutation.NewScript()
	ins.Insert("region", regionRow(1, "AMERICA", "keep-1"))
	ins.Insert("region", regionRow(2, "STRANDED", "delete-at-source"))
	ins.Insert("region", regionRow(3, "ASIA", "keep-3"))
	require.NoError(t, h.RunScript(ctx, ins), "phase 1 inserts")

	h.WaitForSinkValue(t, "region", "1", "r_name", "AMERICA", 30*time.Second)
	h.WaitForSinkValue(t, "region", "2", "r_name", "STRANDED", 30*time.Second)
	h.WaitForSinkValue(t, "region", "3", "r_name", "ASIA", 30*time.Second)
	require.Equal(t, 3, h.SinkCount(t, "region"))
	if hooks.afterPhase1 != nil {
		hooks.afterPhase1(t, h)
	}

	// Phase 2: delete the ingestable, then let the dialect settle so a recreate
	// fresh-snapshots (Postgres waits for its slot to drop; MySQL is a no-op).
	h.DeleteIngestable(t, "region")
	if hooks.afterDeleteIngestable != nil {
		hooks.afterDeleteIngestable(t, h)
	}

	// Phase 3: delete region 2 at the SOURCE while no ingestable is watching.
	// Nothing ingests a delete, so the sink still holds the stale row — this is
	// the gap a same-topic recreate must reconcile. Only the PK is load-bearing.
	del := mutation.NewScript()
	del.Delete("region", regionRow(2, "STRANDED", "delete-at-source"))
	require.NoError(t, h.RunScript(ctx, del), "delete region 2 at the source")
	require.Equal(t, 3, h.SinkCount(t, "region"),
		"the sink still holds the stale row until a refresh reconciles it")

	// Phase 4: recreate the ingestable on the same topic. Its fresh snapshot
	// re-emits regions 1 and 3 at the bumped epoch; the closing marker sweeps
	// every sink row still below that epoch, removing the source-deleted region 2.
	h.RecreateIngestable(t, "region")

	// The stale row is reconciled off the sink. WaitForSinkAbsent also gates on
	// the marker having been applied, so by the time it returns the live rows'
	// preceding re-upserts have landed at the bumped generation too.
	h.WaitForSinkAbsent(t, "region", "2", 30*time.Second)
	require.Equal(t, 2, h.SinkCount(t, "region"),
		"only the two live rows remain after the reconciling refresh")
	if hooks.afterRecreate != nil {
		hooks.afterRecreate(t, h)
	}
}

// pgSinkGeneration reads the committed-managed committed_generation column for
// the region_sink row keyed by pk over the Postgres pgx connection. Scanned as
// int64 because the column is BIGINT — the string-based harness.SinkValue cannot
// read it. Postgres-only (h.Conn() is a pgx escape hatch).
func pgSinkGeneration(t *testing.T, h *harness.Harness, pk string) int64 {
	t.Helper()
	var g int64
	err := h.Conn().QueryRow(context.Background(),
		"SELECT committed_generation FROM region_sink WHERE r_regionkey = $1", pk).Scan(&g)
	require.NoError(t, err, "read committed_generation for region_sink row %s", pk)
	return g
}

// waitForSlotAbsent polls until the table's Postgres replication slot no longer
// exists, or fails the test. Gating the recreate on slot absence is what
// guarantees the fresh-snapshot branch runs (see the Postgres test's phase 2).
func waitForSlotAbsent(t *testing.T, h *harness.Harness, table string, timeout time.Duration) {
	t.Helper()
	slot := h.SlotName(table)
	require.NotEmpty(t, slot, "no slot name recorded for topic %q", table)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var exists bool
		err := h.Conn().QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&exists)
		if err == nil && !exists {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("replication slot %q still present after %s; DeleteIngestable teardown did not drop it", slot, timeout)
}
