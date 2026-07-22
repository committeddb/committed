package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

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
