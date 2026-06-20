package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRebuildSyncable_DropsAndReplaysFromZero is the core Phase 3 proof: rebuild
// keeps the config, drops the destination table (owner), resets the checkpoint,
// and replays from index 0 — so the syncable re-observes every prior payload.
func TestRebuildSyncable_DropsAndReplaysFromZero(t *testing.T) {
	dir := t.TempDir()
	const id = "rebuild-sync"
	rec := &teardownRecorder{}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"a", "b"})

	// Wait for the initial sync: both payloads delivered, checkpoint advanced.
	require.Eventually(t, func() bool { return rec.syncedCount() == 2 },
		10*time.Second, 10*time.Millisecond, "both payloads should sync before rebuild")
	require.Eventually(t, func() bool {
		cp, _ := s.GetSyncableIndex(id)
		return cp > 0
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, d.RebuildSyncable(testCtx(t), id))

	// Config is kept.
	require.True(t, hasSyncable(t, s, id), "rebuild must keep the config")

	// The owner dropped the table for a clean slate.
	require.Eventually(t, func() bool { return rec.count() >= 1 },
		10*time.Second, 10*time.Millisecond, "rebuild should drop the table on the owner")

	// Replay from 0 re-delivers the prior payloads: the synced count climbs
	// past the original 2.
	require.Eventually(t, func() bool { return rec.syncedCount() >= 4 },
		10*time.Second, 10*time.Millisecond, "rebuild should replay the log from index 0")

	// And the checkpoint re-advances after the replay.
	require.Eventually(t, func() bool {
		cp, _ := s.GetSyncableIndex(id)
		return cp > 0
	}, 10*time.Second, 10*time.Millisecond, "checkpoint should re-advance after replay")
}

// Rebuild of an unknown syncable surfaces ErrResourceNotFound (the HTTP layer
// maps it to 404).
func TestRebuildSyncable_NotFound(t *testing.T) {
	dir := t.TempDir()
	rec := &teardownRecorder{}
	d, _ := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	err := d.RebuildSyncable(testCtx(t), "nope")
	require.Error(t, err)
}

// Rebuild is re-runnable: a second rebuild is safe (DROP IF EXISTS + CREATE IF
// NOT EXISTS + replay-from-0 is idempotent) and replays again.
func TestRebuildSyncable_ReRunnable(t *testing.T) {
	dir := t.TempDir()
	const id = "rebuild-rerun"
	rec := &teardownRecorder{}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"x"})
	require.Eventually(t, func() bool { return rec.syncedCount() == 1 },
		10*time.Second, 10*time.Millisecond)

	// Run each rebuild to completion (its replay is async) before the next, so
	// each independently re-delivers "x" — the second rebuild proves a syncable
	// that was already rebuilt rebuilds again safely.
	require.NoError(t, d.RebuildSyncable(testCtx(t), id))
	require.Eventually(t, func() bool { return rec.syncedCount() >= 2 },
		10*time.Second, 10*time.Millisecond, "first rebuild replays from 0")

	require.NoError(t, d.RebuildSyncable(testCtx(t), id))
	require.Eventually(t, func() bool { return rec.syncedCount() >= 3 },
		10*time.Second, 10*time.Millisecond, "second rebuild replays from 0 again")

	require.True(t, hasSyncable(t, s, id))
}
