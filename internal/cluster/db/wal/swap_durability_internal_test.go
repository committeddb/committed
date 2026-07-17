package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// syncFile fsyncs a real file (production mode) and errors on a missing one;
// syncDirBestEffort fsyncs a real directory without returning. Both are no-ops
// when fsync is disabled, so a WithoutFsync test suite never touches the fsync
// path.
func TestSwapDurability_SyncHelpers(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "data")
	require.NoError(t, os.WriteFile(f, []byte("hello"), 0o600))

	// Production mode: real fsyncs succeed on real paths.
	prod := &Storage{logger: zap.NewNop(), fsyncDisabled: false}
	require.NoError(t, prod.syncFile(f))
	prod.syncDirBestEffort(dir, "test") // best-effort; must not panic
	// A pre-rename temp sync must surface a real failure so the caller can abort.
	require.Error(t, prod.syncFile(filepath.Join(dir, "does-not-exist")))

	// Disabled mode (WithoutFsync): no-ops even on missing paths — no error, no
	// panic, no I/O.
	off := &Storage{logger: zap.NewNop(), fsyncDisabled: true}
	require.NoError(t, off.syncFile(filepath.Join(dir, "missing")))
	off.syncDirBestEffort(filepath.Join(dir, "missing-dir"), "test")
}

// With fsync ENABLED (production mode, no WithoutFsync), a RestoreSnapshot swap
// runs its new legs — fsync the temp bbolt before the rename, fsync the parent
// directory after — and still produces a live, correct database. Exercises the
// real fsync path the rest of the wal suite (WithoutFsync) skips.
func TestSwapDurability_RestoreSnapshotFsyncEnabled(t *testing.T) {
	src, err := Open(t.TempDir(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.NoError(t, err)

	dst, err := Open(t.TempDir(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	// Quiesce dst's Open-started scrub worker so its bbolt read can't race the
	// restore's close→rename→reopen swap.
	dst.stopScrubWorker()

	require.NoError(t, dst.RestoreSnapshot(snap))
	require.Equal(t, snap.Metadata.GetIndex(), dst.AppliedIndex(),
		"restore must reconcile appliedIndex to the snapshot index after the fsync'd swap")
}

// resetEntryLogToSnapshot cuts the entry log over to a snapshot point; C2 added
// dir-fsyncs (the reset dir + its parent) so the cut-over survives power loss
// without relying on the Open-time reconcile to heal it. The WithoutFsync suite
// skips those fsyncs, so run it with fsync ENABLED and confirm the reset still
// produces a correct, reopenable entry log — exercising the new fsync legs on the
// real path (they are best-effort, so this is a smoke test; the true power-loss
// guarantee is process-level crash injection).
func TestSwapDurability_EntryLogResetFsyncEnabled(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil) // fsync enabled (no WithoutFsync)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	s.stopScrubWorker()

	require.NoError(t, s.resetEntryLogToSnapshot(9, 3))
	require.Equal(t, uint64(9), s.firstIndex.Load(), "reset must set firstIndex to the snapshot index")
	require.Equal(t, uint64(9), s.lastIndex.Load(), "reset must set lastIndex to the snapshot index")

	li, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(9), li, "the cut-over log must be live and reopenable at the snapshot index")
}

// After a committed event-log scrub swap, a recomputeEventBoundsLocked failure is
// NOT survivable — returning it into the survive-and-continue scrub worker would
// leave stale bounds and an un-bumped scrubGen against the swapped-in log. The
// helper must fatal at the swap site instead. Mirrors
// TestReopenEventLogAfterSwap_FatalsWhenReopenFails.
func TestRecomputeEventBoundsAfterSwap_FatalsOnRecomputeFailure(t *testing.T) {
	s := fatalPanicStorage(t)
	// Quiesce the Open-started scrub worker before poking the event-log handle.
	s.stopScrubWorker()

	// Force the recompute to fail by closing the event log out from under it, then
	// assert the swap site fatals (panics, via the WriteThenPanic fatal hook)
	// rather than returning the error.
	require.NoError(t, s.eventLog.Close())
	require.Panics(t, func() { s.recomputeEventBoundsAfterSwapOrFatal("test") })

	// Restore a live handle so t.Cleanup's Close is well-defined (the dir is intact
	// — only the handle was closed).
	s.reopenEventLogAfterSwapOrFatal("restore for cleanup")
}

// A failed close of the LIVE event-log handle BEFORE the scrub rename is not
// survivable: returning it into the survive-and-continue scrub worker would leave
// the dead handle for appendEvent to hit later as a mis-attributed ErrClosed
// crash. closeEventLogBeforeSwapOrFatal must fatal at the true site instead.
func TestCloseEventLogBeforeSwap_FatalsOnCloseFailure(t *testing.T) {
	s := fatalPanicStorage(t)
	s.stopScrubWorker()

	// Pre-close the event log so the helper's close is a double-close, which
	// tidwall/wal reports as ErrClosed — the forced close failure. Assert the swap
	// site fatals (panics, via the WriteThenPanic fatal hook) rather than returning.
	require.NoError(t, s.eventLog.Close())
	require.Panics(t, func() { s.closeEventLogBeforeSwapOrFatal("test") })

	// Restore a live handle so t.Cleanup's Close is well-defined (dir intact).
	s.reopenEventLogAfterSwapOrFatal("restore for cleanup")
}
