package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestSnapshot_OpenReconcilesBboltAfterCrashedInstall pins the fix for the
// snapshot-install brick: an in-place InstallSnapshot persists the snapshot
// record and cuts the entry log to the snapshot index (saveWithSnapshot), then
// swaps bbolt and reconciles the applied index in a SEPARATE step
// (RestoreSnapshot). A crash between the two leaves the entry log cut to snapIdx
// while bbolt's applied index lags below it — on restart raft's RestartNode
// panics in appliedTo (a crash-loop brick), or with appliedIndex 0 boots on
// stale bbolt missing the snapshot's metadata.
//
// reconcileEntryLogWithSnapshot already re-runs the entry-log half at Open;
// reconcileBboltWithSnapshot must re-run the bbolt half symmetrically, restoring
// BOTH the applied index AND the snapshot's content (not merely clamping the
// index, which would paper over silent metadata divergence).
func TestSnapshot_OpenReconcilesBboltAfterCrashedInstall(t *testing.T) {
	// Source node: apply two distinct types so its bbolt embeds appliedIndex=2,
	// then snapshot at raft index 2. snap.Data is the source's full bbolt.
	src := NewStorage(t, nil)
	defer src.Cleanup()

	marker, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "marker", Name: "Marker", Version: 1})
	require.NoError(t, err)
	filler, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "filler", Name: "Filler", Version: 1})
	require.NoError(t, err)
	saveEntity(t, marker, src, 1, 1)
	saveEntity(t, filler, src, 1, 2)
	require.Equal(t, uint64(2), src.AppliedIndex())

	snap, err := src.CreateSnapshot(2, &pb.ConfState{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.Metadata.GetIndex())

	// Capture the source bbolt for a content-parity assertion after restore. The
	// source is not mutated after CreateSnapshot, so this is exactly what a
	// correctly-restored node must hold.
	srcBuckets, err := src.BucketSnapshot()
	require.NoError(t, err)

	// Destination node: apply two DIFFERENT types (so its bbolt content diverges
	// from the snapshot's), advancing appliedIndex and eventIndex to 2.
	dst := NewStorage(t, nil)
	defer dst.Cleanup()

	other1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "other1", Name: "Other1", Version: 1})
	require.NoError(t, err)
	other2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "other2", Name: "Other2", Version: 1})
	require.NoError(t, err)
	saveEntity(t, other1, dst, 1, 1)
	saveEntity(t, other2, dst, 1, 2)

	// Put the destination in the pre-crash replay window: appliedIndex rewound to
	// 1 (as a crash between an appendEvent and its saveAppliedIndex would leave
	// it) while eventIndex stays at 2. Now snapIdx=2 is > appliedIndex=1 and
	// <= eventIndex=2 — the exact band saveWithSnapshot's install proceeds in.
	require.NoError(t, dst.SetAppliedIndexForTest(1))
	require.Equal(t, uint64(1), dst.AppliedIndex())
	require.Equal(t, uint64(2), dst.EventIndex())

	// Crash DURING the install: snapshot record persisted + entry log cut to the
	// snapshot index, but bbolt NOT swapped (RestoreSnapshot never ran).
	require.NoError(t, dst.SimulateCrashedSnapshotInstallForTest(snap))
	// bbolt is still the stale pre-install content at the lagging applied index.
	require.Equal(t, uint64(1), dst.AppliedIndex())

	// Restart. Without the fix, Open leaves appliedIndex=1 against an entry log
	// cut to 2 (the brick) and bbolt still holds other1/other2. With the fix,
	// Open re-runs the bbolt swap from the persisted snapshot payload.
	dst = dst.CloseAndReopenStorage(t)
	defer dst.Cleanup()

	require.Equal(t, uint64(2), dst.AppliedIndex(),
		"Open must reconcile the applied index up to the snapshot index; a lagging value bricks raft's RestartNode (appliedTo panic)")
	require.GreaterOrEqual(t, dst.EventIndex(), dst.AppliedIndex(),
		"eventIndex >= appliedIndex invariant must hold after the reconcile")

	// Content parity: the reconcile must restore the snapshot's bbolt CONTENT,
	// not merely clamp the index. The restored node must hold the source's
	// marker/filler types and none of its own pre-install other1/other2 state.
	dstBuckets, err := dst.BucketSnapshot()
	require.NoError(t, err)
	require.Equal(t, srcBuckets, dstBuckets,
		"restored bbolt must byte-match the snapshot source; a clamp-only fix would leave stale content")
}

// TestSnapshot_NormalRestartSkipsBboltReconcile guards against the reconcile
// misfiring on an ordinary restart: a node whose applied index is at or above
// its snapshot index (every completed install and every leader, since
// CreateSnapshot requires index <= appliedIndex) must reopen untouched.
func TestSnapshot_NormalRestartSkipsBboltReconcile(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.NoError(t, err)
	saveEntity(t, tp, s, 1, 1)
	saveEntity(t, tp, s, 1, 2)

	// A snapshot at an index at or below appliedIndex, then a plain restart.
	_, err = s.CreateSnapshot(2, &pb.ConfState{})
	require.NoError(t, err)

	before, err := s.BucketSnapshot()
	require.NoError(t, err)

	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	require.Equal(t, uint64(2), s.AppliedIndex())
	after, err := s.BucketSnapshot()
	require.NoError(t, err)
	require.Equal(t, before, after, "a normal restart must not re-run the bbolt swap")
}
