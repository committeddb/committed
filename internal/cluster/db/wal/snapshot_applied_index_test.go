package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestSnapshot_RestoreReconcilesAppliedIndexToMetadataIndex pins the fix for the
// latent inconsistency: CreateSnapshot serializes the LIVE bbolt, whose embedded
// appliedIndex is the leader's current applied position — ABOVE the snapshot's
// raft index (Metadata.Index = compactTo). RestoreSnapshot used to adopt that
// embedded value, setting a restored node's appliedIndex above the snapshot's
// raft index and, when eventIndex sits in [Metadata.Index, embedded), tripping
// the eventIndex >= appliedIndex invariant. After the fix appliedIndex must
// reconcile to snap.Metadata.Index (the bbolt is over-complete, so re-applying
// the (Metadata.Index, embedded] tail is idempotent), and the reconciled value
// must survive a restart (persisted to bbolt, not just in memory).
func TestSnapshot_RestoreReconcilesAppliedIndexToMetadataIndex(t *testing.T) {
	src := NewStorage(t, nil)
	defer src.Cleanup()

	// Apply two distinct types at raft indices 1 and 2 so the live bbolt embeds
	// appliedIndex=2.
	tp1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	tp2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events2", Name: "Events2", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp1, src, 1, 1)
	saveEntity(t, tp2, src, 1, 2)
	require.Equal(t, uint64(2), src.AppliedIndex())

	// Snapshot at raft index 1 — BELOW the embedded appliedIndex (2), the
	// CreateSnapshot(compactTo=applied-8) shape where the serialized bbolt is
	// over-complete relative to the snapshot's raft index.
	snap, err := src.CreateSnapshot(1, &pb.ConfState{})
	require.Nil(t, err)
	require.Equal(t, uint64(1), snap.Metadata.GetIndex())

	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	// Put dst.EventIndex at exactly 1 — inside the harmful band [Metadata.Index=1,
	// embedded=2): restore is allowed (Metadata.Index <= EventIndex) and adopting
	// the embedded 2 would violate eventIndex(1) >= appliedIndex.
	saveEntity(t, tp1, dst, 1, 1)
	require.Equal(t, uint64(1), dst.EventIndex())

	require.Nil(t, dst.RestoreSnapshot(snap))

	require.Equal(t, uint64(1), dst.AppliedIndex(),
		"appliedIndex must reconcile to snap.Metadata.Index, not the embedded over-complete value")
	require.GreaterOrEqual(t, dst.EventIndex(), dst.AppliedIndex(),
		"eventIndex >= appliedIndex invariant must hold after restore")

	// Restart-safety: the reconciled value is persisted to bbolt, so a reopen's
	// loadAppliedIndex reads 1, not the embedded 2.
	dst = dst.CloseAndReopenStorage(t)
	defer dst.Cleanup()
	require.Equal(t, uint64(1), dst.AppliedIndex(),
		"reconciled appliedIndex must survive a restart (persisted to bbolt)")
}
