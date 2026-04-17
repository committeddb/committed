package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TestSnapshot_RoundTrip captures a metadata snapshot after applying a
// few entities, then restores that snapshot onto a second fresh
// storage and verifies the entities are queryable. This is the basic
// Phase-3 contract: Snapshot.Data carries enough state to reconstitute
// a node's metadata tier.
func TestSnapshot_RoundTrip(t *testing.T) {
	src := NewStorage(t, nil)
	defer src.Cleanup()

	// Apply a Type so there's bucket content to capture.
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, src, 1, 1)

	// Snapshot at the applied index; ConfState empty is fine for the
	// unit test — raft won't use it here.
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.Nil(t, err)
	require.Equal(t, uint64(1), snap.Metadata.Index)
	require.Greater(t, len(snap.Data), 0, "snapshot must have bbolt payload")

	// Build a second storage with no state. Mirror enough of src's
	// event log into dst so RestoreSnapshot's invariant guard is
	// satisfied (snap.Metadata.Index <= dst.EventIndex).
	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	saveEntity(t, tp, dst, 1, 1)

	require.Nil(t, dst.RestoreSnapshot(snap))

	// The restored storage should have the Type visible via the normal
	// query API.
	got, err := dst.ResolveType(cluster.LatestTypeRef("events"))
	require.Nil(t, err)
	require.Equal(t, "events", got.ID)
	require.Equal(t, "Events", got.Name)
	require.Equal(t, 1, got.Version)

	// appliedIndex must have been lifted to match the snapshot so the
	// Ready loop's replay-skip guard does the right thing going
	// forward.
	require.Equal(t, uint64(1), dst.AppliedIndex(), "AppliedIndex restored from snapshot")
}

// TestSnapshot_RestoreRejectsWhenEventLogBehind verifies the severely-
// behind safety path: if a snapshot's metadata index is past the
// local event log's high watermark, RestoreSnapshot returns an error
// rather than overwriting bbolt. The Ready loop's invariant check is
// the backstop, but catching this early keeps bbolt intact so the
// operator can rebuild cleanly.
func TestSnapshot_RestoreRejectsWhenEventLogBehind(t *testing.T) {
	// Source node applies two entities (event log and applied index
	// both at 2).
	src := NewStorage(t, nil)
	defer src.Cleanup()
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, src, 1, 1)
	saveEntity(t, tp, src, 1, 2)

	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.Nil(t, err)
	require.Equal(t, uint64(2), snap.Metadata.Index)

	// Destination node has no events applied yet — its EventIndex is 0.
	// Restoring a snap at index 2 must fail.
	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	err = dst.RestoreSnapshot(snap)
	require.NotNil(t, err, "RestoreSnapshot must refuse when event log is behind")
}
