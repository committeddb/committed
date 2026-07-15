package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestSnapshot_RestoreResetsMetadataBacklog closes the remaining leg of
// restore-snapshot-scrub-state-not-reloaded for the metadataBacklog field.
// metadataBacklog is a non-durable, leader-local counter of EntityKindSnapshot
// entities applied since the last scrub (a proxy for "superseded metadata is
// waiting to be GC'd", read only by the leader's auto-scrub trigger). A snapshot
// restore adopts a compacted metadata baseline, so a pre-restore count no longer
// maps onto the state; it must reset — exactly as a restart does, where Open
// starts the counter at zero and re-accumulates from post-restart applies.
// (dataEventIndex, the other field the ticket named, needs nothing: it is
// derived from the event log, which RestoreSnapshot does not touch.)
func TestSnapshot_RestoreResetsMetadataBacklog(t *testing.T) {
	restore := wal.SetMetadataBacklogThresholdForTest(3)
	defer restore()

	// src: a minimal snapshot at raft index 1 with no RTBF or metadata backlog.
	src := NewStorage(t, nil)
	defer src.Cleanup()
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, src, 1, 1)
	snap, err := src.CreateSnapshot(1, &pb.ConfState{})
	require.Nil(t, err)

	// dst: accumulate metadata-GC backlog across the threshold so the leader-local
	// auto-scrub trigger fires. EventIndex reaches 3, above snap.Metadata.Index=1,
	// so the restore is allowed.
	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	saveEntity(t, syncIndex(t, "A", 1), dst, 1, 1)
	saveEntity(t, syncIndex(t, "A", 2), dst, 1, 2)
	saveEntity(t, syncIndex(t, "A", 3), dst, 1, 3)
	require.True(t, dst.HasScrubBacklog(), "precondition: metadata backlog accumulated past the threshold")

	require.Nil(t, dst.RestoreSnapshot(snap))

	require.False(t, dst.HasScrubBacklog(),
		"metadataBacklog must reset after a snapshot restore (compacted baseline), as it does across a restart")
}
