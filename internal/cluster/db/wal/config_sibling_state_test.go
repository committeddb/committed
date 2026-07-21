package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

func applyStuck(t *testing.T, s *StorageWrapper, idx uint64, id string, index uint64) {
	t.Helper()
	e, err := cluster.NewUpsertSyncableStuckEntity(&cluster.SyncableStuck{ID: id, Index: index})
	require.NoError(t, err)
	saveEntity(t, e, s, 1, idx)
}

func applySkipRequest(t *testing.T, s *StorageWrapper, idx uint64, id string, index uint64) {
	t.Helper()
	e, err := cluster.NewUpsertSyncableSkipRequestEntity(&cluster.SyncableSkipRequest{ID: id, Index: index})
	require.NoError(t, err)
	saveEntity(t, e, s, 1, idx)
}

// TestDeleteSyncable_SweepsSiblingStateSoRecreateStartsClean is the A7a/A7c/A7d
// half-A (sweep) proof: deleting a syncable removes the per-id state that lives
// outside the config sub-bucket and is not carried as its own delete-bundle
// tombstone — the dead-letter sub-bucket, the stuck record, and the skip request —
// so a same-id recreate does not inherit it and silently skip work.
func TestDeleteSyncable_SweepsSiblingStateSoRecreateStartsClean(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	const id = "sync"
	seedSyncableConfig(t, s, id, 1)
	applyDeadLetter(t, s, 2, &cluster.SyncableDeadLetter{ID: id, Index: 5, Kind: "permanent", Message: "boom"})
	applyStuck(t, s, 3, id, 7)
	applySkipRequest(t, s, 4, id, 7)

	// Precondition: all three sibling records exist while the config is present.
	has, err := s.HasSyncableDeadLetter(id, 5)
	require.NoError(t, err)
	require.True(t, has, "dead-letter should be recorded while the config exists")
	_, ok, err := s.SyncableStuck(id)
	require.NoError(t, err)
	require.True(t, ok, "stuck record should exist while the config exists")
	_, ok, err = s.SyncableSkipRequest(id)
	require.NoError(t, err)
	require.True(t, ok, "skip request should exist while the config exists")

	// Delete the syncable (config + checkpoint tombstones). The sweep must drop all
	// three siblings in the same apply.
	saveProposal(t, &cluster.Proposal{Entities: cluster.NewDeleteSyncableEntities(id, false)}, s, 1, 5)

	has, err = s.HasSyncableDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "delete must sweep the dead-letter sub-bucket")
	_, ok, err = s.SyncableStuck(id)
	require.NoError(t, err)
	require.False(t, ok, "delete must sweep the stuck record")
	_, ok, err = s.SyncableSkipRequest(id)
	require.NoError(t, err)
	require.False(t, ok, "delete must sweep the skip request")

	// Recreate the same id: a fresh worker at index 0 must not see the old
	// dead-letter and skip index 5, and the swept state must not resurrect.
	seedSyncableConfig(t, s, id, 6)
	has, err = s.HasSyncableDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "a same-id recreate must start clean (no inherited dead-letters)")

	// And the sweep is durable across a restart.
	reopened := s.CloseAndReopenStorage(t)
	defer reopened.Cleanup()
	has, err = reopened.HasSyncableDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "the swept dead-letter must not resurrect on reopen")
}

// TestSaveSyncableSiblings_GuardReapsPostDeleteWrite is the A7a/A7c/A7d half-B
// (write-guard) proof: a sibling write that commits AFTER the config was deleted —
// a racing in-flight worker, or an old write replayed before a same-id recreate —
// must be dropped and any lingering value reaped, not re-established as an orphan.
// This is the case the sweep alone cannot cover (DeleteSyncable does not drain the
// worker first).
func TestSaveSyncableSiblings_GuardReapsPostDeleteWrite(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	const id = "sync"
	seedSyncableConfig(t, s, id, 1)
	saveProposal(t, &cluster.Proposal{Entities: cluster.NewDeleteSyncableEntities(id, false)}, s, 1, 2)

	// These commit after the delete (higher raft index) with no config present.
	applyDeadLetter(t, s, 3, &cluster.SyncableDeadLetter{ID: id, Index: 9, Kind: "permanent", Message: "late"})
	applyStuck(t, s, 4, id, 9)
	applySkipRequest(t, s, 5, id, 9)

	has, err := s.HasSyncableDeadLetter(id, 9)
	require.NoError(t, err)
	require.False(t, has, "a dead-letter committing after the config delete must be reaped, not stored")
	_, ok, err := s.SyncableStuck(id)
	require.NoError(t, err)
	require.False(t, ok, "a stuck record committing after the config delete must be reaped")
	_, ok, err = s.SyncableSkipRequest(id)
	require.NoError(t, err)
	require.False(t, ok, "a skip request committing after the config delete must be reaped")
}

// TestDeleteIngestable_SweepsSourceSeqButPreservesTopicEpoch is the A7b half-A
// (sweep) proof AND the inverse regression guard: deleting an ingestable resets its
// source-seq highwater (so a same-id recreate's re-emitted CDC proposals are not
// dropped pre-raft) while the per-TOPIC refresh epoch — keyed by type id, the
// deliberate inverse — survives the very same delete.
func TestDeleteIngestable_SweepsSourceSeqButPreservesTopicEpoch(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	const id = "ing"
	tp := &cluster.Type{ID: "orders", Name: "Orders", Version: 1}
	tpe, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, tpe, s, 1, 1)

	seedIngestableConfig(t, s, id, 2)
	applyIngestSeq(t, s, 3, id, 50) // highwater 50
	// Bump the per-topic refresh epoch (a generation-stamped entity on topic orders).
	saveEntity(t, cluster.NewRefreshBoundaryEntity(tp, 4), s, 1, 4)

	require.Equal(t, uint64(50), s.IngestSourceSeqHighwater(id))
	require.Equal(t, uint64(4), s.TopicRefreshEpoch("orders"))

	saveProposal(t, &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities(id)}, s, 1, 5)

	require.Equal(t, uint64(0), s.IngestSourceSeqHighwater(id),
		"delete must sweep the id-keyed source-seq highwater")
	require.Equal(t, uint64(4), s.TopicRefreshEpoch("orders"),
		"the topic-keyed refresh epoch is the inverse and must survive the delete")

	// Recreate + a lower source-seq must now advance from 0, not be dropped.
	seedIngestableConfig(t, s, id, 6)
	applyIngestSeq(t, s, 7, id, 7)
	require.Equal(t, uint64(7), s.IngestSourceSeqHighwater(id),
		"a same-id recreate must track its fresh source from 0")
}

// TestAdvanceIngestSourceSeq_GuardReapsPostDeleteApply is the A7b half-B
// (write-guard) proof: an ingest proposal applied after the config was deleted must
// not re-establish the highwater (which would make a same-id recreate silently drop
// re-emitted proposals).
func TestAdvanceIngestSourceSeq_GuardReapsPostDeleteApply(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	const id = "ing"
	seedIngestableConfig(t, s, id, 1)
	saveProposal(t, &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities(id)}, s, 1, 2)

	applyIngestSeq(t, s, 3, id, 99) // racing post-delete ingest

	require.Equal(t, uint64(0), s.IngestSourceSeqHighwater(id),
		"an ingest proposal applied after the config delete must not re-establish the highwater")
}
