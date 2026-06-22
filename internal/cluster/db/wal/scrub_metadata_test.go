package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// syncIndex builds a SyncableIndex upsert (an internal, EntityKindSnapshot
// metadata entity) keyed by the syncable id. Repeated writes for the same id
// are last-writer-wins, so superseded ones are what metadata GC compacts.
func syncIndex(t *testing.T, id string, idx uint64) *cluster.Entity {
	t.Helper()
	e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: idx})
	require.Nil(t, err)
	return e
}

// userSnapshotType builds a user-defined type declared EntityKindSnapshot.
// Such a type is LWW too, but this ticket compacts internal metadata only — the
// follow-up (compact-user-snapshot-streams) extends GC to it.
func userSnapshotType(t *testing.T, id string) *cluster.Entity {
	t.Helper()
	e, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: id, Name: id, Version: 1, EntityKind: cluster.EntityKindSnapshot,
	})
	require.Nil(t, err)
	return e
}

// TestScrub_MetadataKeepsLatestDropsSuperseded is the core metadata-GC test:
// superseded SyncableIndex bumps are removed, keeping the latest per id <= bound
// (plus everything past the freeze line), and the live bbolt value is untouched.
func TestScrub_MetadataKeepsLatestDropsSuperseded(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, syncIndex(t, "A", 10), s, 1, 1)
	saveEntity(t, syncIndex(t, "A", 20), s, 1, 2)
	saveEntity(t, syncIndex(t, "A", 30), s, 1, 3) // latest for A <= bound
	saveEntity(t, syncIndex(t, "B", 5), s, 1, 4)  // latest for B <= bound
	saveEntity(t, syncIndex(t, "A", 40), s, 1, 5) // tail (index > bound), always kept

	require.Equal(t, uint64(5), s.EventIndex())

	require.Nil(t, s.RunScrubForTest(4))

	// idx 1, 2 (superseded A bumps) are gone; idx 3 (latest A <= bound), 4
	// (latest B), and 5 (tail) survive — in raft-index order across the gap.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{3, 4, 5}, got)

	// The live (bbolt) value per id is never touched by a scrub — only the log's
	// superseded copies are removed. This is the "no consumer observes a missing
	// latest value" guarantee.
	a, err := s.GetSyncableIndex("A")
	require.Nil(t, err)
	require.Equal(t, uint64(40), a)
	b, err := s.GetSyncableIndex("B")
	require.Nil(t, err)
	require.Equal(t, uint64(5), b)

	// EventIndex (P_local) preserved; firstEventIndex advances to the first
	// survivor.
	require.Equal(t, uint64(5), s.EventIndex(), "EventIndex must not regress")
	require.Equal(t, uint64(3), s.FirstEventIndex())
}

// TestScrub_MetadataDropsSupersededInternalDelete verifies the divergence from
// RTBF: a superseded internal *delete* is removed (RTBF always keeps deletes;
// metadata GC drops one once a later write supersedes it).
func TestScrub_MetadataDropsSupersededInternalDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, syncIndex(t, "A", 10), s, 1, 1)                     // upsert
	saveEntity(t, cluster.NewDeleteSyncableIndexEntity("A"), s, 1, 2) // delete (reset)
	saveEntity(t, syncIndex(t, "A", 99), s, 1, 3)                     // re-created; latest <= bound
	saveEntity(t, syncIndex(t, "A", 100), s, 1, 4)                    // tail (index > bound)

	require.Nil(t, s.RunScrubForTest(3))

	// Both the superseded upsert (1) AND the superseded delete (2) are gone;
	// only the latest <= bound (3) and the tail (4) remain.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{3, 4}, got)
}

// TestScrub_MetadataKeepsLatestWhenItIsADelete verifies that when the latest
// entry for a key <= bound is a delete (the syncable was removed), that delete
// survives as the faithful final state and earlier upserts are dropped.
func TestScrub_MetadataKeepsLatestWhenItIsADelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, syncIndex(t, "A", 10), s, 1, 1)
	saveEntity(t, syncIndex(t, "A", 20), s, 1, 2)
	saveEntity(t, cluster.NewDeleteSyncableIndexEntity("A"), s, 1, 3) // latest for A <= bound
	saveEntity(t, syncIndex(t, "B", 7), s, 1, 4)                      // tail (index > bound)

	require.Nil(t, s.RunScrubForTest(3))

	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{3, 4}, got)

	// The surviving idx-3 entry is the delete tombstone (the latest A state).
	del, err := s.ActualAt(3)
	require.Nil(t, err)
	require.True(t, del.Entities[0].IsDelete())
	require.Equal(t, []byte("A"), del.Entities[0].Key)
}

// TestScrub_UserSnapshotNotSystemTombstoned locks in this ticket's scope: the
// system-tombstone (metadata GC) pass compacts INTERNAL snapshot types only.
// A user-defined EntityKindSnapshot stream is last-writer-wins too, but is NOT
// compacted here — the follow-up (compact-user-snapshot-streams) extends GC to
// it, at which point this expectation changes.
func TestScrub_UserSnapshotNotSystemTombstoned(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, userSnapshotType(t, "u"), s, 1, 1) // user snapshot type
	saveEntity(t, userUpsert("u", "k", `{"v":1}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "k", `{"v":2}`), s, 1, 3)
	saveEntity(t, userUpsert("u", "k", `{"v":3}`), s, 1, 4)
	saveEntity(t, userUpsert("u", "other", `{"v":9}`), s, 1, 5)

	require.Nil(t, s.RunScrubForTest(4))

	// Nothing removed: "u" is user-defined, so not system-tombstonable, and
	// there are no RTBF deletes.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, got)
}

// TestScrub_SyncableDeadLettersNotCompacted guards the asymmetric-key hazard:
// dead letters are an append-style audit log (EntityKindStandalone), with many
// distinct live records per syncable id all sharing the same event-log upsert
// key (the id). A naive keep-latest-per-key compaction would wrongly drop all
// but the newest; they must instead survive intact.
func TestScrub_SyncableDeadLettersNotCompacted(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	dl := func(id string, index uint64) *cluster.Entity {
		e, err := cluster.NewUpsertSyncableDeadLetterEntity(&cluster.SyncableDeadLetter{
			ID: id, Index: index, Kind: "permanent", Message: "boom",
		})
		require.Nil(t, err)
		return e
	}
	// Two distinct dead letters for syncable "X" (proposals 100 and 200), both
	// keyed in the event log by the id "X".
	saveEntity(t, dl("X", 100), s, 1, 1)
	saveEntity(t, dl("X", 200), s, 1, 2)
	saveEntity(t, syncIndex(t, "A", 1), s, 1, 3) // tail (index > bound)

	require.Nil(t, s.RunScrubForTest(2))

	// Nothing dropped — the earlier dead letter (idx 1) is NOT superseded by the
	// later one despite sharing the event-log key.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 2, 3}, got)

	// Both remain queryable.
	dls, err := s.SyncableDeadLetters("X", 0, 10)
	require.Nil(t, err)
	require.Len(t, dls, 2)
}

// TestScrubBacklog_MetadataTriggersAndResets verifies the generalized
// HasScrubBacklog: a metadata-heavy, RTBF-free node reports backlog once enough
// superseded metadata accumulates, and the backlog clears after a scrub.
func TestScrubBacklog_MetadataTriggersAndResets(t *testing.T) {
	restore := wal.SetMetadataBacklogThresholdForTest(3)
	defer restore()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Below threshold (and no RTBF deletes): no backlog.
	saveEntity(t, syncIndex(t, "A", 1), s, 1, 1)
	saveEntity(t, syncIndex(t, "A", 2), s, 1, 2)
	require.False(t, s.HasScrubBacklog())

	// Crossing the threshold: backlog reported.
	saveEntity(t, syncIndex(t, "A", 3), s, 1, 3)
	require.True(t, s.HasScrubBacklog())

	// A scrub clears the metadata backlog (and there is no RTBF backlog).
	require.Nil(t, s.RunScrubForTest(3))
	require.False(t, s.HasScrubBacklog())
	require.Equal(t, uint64(3), s.EventIndex(), "tail preserved")
}

// TestScrubDeterminism_Metadata applies identical metadata churn + a scrub to
// three fresh nodes and requires byte-identical event logs and bbolt — the
// rsync-rebuild / cross-node-hash contract, extended to metadata GC.
func TestScrubDeterminism_Metadata(t *testing.T) {
	const nodes = 3
	snaps := make([][]string, nodes)
	buckets := make([][]string, nodes)
	eventIdx := make([]uint64, nodes)
	firstIdx := make([]uint64, nodes)

	for i := 0; i < nodes; i++ {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		saveEntity(t, syncIndex(t, "A", 10), s, 1, 1)
		saveEntity(t, syncIndex(t, "A", 20), s, 1, 2)
		saveEntity(t, syncIndex(t, "B", 5), s, 1, 3)
		saveEntity(t, cluster.NewDeleteSyncableIndexEntity("A"), s, 1, 4)
		saveEntity(t, syncIndex(t, "A", 99), s, 1, 5)  // latest A <= bound
		saveEntity(t, syncIndex(t, "B", 6), s, 1, 6)   // latest B <= bound
		saveEntity(t, syncIndex(t, "A", 100), s, 1, 7) // tail

		require.Nil(t, s.RunScrubForTest(6))

		var err error
		snaps[i], err = s.EventLogSnapshot()
		require.Nil(t, err)
		buckets[i], err = s.BucketSnapshot()
		require.Nil(t, err)
		eventIdx[i] = s.EventIndex()
		firstIdx[i] = s.FirstEventIndex()
	}

	for i := 1; i < nodes; i++ {
		require.Equalf(t, snaps[0], snaps[i], "event log differs: node %d vs 0", i)
		require.Equalf(t, buckets[0], buckets[i], "bbolt differs: node %d vs 0", i)
		require.Equalf(t, eventIdx[0], eventIdx[i], "EventIndex differs: node %d vs 0", i)
		require.Equalf(t, firstIdx[0], firstIdx[i], "FirstEventIndex differs: node %d vs 0", i)
	}
	// Survivors: latest A <= bound (5), latest B <= bound (6), tail (7).
	require.Equal(t, uint64(7), eventIdx[0], "tail (EventIndex) preserved")
}
