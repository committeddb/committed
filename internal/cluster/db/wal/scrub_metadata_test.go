package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
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

// userType builds a user-defined type declaration with the given EntityKind.
func userType(t *testing.T, id string, kind cluster.EntityKind) *cluster.Entity {
	t.Helper()
	e, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: id, Name: id, Version: 1, EntityKind: kind,
	})
	require.Nil(t, err)
	return e
}

// userSnapshotType is the EntityKindSnapshot convenience.
func userSnapshotType(t *testing.T, id string) *cluster.Entity {
	t.Helper()
	return userType(t, id, cluster.EntityKindSnapshot)
}

// TestScrub_MetadataKeepsLatestDropsSuperseded is the core metadata-GC test:
// superseded SyncableIndex bumps are removed, keeping the latest per id <= bound
// (plus everything past the freeze line), and the live bbolt value is untouched.
func TestScrub_MetadataKeepsLatestDropsSuperseded(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	require.NoError(t, s.SeedSyncableConfigForTest("A"))
	require.NoError(t, s.SeedSyncableConfigForTest("B"))

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

// TestScrub_UserSnapshotCompacted: a user-defined EntityKindSnapshot stream is
// compacted to the latest value per key, exactly like internal Snapshot
// bookkeeping (the kind is harvested from the type registration). The type
// config itself (EntityKindRevision) is retained.
func TestScrub_UserSnapshotCompacted(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, userSnapshotType(t, "u"), s, 1, 1) // Revision config — retained
	saveEntity(t, userUpsert("u", "k", `{"v":1}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "k", `{"v":2}`), s, 1, 3)
	saveEntity(t, userUpsert("u", "k", `{"v":3}`), s, 1, 4)     // latest "k" <= bound
	saveEntity(t, userUpsert("u", "other", `{"v":9}`), s, 1, 5) // tail (index > bound)

	require.Nil(t, s.RunScrubForTest(4))

	// Superseded "k" snapshots (2, 3) removed; latest "k" (4) and the tail (5)
	// survive; the type registration (1) is retained because typeType is Revision.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 4, 5}, got)
}

// TestScrub_UserNonSnapshotKindsRetained guards the kind gate: only Snapshot is
// system-tombstoned. Event / Standalone / Unspecified user streams are retained
// whole — every entry survives a scrub.
func TestScrub_UserNonSnapshotKindsRetained(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind cluster.EntityKind
	}{
		{"event", cluster.EntityKindEvent},
		{"standalone", cluster.EntityKindStandalone},
		{"unspecified", cluster.EntityKindUnspecified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()

			saveEntity(t, userType(t, "u", tc.kind), s, 1, 1)
			saveEntity(t, userUpsert("u", "k", `{"v":1}`), s, 1, 2)
			saveEntity(t, userUpsert("u", "k", `{"v":2}`), s, 1, 3)
			saveEntity(t, userUpsert("u", "k", `{"v":3}`), s, 1, 4)
			saveEntity(t, userUpsert("u", "tail", `{"v":9}`), s, 1, 5)

			require.Nil(t, s.RunScrubForTest(4))

			got, err := s.EventIndices()
			require.Nil(t, err)
			require.Equal(t, []uint64{1, 2, 3, 4, 5}, got)
		})
	}
}

// TestScrub_VersionedConfigsRetained: the version-stored configs are
// EntityKindRevision, so a scrub retains every version even though they share an
// event-log key — proving they are not keep-latest-compacted like Snapshot.
func TestScrub_VersionedConfigsRetained(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	v1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "u", Name: "u", Version: 1, EntityKind: cluster.EntityKindSnapshot})
	require.Nil(t, err)
	saveEntity(t, v1, s, 1, 1)
	v2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "u", Name: "u", Version: 2, EntityKind: cluster.EntityKindSnapshot})
	require.Nil(t, err)
	saveEntity(t, v2, s, 1, 2)
	saveEntity(t, syncIndex(t, "A", 1), s, 1, 3) // tail

	require.Nil(t, s.RunScrubForTest(2))

	// Both type-config versions (1, 2) survive despite sharing the event-log key
	// "u" — typeType is Revision, so its version history is retained.
	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 2, 3}, got)
}

// TestScrub_SyncableDeadLettersCompact verifies the reshaped (id+index) key
// makes dead letters compact correctly now they are EntityKindSnapshot: each
// record is a distinct key, so distinct dead letters survive, while a re-propose
// or a clear of the same (id, index) collapses to the latest entry.
func TestScrub_SyncableDeadLettersCompact(t *testing.T) {
	dl := func(id string, index uint64) *cluster.Entity {
		e, err := cluster.NewUpsertSyncableDeadLetterEntity(&cluster.SyncableDeadLetter{
			ID: id, Index: index, Kind: "permanent", Message: "boom",
		})
		require.Nil(t, err)
		return e
	}

	t.Run("distinct dead letters survive", func(t *testing.T) {
		s := NewStorageWithParser(t, nil, parser.New())
		defer s.Cleanup()

		// Seed config X first so the dead-letters persist in bbolt (the
		// per-config-id write-guard reaps them otherwise). The config is a
		// retained versioned entity, so it also survives the scrub at index 1.
		seedSyncableConfig(t, s, "X", 1)
		saveEntity(t, dl("X", 100), s, 1, 2)         // key X+100
		saveEntity(t, dl("X", 200), s, 1, 3)         // key X+200 — distinct
		saveEntity(t, syncIndex(t, "A", 1), s, 1, 4) // tail

		// Scrub window covers through index 3 so both dead-letters are in it
		// (the config seed shifted them up by one).
		require.Nil(t, s.RunScrubForTest(3))

		got, err := s.EventIndices()
		require.Nil(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4}, got) // config + both dead-letters + tail survive

		dls, err := s.SyncableDeadLetters("X", 0, 10)
		require.Nil(t, err)
		require.Len(t, dls, 2)
	})

	t.Run("re-propose collapses to latest", func(t *testing.T) {
		s := NewStorageWithParser(t, nil, parser.New())
		defer s.Cleanup()

		seedSyncableConfig(t, s, "X", 1)
		saveEntity(t, dl("X", 100), s, 1, 2)         // key X+100
		saveEntity(t, dl("X", 100), s, 1, 3)         // re-propose, same key X+100
		saveEntity(t, syncIndex(t, "A", 1), s, 1, 4) // tail

		// Scrub window covers through index 3 so the re-propose is in it (the
		// config seed shifted both writes up by one).
		require.Nil(t, s.RunScrubForTest(3))

		got, err := s.EventIndices()
		require.Nil(t, err)
		require.Equal(t, []uint64{1, 3, 4}, got) // config (1) + latest dl (3) kept; superseded upsert (2) dropped

		dls, err := s.SyncableDeadLetters("X", 0, 10)
		require.Nil(t, err)
		require.Len(t, dls, 1) // one record (same identity, overwritten)
	})

	t.Run("clear keeps the delete tombstone", func(t *testing.T) {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		saveEntity(t, dl("X", 100), s, 1, 1)                                        // upsert key X+100
		saveEntity(t, cluster.NewDeleteSyncableDeadLetterEntity("X", 100), s, 1, 2) // delete key X+100
		saveEntity(t, syncIndex(t, "A", 1), s, 1, 3)                                // tail

		require.Nil(t, s.RunScrubForTest(2))

		got, err := s.EventIndices()
		require.Nil(t, err)
		require.Equal(t, []uint64{2, 3}, got) // upsert (1) dropped; delete (2) kept

		// The cleared record is gone from bbolt; the surviving log entry is its
		// faithful final state (the delete).
		dls, err := s.SyncableDeadLetters("X", 0, 10)
		require.Nil(t, err)
		require.Len(t, dls, 0)
	})
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

// TestScrubBacklog_UserSnapshotTriggers verifies user EntityKindSnapshot churn
// also drives the backlog signal, and that the Revision type config does NOT
// count toward it.
func TestScrubBacklog_UserSnapshotTriggers(t *testing.T) {
	restore := wal.SetMetadataBacklogThresholdForTest(3)
	defer restore()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	// The type registration is EntityKindRevision — not counted.
	saveEntity(t, userSnapshotType(t, "u"), s, 1, 1)
	require.False(t, s.HasScrubBacklog())

	// User Snapshot writes count; below threshold, still no backlog.
	saveEntity(t, userUpsert("u", "k", `{"v":1}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "k", `{"v":2}`), s, 1, 3)
	require.False(t, s.HasScrubBacklog())

	// Crossing the threshold triggers it.
	saveEntity(t, userUpsert("u", "k", `{"v":3}`), s, 1, 4)
	require.True(t, s.HasScrubBacklog())
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

// TestScrubDeterminism_UserSnapshot applies identical user-Snapshot churn + a
// scrub to three fresh nodes and requires byte-identical event logs and bbolt —
// the harvest-driven user-kind resolution must be deterministic across replicas.
func TestScrubDeterminism_UserSnapshot(t *testing.T) {
	const nodes = 3
	snaps := make([][]string, nodes)
	buckets := make([][]string, nodes)
	eventIdx := make([]uint64, nodes)
	firstIdx := make([]uint64, nodes)

	for i := 0; i < nodes; i++ {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		saveEntity(t, userSnapshotType(t, "u"), s, 1, 1) // Revision config (retained)
		saveEntity(t, userUpsert("u", "a", `{"v":1}`), s, 1, 2)
		saveEntity(t, userUpsert("u", "b", `{"v":1}`), s, 1, 3)
		saveEntity(t, userUpsert("u", "a", `{"v":2}`), s, 1, 4)
		saveEntity(t, userUpsert("u", "a", `{"v":3}`), s, 1, 5) // latest "a" <= bound
		saveEntity(t, userUpsert("u", "b", `{"v":2}`), s, 1, 6) // latest "b" <= bound
		saveEntity(t, userUpsert("u", "a", `{"v":4}`), s, 1, 7) // tail

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
	// Survivors: type config (1), latest "a" <= bound (5), latest "b" <= bound
	// (6), tail (7).
	require.Equal(t, uint64(7), eventIdx[0], "tail (EventIndex) preserved")
}
