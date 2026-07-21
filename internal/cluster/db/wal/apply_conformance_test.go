package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// This file is the batch-apply ordering CONFORMANCE HARNESS. ApplyCommittedBatch
// is a pure fsync optimization over a per-entry ApplyCommitted loop: it must
// produce byte-identical applied state, and its widened crash-replay window
// (one Ready batch instead of one entry) must converge to the same state on
// replay. Round 7 found this was NOT true for config version history (a
// separate ticket fixes that and adds its red case here); this harness is the
// permanent guard so a future ordering change can't silently diverge the two
// paths again. See the batch-apply-ordering-conformance ticket and
// docs/event-log-architecture.md § companion rules.

// buildEntries wraps each entity as its own single-entity proposal at ascending
// raft indexes starting at 2 (index 1 is reserved for a type registration the
// callers seed first). One entity per entry keeps the batch/per-entry split
// observable at entry granularity.
func buildEntries(t *testing.T, entities []*cluster.Entity, startIndex uint64) []*pb.Entry {
	t.Helper()
	ents := make([]*pb.Entry, len(entities))
	for i, e := range entities {
		bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
		require.NoError(t, err)
		term, idx := uint64(1), startIndex+uint64(i)
		ents[i] = &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: bs}
	}
	return ents
}

// applyBatchVsPerEntry applies the SAME entries to two fresh storages — one via
// ApplyCommittedBatch (production), one via a per-entry ApplyCommitted loop (the
// pre-batch semantics) — and asserts identical bucket state. seed runs on each
// fresh storage first (e.g. a type registration) at index 1.
func applyBatchVsPerEntry(t *testing.T, seed func(s *StorageWrapper), entities []*cluster.Entity) {
	t.Helper()

	batchS := NewStorageWithParser(t, nil, parser.New())
	defer batchS.Cleanup()
	seed(batchS)
	bEnts := buildEntries(t, entities, 2)
	require.NoError(t, batchS.Save(&defaultHardState, bEnts, &defaultSnap))
	require.NoError(t, batchS.ApplyCommittedBatch(bEnts))
	batchState, err := batchS.BucketSnapshot()
	require.NoError(t, err)

	perEntryS := NewStorageWithParser(t, nil, parser.New())
	defer perEntryS.Cleanup()
	seed(perEntryS)
	pEnts := buildEntries(t, entities, 2)
	require.NoError(t, perEntryS.Save(&defaultHardState, pEnts, &defaultSnap))
	for _, e := range pEnts {
		require.NoError(t, perEntryS.ApplyCommitted(e))
	}
	perEntryState, err := perEntryS.BucketSnapshot()
	require.NoError(t, err)

	require.Equal(t, perEntryState, batchState,
		"ApplyCommittedBatch must produce byte-identical state to a per-entry ApplyCommitted loop")
}

// applyBatchCrashReplayConverges applies the entries via ApplyCommittedBatch,
// then for EACH intermediate applied index simulates a crash in the tolerated
// p>r window (bbolt applied through the batch, appliedIndex persisted behind)
// by rewinding appliedIndex and re-applying the batch — asserting the state
// converges to the no-crash state at every rewind. This is the whole-batch
// replay-idempotency the widened window requires.
func applyBatchCrashReplayConverges(t *testing.T, seed func(s *StorageWrapper), entities []*cluster.Entity) {
	t.Helper()

	base := NewStorageWithParser(t, nil, parser.New())
	defer base.Cleanup()
	seed(base)
	ents := buildEntries(t, entities, 2)
	require.NoError(t, base.Save(&defaultHardState, ents, &defaultSnap))
	require.NoError(t, base.ApplyCommittedBatch(ents))
	want, err := base.BucketSnapshot()
	require.NoError(t, err)

	// Rewind appliedIndex to each intermediate value and replay the batch on
	// top of the already-applied bbolt state; every rewind must converge to
	// `want`.
	for _, e := range ents {
		require.NoError(t, base.SetAppliedIndexForTest(e.GetIndex()-1))
		require.NoError(t, base.ApplyCommittedBatch(ents))
		got, err := base.BucketSnapshot()
		require.NoError(t, err)
		require.Equal(t, want, got,
			"replaying the batch after a crash-window rewind to applied<%d must converge, not diverge", e.GetIndex())
	}
}

// distinctKeyWorkload is the equivalence baseline: distinct-key upserts + a
// delete + an ingest position bump — all idempotent side effects (last-writer-
// wins puts, monotonic source-seq, config-guarded position). Both the
// equivalence and the crash-replay harness must hold for it TODAY; it proves
// the harness works before the config-version ticket adds its red case.
func distinctKeyWorkload(t *testing.T) (func(s *StorageWrapper), []*cluster.Entity) {
	tp := &cluster.Type{ID: "conf-topic", Name: "conf", Version: 1}
	seed := func(s *StorageWrapper) {
		reg, err := cluster.NewUpsertTypeEntity(tp)
		require.NoError(t, err)
		saveEntity(t, reg, s, 1, 1)
	}
	entities := []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k1"), []byte("v1")),
		cluster.NewUpsertEntity(tp, []byte("k2"), []byte("v2")),
		cluster.NewUpsertEntity(tp, []byte("k3"), []byte("v3")),
		cluster.NewDeleteEntity(tp, []byte("k2")),
	}
	return seed, entities
}

func TestApplyConformance_BatchEqualsPerEntry(t *testing.T) {
	seed, entities := distinctKeyWorkload(t)
	applyBatchVsPerEntry(t, seed, entities)
}

func TestApplyConformance_CrashReplayConverges(t *testing.T) {
	seed, entities := distinctKeyWorkload(t)
	applyBatchCrashReplayConverges(t, seed, entities)
}

// sameIDConfigWorkload is the config-version-replay red case: two upserts of
// the SAME type id (v1 then v2) in one batch. On a crash-window replay the
// last+1 version allocator used to append phantom versions ({v1,v2,v1,v2}),
// diverging version history across replicas. The raft-index replay guard makes
// the whole-batch replay idempotent, so the crash-replay harness converges.
func sameIDConfigWorkload(t *testing.T) (func(s *StorageWrapper), []*cluster.Entity) {
	seed := func(s *StorageWrapper) {} // no pre-seed; the entities register the type
	v1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "revved", Name: "Revved", Version: 1})
	require.NoError(t, err)
	v2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "revved", Name: "Revved", Version: 2, Schema: []byte("{}"), SchemaType: "JSONSchema"})
	require.NoError(t, err)
	return seed, []*cluster.Entity{v1, v2}
}

// TestApplyConformance_SameIDConfigCrashReplay is the red-proof: replaying a
// batch of two same-id type versions after a crash-window rewind must converge
// to exactly two versions, not append phantoms. Fails on the pre-guard
// last+1 allocator.
func TestApplyConformance_SameIDConfigCrashReplay(t *testing.T) {
	seed, entities := sameIDConfigWorkload(t)
	applyBatchCrashReplayConverges(t, seed, entities)
}

// TestApplyConformance_SameIDConfigVersionCount pins the exact history: after
// applying and crash-replaying the two-version batch, the type has exactly two
// versions (1 and 2), current is 2 — no phantoms.
func TestApplyConformance_SameIDConfigVersionCount(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	_, entities := sameIDConfigWorkload(t)
	ents := buildEntries(t, entities, 2)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
	require.NoError(t, s.ApplyCommittedBatch(ents))
	// Crash-window replay of the whole batch.
	require.NoError(t, s.SetAppliedIndexForTest(2))
	require.NoError(t, s.ApplyCommittedBatch(ents))

	versions, err := s.TypeVersions("revved")
	require.NoError(t, err)
	require.Len(t, versions, 2, "replay must not append phantom versions")
	require.Equal(t, uint64(2), versions[len(versions)-1].Version)
}
