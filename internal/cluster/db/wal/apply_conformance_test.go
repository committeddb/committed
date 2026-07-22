package wal_test

import (
	"io"
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

// TestReaderWatermark_DoesNotSurfaceUnappliedEntry pins reader-sees-unapplied:
// ApplyCommittedBatch publishes a whole Ready's entries to the event log before
// applying them, so a reader can reach a data entry whose type isn't in bbolt
// yet. The reader must EOF at the applied watermark (holding its cursor), not
// surface the entry, fail type resolution, and skip it forever.
func TestReaderWatermark_DoesNotSurfaceUnappliedEntry(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	tp := &cluster.Type{ID: "wm-topic", Name: "wm", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp) // the type entry (M)
	require.NoError(t, err)
	data := cluster.NewUpsertEntity(tp, []byte("k"), []byte("v")) // data of type M (N)
	ents := buildEntries(t, []*cluster.Entity{reg, data}, 1)      // M@1, N@2

	// Publish to the event log WITHOUT applying: type M is durable+visible but
	// not yet in bbolt (the mid-batch window).
	require.NoError(t, s.AppendEventsForTest(ents))

	r := s.Reader("wm-reader")
	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF,
		"the reader must not surface an unapplied entry; it must wait at the watermark")

	// Apply (the batch's appendEvents no-ops via the eventIndex guard; the
	// entities apply, appliedIndex advances).
	require.NoError(t, s.ApplyCommittedBatch(ents))

	// The SAME reader now surfaces N — it was held, not skipped.
	a, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(2), a.Index)
	require.Len(t, a.Entities, 1)
	require.Equal(t, "k", string(a.Entities[0].Key))
}

// TestApplyConformance_OutOfBandStateCrashReplay extends the harness to the
// OUT-OF-BAND applied state produced by raft's conf-change apply (raft.go),
// which runs outside the ApplyCommittedBatch entity path. It pins the
// durability-watermark rule (docs/event-log-architecture.md) for both
// strategies: the ConfState is staged before the batch and written ATOMICALLY
// with the applied index (strategy 2); the peer URL is a keyed put written
// before the index advances and idempotent on replay (strategy 1). Both must
// be durable the instant the index passes the conf entry, and converge under
// crash-replay.
func TestApplyConformance_OutOfBandStateCrashReplay(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	tp := &cluster.Type{ID: "oob", Name: "oob", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)

	cs := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	peerURL := []byte("http://127.0.0.1:2")

	// Conf pre-pass: stage the ConfState and persist the peer URL BEFORE the
	// batch that advances the applied index past the conf entry.
	s.ConfState(cs)
	require.NoError(t, s.PutMemberPeerURL(2, peerURL))

	ents := buildEntries(t, []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k"), []byte("v"))}, 2)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
	require.NoError(t, s.ApplyCommittedBatch(ents))

	// Durable (in confStateBucket), not merely in the in-memory snapshot copy.
	dcs := s.DurableConfStateForTest()
	require.NotNil(t, dcs, "ConfState must be durable after the applied index passed the conf entry")
	require.Equal(t, []uint64{2}, dcs.GetLearners())
	require.Contains(t, s.MemberPeerURLs(), uint64(2))

	// Crash-replay: raft re-delivers the conf, so re-stage (as applyConfChange
	// would) and re-apply. Out-of-band state must CONVERGE — survive, not be
	// lost or duplicated.
	require.NoError(t, s.SetAppliedIndexForTest(1))
	s.ConfState(cs)
	require.NoError(t, s.PutMemberPeerURL(2, peerURL))
	require.NoError(t, s.ApplyCommittedBatch(ents))

	dcs2 := s.DurableConfStateForTest()
	require.NotNil(t, dcs2)
	require.Equal(t, []uint64{2}, dcs2.GetLearners(), "ConfState must survive crash-replay")
	require.Equal(t, []uint64{1}, dcs2.GetVoters())
	urls := s.MemberPeerURLs()
	require.Len(t, urls, 1, "peer URL must be idempotent across replay (no duplicate)")
	require.Contains(t, urls, uint64(2))
}
