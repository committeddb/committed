package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
)

// applyIngestSeq commits one ingest proposal at raft index idx tagged
// with (ingestableID, seq). The entity is an IngestablePosition (a
// self-applying system entity that needs no prior type registration) and
// is just a vehicle — the highwater is advanced from the proposal-level
// IngestableID/SourceSeq, which is what this exercises.
func applyIngestSeq(t *testing.T, s *StorageWrapper, idx uint64, ingestableID string, seq uint64) {
	t.Helper()
	ent, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: "vehicle", Position: []byte("p")})
	require.NoError(t, err)
	p := &cluster.Proposal{
		IngestableID: ingestableID,
		SourceSeq:    seq,
		Entities:     []*cluster.Entity{ent},
	}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := pb.Entry{Term: 1, Index: idx, Type: pb.EntryNormal, Data: bs}
	require.NoError(t, s.Save(defaultHardState, []pb.Entry{entry}, defaultSnap))
	require.NoError(t, s.ApplyCommitted(entry))
}

// TestSourceSeqHighwater_DeterministicAndIdempotent proves the dedup
// highwater is a deterministic function of the committed source seqs —
// the property that makes the per-replica dedup decision safe. Two
// storages fed the same seqs in different order, one with a repeat,
// converge to the same highwater because the advance is a monotonic max.
func TestSourceSeqHighwater_DeterministicAndIdempotent(t *testing.T) {
	const id = "ing"

	s1 := NewStorage(t, nil)
	defer s1.Cleanup()
	for i, seq := range []uint64{1, 2, 3, 4, 5} {
		applyIngestSeq(t, s1, uint64(i+1), id, seq)
	}

	s2 := NewStorage(t, nil)
	defer s2.Cleanup()
	// Different order, and seq 5 applied twice — max() must absorb both.
	for i, seq := range []uint64{3, 1, 5, 2, 5, 4} {
		applyIngestSeq(t, s2, uint64(i+1), id, seq)
	}

	require.Equal(t, uint64(5), s1.IngestSourceSeqHighwater(id))
	require.Equal(t, s1.IngestSourceSeqHighwater(id), s2.IngestSourceSeqHighwater(id),
		"highwater must be a deterministic max over committed source seqs, independent of order/repeats")

	// A never-seen ingestable reports 0 (dedup nothing).
	require.Equal(t, uint64(0), s1.IngestSourceSeqHighwater("other"))
}

// TestSourceSeqHighwater_SurvivesReopen is the durability guarantee the
// whole feature rests on: the highwater must survive a process restart,
// or a restarted node would re-accept re-emitted proposals it already
// committed. Apply some seqs, close, reopen the same dir, assert the
// highwater is still there.
func TestSourceSeqHighwater_SurvivesReopen(t *testing.T) {
	const id = "ing"

	s := NewStorage(t, nil)
	defer s.Cleanup()
	for i, seq := range []uint64{10, 20, 30} {
		applyIngestSeq(t, s, uint64(i+1), id, seq)
	}
	require.Equal(t, uint64(30), s.IngestSourceSeqHighwater(id))

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()

	require.Equal(t, uint64(30), reopened.IngestSourceSeqHighwater(id),
		"highwater must survive a restart so a recovered node still dedups re-emits")
}
