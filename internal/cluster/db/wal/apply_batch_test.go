package wal_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

func batchEntries(t *testing.T, tp *cluster.Type, startIndex uint64, n int) []*pb.Entry {
	t.Helper()
	ents := make([]*pb.Entry, n)
	for i := range ents {
		e := cluster.NewUpsertEntity(tp, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i))
		bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
		require.NoError(t, err)
		term, idx := uint64(1), startIndex+uint64(i)
		ents[i] = &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: bs}
	}
	return ents
}

// TestApplyCommittedBatch_TwoDurableOpsPerReady is the fsync-batching
// regression pin: applying a Ready batch of N entries must cost exactly ONE
// event-log write op and ONE appliedIndex persist — not N of each. Apply
// throughput was previously capped at ~2 fsyncs per entry, serially, on the
// Ready goroutine (the dominant bottleneck for ingest-heavy load and the
// prerequisite for per-row snapshot proposals).
func TestApplyCommittedBatch_TwoDurableOpsPerReady(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	tp := &cluster.Type{ID: "batch-topic", Name: "batch", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)

	const n = 16
	ents := batchEntries(t, tp, 2, n)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))

	preEvents, prePersists := s.ApplyFsyncOpsForTest()
	require.NoError(t, s.ApplyCommittedBatch(ents))
	postEvents, postPersists := s.ApplyFsyncOpsForTest()

	require.Equal(t, int64(1), postEvents-preEvents,
		"a Ready batch must append its event-log records in one batched write")
	require.Equal(t, int64(1), postPersists-prePersists,
		"a Ready batch must persist appliedIndex once, not per entry")
	require.Equal(t, uint64(n+1), s.AppliedIndex())
	require.Equal(t, uint64(n+1), s.EventIndex())
}

// TestApplyCommittedBatch_EquivalentToPerEntry pins semantic equivalence: the
// batch path must produce exactly the state the per-entry path produces —
// same entities readable in order, same indexes — including on a replay
// overlap (already-applied prefix skipped idempotently).
func TestApplyCommittedBatch_EquivalentToPerEntry(t *testing.T) {
	build := func(t *testing.T) (*StorageWrapper, []*pb.Entry, *cluster.Type) {
		s := NewStorageWithParser(t, nil, parser.New())
		tp := &cluster.Type{ID: "batch-topic", Name: "batch", Version: 1}
		reg, err := cluster.NewUpsertTypeEntity(tp)
		require.NoError(t, err)
		saveEntity(t, reg, s, 1, 1)
		ents := batchEntries(t, tp, 2, 8)
		require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
		return s, ents, tp
	}
	readKeys := func(t *testing.T, s *StorageWrapper, typeID string) []string {
		r := s.Reader("equiv")
		var keys []string
		for {
			a, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			for _, e := range a.Entities {
				if e.ID == typeID {
					keys = append(keys, string(e.Key))
				}
			}
		}
		return keys
	}

	sBatch, ents, tp := build(t)
	defer sBatch.Cleanup()
	require.NoError(t, sBatch.ApplyCommittedBatch(ents))
	// Replay overlap: re-applying the same batch must be a no-op.
	require.NoError(t, sBatch.ApplyCommittedBatch(ents))

	sSingle, ents2, _ := build(t)
	defer sSingle.Cleanup()
	for _, e := range ents2 {
		require.NoError(t, sSingle.ApplyCommitted(e))
	}

	require.Equal(t, readKeys(t, sSingle, tp.ID), readKeys(t, sBatch, tp.ID))
	require.Equal(t, sSingle.AppliedIndex(), sBatch.AppliedIndex())
	require.Equal(t, sSingle.EventIndex(), sBatch.EventIndex())
}

// BenchmarkApply measures the apply path with REAL fsyncs, per-entry vs
// batched — the measurement behind the apply-loop-fsync-batching ticket and
// the throughput prerequisite for per-row snapshot proposals.
func BenchmarkApply(b *testing.B) {
	tp := &cluster.Type{ID: "bench-topic", Name: "bench", Version: 1}
	const batch = 64

	makeEnts := func(start uint64) []*pb.Entry {
		ents := make([]*pb.Entry, batch)
		for i := range ents {
			e := cluster.NewUpsertEntity(tp, []byte("k"), make([]byte, 256))
			bs, _ := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
			term, idx := uint64(1), start+uint64(i)
			ents[i] = &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: bs}
		}
		return ents
	}

	for name, batched := range map[string]bool{"per-entry": false, "batched": true} {
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			s, err := wal.Open(dir, parser.New(), nil, nil) // real fsync
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			reg, _ := cluster.NewUpsertTypeEntity(tp)
			bs, _ := (&cluster.Proposal{Entities: []*cluster.Entity{reg}}).Marshal()
			term, idx := uint64(1), uint64(1)
			regEnt := &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: bs}
			if err := s.Save(&defaultHardState, []*pb.Entry{regEnt}, &defaultSnap); err != nil {
				b.Fatal(err)
			}
			if err := s.ApplyCommitted(regEnt); err != nil {
				b.Fatal(err)
			}

			next := uint64(2)
			b.ResetTimer()
			for b.Loop() {
				ents := makeEnts(next)
				next += batch
				b.StopTimer()
				if err := s.Save(&defaultHardState, ents, &defaultSnap); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if batched {
					if err := s.ApplyCommittedBatch(ents); err != nil {
						b.Fatal(err)
					}
				} else {
					for _, e := range ents {
						if err := s.ApplyCommitted(e); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.ReportMetric(float64(batch), "entries/op")
		})
	}
}
