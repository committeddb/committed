package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// BenchmarkReaderCatchUp measures the syncable catch-up path: a fresh Reader
// draining an already-persisted log from index 0 — the rate that bounds how
// fast a rebuilt sink, a re-materialization, or a new consumer of an old
// topic converges. Reads have no fsync, so the log is seeded WithoutFsync
// (irrelevant to the measured path) and each iteration re-drains the same
// log through a fresh reader.
func BenchmarkReaderCatchUp(b *testing.B) {
	const total, batch = 4096, 64

	dir := b.TempDir()
	s, err := wal.Open(dir, parser.New(), nil, nil, wal.WithoutFsync())
	require.NoError(b, err)
	defer s.Close()

	tp := &cluster.Type{ID: "bench-topic", Name: "bench", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(b, err)
	next := uint64(1)
	save := func(ents []*cluster.Entity) {
		raft := make([]*pb.Entry, len(ents))
		for i, e := range ents {
			bs, merr := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
			require.NoError(b, merr)
			term, idx := uint64(1), next
			next++
			raft[i] = &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: bs}
		}
		require.NoError(b, s.Save(&defaultHardState, raft, &defaultSnap))
		require.NoError(b, s.ApplyCommittedBatch(raft))
	}
	save([]*cluster.Entity{reg})
	for seeded := 0; seeded < total; seeded += batch {
		ents := make([]*cluster.Entity, batch)
		for i := range ents {
			ents[i] = cluster.NewUpsertEntity(tp, []byte("k"), make([]byte, 256))
		}
		save(ents)
	}

	b.ResetTimer()
	for b.Loop() {
		r := s.Reader("") // no checkpoint: the full-log catch-up
		for range total {
			if _, rerr := r.Read(); rerr != nil {
				b.Fatal(rerr)
			}
		}
	}
	b.ReportMetric(float64(total), "entries/op")
}
