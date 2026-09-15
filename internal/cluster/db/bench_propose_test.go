package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// BenchmarkProposeApplyRoundTrip measures the full write path a caller
// experiences: Propose blocks until the entry is raft-committed AND applied
// locally, so ns/op is the end-to-end single-node write latency — raft
// Ready handling, the REAL raft-log fsync (WithoutFsync relaxes only bbolt,
// never the WAL), and apply. The serial variant is the per-caller latency
// floor (fsync-bound); the parallel variant shows what raft's Ready
// coalescing buys concurrent proposers (many proposals amortizing shared
// fsyncs), which is the number capacity planning should use for aggregate
// throughput.
func BenchmarkProposeApplyRoundTrip(b *testing.B) {
	dir := b.TempDir()
	p := parser.New()
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(b, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	defer func() { _ = d.Close(); _ = s.Close() }()

	// The first successful propose doubles as the leader-election barrier.
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = d.ProposeType(ctx, &cluster.Configuration{
			ID: "bench", MimeType: "text/toml", Data: []byte("[type]\nname = \"bench\"\n"),
		})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("no leader within 20s: %v", err)
		}
	}
	tp, err := s.ResolveType(cluster.LatestTypeRef("bench"))
	require.NoError(b, err)

	propose := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return d.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, []byte("k"), make([]byte, 256)),
		}})
	}

	b.Run("serial", func(b *testing.B) {
		for b.Loop() {
			if err := propose(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parallel-8", func(b *testing.B) {
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := propose(); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
