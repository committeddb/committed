package sql

import (
	"testing"

	"github.com/committeddb/committed/internal/cluster"
)

// TestChunkEntitiesByBytes pins the snapshot-batch byte budgeting: chunks stay
// under the budget, order is preserved with no row lost, and a single entity
// larger than the whole budget still gets a chunk of its own (the ingest
// worker freezes on it loudly if it also exceeds the cluster cap).
func TestChunkEntitiesByBytes(t *testing.T) {
	ent := func(size int) *cluster.Entity {
		return &cluster.Entity{Key: []byte("k"), Data: make([]byte, size)}
	}
	est := func(e *cluster.Entity) int { return len(e.Key) + len(e.Data) + chunkEntityOverheadBytes }

	t.Run("splits at the budget preserving order", func(t *testing.T) {
		perEntity := 1 << 20 // 1MiB data → ~4 entities per 4MiB budget
		in := make([]*cluster.Entity, 0, 10)
		for range 10 {
			in = append(in, ent(perEntity))
		}
		chunks := ChunkEntitiesByBytes(in)
		if len(chunks) < 2 {
			t.Fatalf("10 x 1MiB should split under a %d-byte budget, got %d chunk(s)", snapshotProposalMaxBytes, len(chunks))
		}
		var flat []*cluster.Entity
		for _, c := range chunks {
			if len(c) == 0 {
				t.Fatal("empty chunk")
			}
			var sz int
			for _, e := range c {
				sz += est(e)
			}
			if sz > snapshotProposalMaxBytes {
				t.Fatalf("chunk estimate %d exceeds budget %d", sz, snapshotProposalMaxBytes)
			}
			flat = append(flat, c...)
		}
		if len(flat) != len(in) {
			t.Fatalf("chunking lost rows: %d in, %d out", len(in), len(flat))
		}
		for i := range in {
			if flat[i] != in[i] {
				t.Fatalf("order not preserved at %d", i)
			}
		}
	})

	t.Run("oversize entity gets its own chunk", func(t *testing.T) {
		in := []*cluster.Entity{ent(64), ent(snapshotProposalMaxBytes + 1), ent(64)}
		chunks := ChunkEntitiesByBytes(in)
		if len(chunks) != 3 {
			t.Fatalf("want 3 chunks (small / oversize alone / small), got %d", len(chunks))
		}
		if len(chunks[1]) != 1 || chunks[1][0] != in[1] {
			t.Fatalf("the oversize entity must ride alone")
		}
	})

	t.Run("small batch stays one chunk", func(t *testing.T) {
		in := []*cluster.Entity{ent(64), ent(64)}
		if chunks := ChunkEntitiesByBytes(in); len(chunks) != 1 || len(chunks[0]) != 2 {
			t.Fatalf("small batch must stay a single chunk, got %v-shaped result", len(chunks))
		}
	})

	t.Run("empty input yields no chunks", func(t *testing.T) {
		if chunks := ChunkEntitiesByBytes(nil); len(chunks) != 0 {
			t.Fatalf("nil input must yield no chunks, got %d", len(chunks))
		}
	})
}
