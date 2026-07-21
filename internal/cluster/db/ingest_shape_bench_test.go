package db_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// benchWindowIngestable forwards windows of proposals pushed through next —
// the bench drives it one read-window at a time and awaits each window's
// bundled checkpoint.
type benchWindowIngestable struct {
	next chan []*cluster.Proposal
}

func (bi *benchWindowIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	for {
		select {
		case window := <-bi.next:
			for _, p := range window {
				select {
				case pr <- p:
				case <-ctx.Done():
					return nil
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (bi *benchWindowIngestable) Close() error { return nil }

func (bi *benchWindowIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// BenchmarkIngestShape measures the ingest path with REAL fsyncs for the two
// proposal shapes — the old counterfeit-transaction batch (one N-row
// proposal) vs true per-row transactions through the pipelined worker — and
// the on-disk event-log cost of each. These are the measurements the
// ingest-true-transaction-proposals ticket requires: wall-clock ratio and
// log bytes per row.
func BenchmarkIngestShape(b *testing.B) {
	const rows = 512
	const rowBytes = 256

	run := func(b *testing.B, perRow bool) {
		dir := b.TempDir()
		p := parser.New()
		s, err := wal.Open(dir, p, nil, nil) // real fsync
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		d := db.New(1, db.Peers{1: ""}, s, p, nil, nil)
		defer d.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Single-node leadership + type + ingestable config (the position
		// guard persists checkpoints only for configs that exist).
		typCfg := &cluster.Configuration{ID: "bench", MimeType: "text/toml", Data: []byte("[type]\nname = \"bench\"")}
		for d.ProposeType(ctx, typCfg) != nil {
			time.Sleep(10 * time.Millisecond)
		}
		typ, err := s.ResolveType(cluster.LatestTypeRef("bench"))
		if err != nil {
			b.Fatal(err)
		}
		const id = "bench-ingest"
		cfgEnt, err := cluster.NewUpsertIngestableEntity(&cluster.Configuration{
			ID: id, MimeType: "application/json", Data: []byte("{}"),
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := d.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{cfgEnt}}); err != nil {
			b.Fatal(err)
		}

		ing := &benchWindowIngestable{next: make(chan []*cluster.Proposal)}
		if err := d.Ingest(context.Background(), id, ing); err != nil {
			b.Fatal(err)
		}

		window := 0
		b.ResetTimer()
		for b.Loop() {
			window++
			pos := cluster.Position(fmt.Sprintf("cp-%d", window))
			var proposals []*cluster.Proposal
			if perRow {
				proposals = make([]*cluster.Proposal, rows)
				for i := range proposals {
					proposals[i] = &cluster.Proposal{Entities: []*cluster.Entity{{
						Type: typ,
						Key:  fmt.Appendf(nil, "w%d-k%d", window, i),
						Data: make([]byte, rowBytes),
					}}}
				}
				proposals[rows-1].Position = pos
			} else {
				ents := make([]*cluster.Entity, rows)
				for i := range ents {
					ents[i] = &cluster.Entity{
						Type: typ,
						Key:  fmt.Appendf(nil, "w%d-k%d", window, i),
						Data: make([]byte, rowBytes),
					}
				}
				proposals = []*cluster.Proposal{{Entities: ents, Position: pos}}
			}
			ing.next <- proposals
			deadline := time.Now().Add(2 * time.Minute)
			for string(s.Position(id)) != string(pos) {
				if time.Now().After(deadline) {
					b.Fatalf("window %d checkpoint never applied", window)
				}
				time.Sleep(200 * time.Microsecond)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(rows), "rows/op")
		b.ReportMetric(dirBytes(filepath.Join(dir, "events"))/float64(uint64(b.N)*rows), "logB/row")
	}

	b.Run("batch", func(b *testing.B) { run(b, false) })
	b.Run("per-row", func(b *testing.B) { run(b, true) })
}

func dirBytes(dir string) float64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return float64(total)
}
