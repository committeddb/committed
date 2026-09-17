package segmentlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type liveChurnMetrics struct {
	append, rewrite, reclaim, reopen time.Duration
	metadataStart, metadataEnd       int64
	allocated                        int64
	free, pending                    int
}

// runLiveChurn uses real managed appends and files, with eight 128-byte records
// per range. It is a file-count/churn fixture, not a large-payload experiment.
func runLiveChurn(t testing.TB, ranges, rounds int, codec Compression) liveChurnMetrics {
	t.Helper()
	const perRange = 8
	opts := LogOptions{SegmentBytes: perRange * (128 + 16), Encoding: Options{Compression: codec}}
	l, err := CreateLog(t.TempDir(), 0, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if l != nil {
			_ = l.Close()
		}
	})
	expected := make(map[uint64][]byte, (ranges+1)*perRange)
	records := make([]Record, 0, (ranges+1)*perRange)
	for i := range (ranges + 1) * perRange {
		id := uint64(i) * 3
		payload := bytes.Repeat([]byte{'a'}, 128)
		binary.LittleEndian.PutUint64(payload, id)
		expected[id] = bytes.Clone(payload)
		records = append(records, Record{id, payload})
	}
	if err = l.Append(records); err != nil {
		t.Fatal(err)
	}
	metadataSize := func() int64 {
		info, e := os.Stat(filepath.Join(l.path, boltCatalogName))
		if e != nil {
			t.Fatal(e)
		}
		return info.Size()
	}
	m := liveChurnMetrics{metadataStart: metadataSize()}
	for round := range rounds {
		before, e := l.InspectCatalog()
		if e != nil || len(before.Segments) != ranges {
			t.Fatal("unexpected live fixture", len(before.Segments), e)
		}
		stats := l.catalog.(*boltCatalog).db.Stats()
		started := time.Now()
		result, e := l.Rewrite(t.Context(), uint64(round+1), func(r Record) ([]byte, bool, error) {
			ordinal := r.ID / 3
			// Revisit the same scattered ranges to exercise metadata page reuse.
			if (ordinal/perRange)%16 != 0 {
				return r.Payload, true, nil
			}
			if ordinal%perRange == 0 {
				delete(expected, r.ID)
				return nil, false, nil
			}
			r.Payload[8]++
			expected[r.ID] = bytes.Clone(r.Payload)
			return r.Payload, true, nil
		})
		m.rewrite += time.Since(started)
		if e != nil || !result.Published || result.ChangedSegments != uint64((ranges+15)/16) {
			t.Fatal(result, e)
		}
		after, e := l.InspectCatalog()
		if e != nil || len(after.Segments) != len(before.Segments) {
			t.Fatal(e)
		}
		for i, previous := range before.Segments {
			current := after.Segments[i]
			if previous.Coverage != current.Coverage || (i%16 != 0 && previous != current) {
				t.Fatal("changed unrelated range", i)
			}
		}
		started = time.Now()
		reclaimed, e := l.Reclaim(t.Context())
		m.reclaim += time.Since(started)
		wantRemoved := result.ChangedSegments
		if result.TailChanged {
			wantRemoved++
		}
		if e != nil || reclaimed.RemovedFiles != wantRemoved {
			t.Fatal("retirement mismatch", reclaimed, wantRemoved, e)
		}
		currentStats := l.catalog.(*boltCatalog).db.Stats()
		delta := currentStats.Sub(&stats)
		m.allocated += delta.TxStats.GetPageAlloc()
		m.free, m.pending = currentStats.FreePageN, currentStats.PendingPageN
		path := l.path
		if e = l.Close(); e != nil {
			t.Fatal(e)
		}
		started = time.Now()
		l, e = OpenLog(path, opts.Encoding)
		m.reopen += time.Since(started)
		if e != nil {
			t.Fatal(e)
		}
		seen := 0
		if e = l.Scan(t.Context(), Coverage{0, ^uint64(0)}, func(r Record) error {
			want, ok := expected[r.ID]
			if !ok || !bytes.Equal(r.Payload, want) {
				return fmt.Errorf("unexpected survivor %d", r.ID)
			}
			seen++
			return nil
		}); e != nil || seen != len(expected) {
			t.Fatal("recovered survivors", seen, len(expected), e)
		}
	}
	// The active range remains full according to original accounting, including
	// erased records. This append must cross a boundary after all churn rounds.
	last := records[len(records)-1].ID
	started := time.Now()
	if err = l.Append([]Record{{last + 3, []byte("after churn")}}); err != nil {
		t.Fatal(err)
	}
	m.append = time.Since(started)
	c, err := l.InspectCatalog()
	if err != nil || len(c.Segments) != ranges+1 {
		t.Fatal("append did not cross boundary", err)
	}
	if id, ok, e := l.LastAppended(); e != nil || !ok || id != last+3 {
		t.Fatal(id, ok, e)
	}
	path := l.path
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLog(path, opts.Encoding)
	if err != nil {
		t.Fatal(err)
	}
	if r, e := l.Read(last + 3); e != nil || !bytes.Equal(r.Payload, []byte("after churn")) {
		t.Fatal("lost boundary append", r, e)
	}
	if err = l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if result, e := l.ReclaimOrphans(t.Context()); e != nil || result.RemovedFiles != 0 {
		t.Fatal("successful lifecycle left orphans", result, e)
	}
	m.metadataEnd = metadataSize()
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLiveSegmentChurn(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) { runLiveChurn(t, 32, 3, codec) })
	}
}

func BenchmarkLiveSegmentChurn(b *testing.B) {
	const rounds = 6
	for _, count := range []int{128, 1024} {
		for _, codec := range []Compression{NoCompression, ZstdDefault} {
			b.Run(fmt.Sprintf("ranges=%d/codec=%d", count, codec), func(b *testing.B) {
				for b.Loop() {
					m := runLiveChurn(b, count, rounds, codec)
					b.ReportMetric(float64(m.append.Nanoseconds())/1e6, "boundary-ms")
					b.ReportMetric(float64(m.rewrite.Nanoseconds())/rounds/1e6, "scrub-ms/round")
					b.ReportMetric(float64(m.reclaim.Nanoseconds())/rounds/1e6, "reclaim-ms/round")
					b.ReportMetric(float64(m.reopen.Nanoseconds())/rounds/1e6, "reopen-ms/round")
					b.ReportMetric(float64(m.metadataStart), "metadata-start-B")
					b.ReportMetric(float64(m.metadataEnd), "metadata-end-B")
					b.ReportMetric(float64(m.allocated)/rounds, "metadata-allocated-B/round")
					b.ReportMetric(float64(m.free), "free-pages")
					b.ReportMetric(float64(m.pending), "pending-pages")
				}
			})
		}
	}
}
