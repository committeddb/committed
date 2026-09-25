package segmentlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// seedErasedHistory builds a synthetic metadata fixture, not a crash-safe import
// API. It represents original ranges whose records have all been erased. The
// completed fixture is opened through the real managed recovery path.
func seedErasedHistory(t testing.TB, count uint64) *Log {
	t.Helper()
	l, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	s := l.catalog.(*boltCatalog)
	for start := uint64(0); start < count; start += 10000 {
		end := min(count, start+10000)
		if err = s.db.Update(func(tx *bolt.Tx) error {
			for i := start; i < end; i++ {
				if e := putBoltRef(tx, SegmentRef{Coverage: Coverage{i, i + 1}}); e != nil {
					return e
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	name, err := uniqueName("tail", count, ".active")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.dir.Install(name, func(w io.Writer) error { return WriteTailHeader(w, count) }); err != nil {
		t.Fatal(err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		h, e := readBoltHeader(tx)
		if e != nil {
			return e
		}
		if e = retireBoltFile(tx, h.Catalog.Active.File, 0); e != nil {
			return e
		}
		h.Catalog.Active = &TailRef{File: name, Start: count}
		h.Catalog.Revision++
		h.Catalog.Generation++
		h.Ranges = count
		return putBoltHeader(tx, h)
	})
	if err != nil {
		t.Fatal(err)
	}
	path := l.path
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLog(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

type noFullCatalog struct{ layout }

func (noFullCatalog) Current() (Catalog, error) { panic("managed operation requested entire catalog") }

func TestBoltLogBeyondFlatCatalogLimit(t *testing.T) {
	const count = 100000
	l := seedErasedHistory(t, count)
	l.catalog = noFullCatalog{l.catalog}
	if id, ok, e := l.LastAppended(); e != nil || !ok || id != count-1 {
		t.Fatal(id, ok, e)
	}
	if e := l.Append([]Record{{count, nil}, {count + 10, []byte("next")}}); e != nil {
		t.Fatal(e)
	}
	if r, e := l.Seek(count - 1); e != nil || r.ID != count {
		t.Fatal(r, e)
	}
	if _, e := l.Rewrite(t.Context(), 2, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != count, nil }); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Reclaim(t.Context()); e != nil {
		t.Fatal(e)
	}
	if _, e := l.ReclaimOrphans(t.Context()); e != nil {
		t.Fatal(e)
	}
	l = reopenBoltLog(t, l)
	if r, e := l.Seek(count - 1); e != nil || r.ID != count+10 || !bytes.Equal(r.Payload, []byte("next")) {
		t.Fatal(r, e)
	}
	if e := l.Verify(t.Context()); e != nil {
		t.Fatal(e)
	}
}

// BenchmarkBoltCatalogScale measures actual managed boundary appends with large
// synthetic erased histories. No payload TBs or millions of physical files are
// created. Use -benchtime=1x; fixture creation is outside the measured interval.
func BenchmarkBoltCatalogScale(b *testing.B) {
	for _, count := range []uint64{50000, 500000, 5000000} {
		b.Run(fmt.Sprintf("ranges=%d", count), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				l := seedErasedHistory(b, count)
				if e := l.Append([]Record{{count, nil}}); e != nil {
					b.Fatal(e)
				}
				before := l.catalog.(*boltCatalog).db.Stats()
				b.StartTimer()
				start := time.Now()
				for i := uint64(1); i <= 20; i++ {
					if e := l.Append([]Record{{count + i, nil}}); e != nil {
						b.Fatal(e)
					}
				}
				elapsed := time.Since(start)
				b.StopTimer()
				after := l.catalog.(*boltCatalog).db.Stats()
				delta := after.Sub(&before)
				b.ReportMetric(float64(elapsed.Nanoseconds())/20/1e6, "append-ms/op")
				b.ReportMetric(float64(delta.TxStats.GetPageAlloc())/20, "metadata-alloc-B/append")
				info, e := os.Stat(l.path + "/" + boltCatalogName)
				if e != nil {
					b.Fatal(e)
				}
				b.ReportMetric(float64(info.Size()), "metadata-file-B")
				path := l.path
				if e = l.Close(); e != nil {
					b.Fatal(e)
				}
				start = time.Now()
				l, e = OpenLog(path, Options{})
				if e != nil {
					b.Fatal(e)
				}
				b.ReportMetric(float64(time.Since(start).Nanoseconds())/1e6, "reopen-ms/op")
				if id, ok, e := l.LastAppended(); e != nil || !ok || id != count+20 {
					b.Fatal(id, ok, e)
				}
				for id := count; id <= count+20; id++ {
					if r, e := l.Read(id); e != nil || r.ID != id {
						b.Fatal(id, r, e)
					}
				}
				if e = l.Close(); e != nil {
					b.Fatal(e)
				}
				b.StartTimer()
			}
		})
	}
}
