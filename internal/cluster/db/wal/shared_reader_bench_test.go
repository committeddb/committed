package wal

import (
	"fmt"
	"io"
	"testing"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// readerBenchmarkStore shares fixture writes and reader construction; timed
// consumption uses the application's existing db.ActualReader interface.
// The legacy implementation uses the production Reader and event framing.
type readerBenchmarkStore interface {
	append([][]byte, uint64) error
	reader(uint64) (db.ActualReader, error)
	close() error
}

type legacyReaderBenchmark struct {
	storage *Storage
	fixture *tidwal.Log // Untimed fixture construction only; Storage owns its lifetime.
	next    uint64
}

func (s *legacyReaderBenchmark) append(records [][]byte, last uint64) error {
	batch := new(tidwal.Batch)
	for _, raw := range records {
		s.next++
		batch.Write(s.next, frame(raw))
	}
	if err := s.fixture.WriteBatch(batch); err != nil {
		return err
	}
	s.storage.appliedIndex.Store(last)
	return nil
}

func (s *legacyReaderBenchmark) reader(after uint64) (db.ActualReader, error) {
	return &Reader{s: s.storage, raftIndex: after}, nil
}
func (s *legacyReaderBenchmark) close() error { return s.storage.eventLog.Close() }

type segmentedReaderBenchmark struct {
	adapter  *eventLogAdapter
	resolver *Storage
}

func (s *segmentedReaderBenchmark) append(records [][]byte, last uint64) error {
	if err := s.adapter.appendRaw(records); err != nil {
		return err
	}
	s.resolver.appliedIndex.Store(last)
	return nil
}

func (s *segmentedReaderBenchmark) reader(after uint64) (db.ActualReader, error) {
	return s.adapter.readerAt(after, s.resolver, s.resolver.AppliedIndex)
}
func (s *segmentedReaderBenchmark) close() error { return s.adapter.log.Close() }

func newReaderBenchmarkStore(b *testing.B, name string) readerBenchmarkStore {
	b.Helper()
	const target = 20 << 20
	resolver := &Storage{}
	typ, err := eventTestType(cluster.TypeRef{ID: "items", Version: 1})
	if err != nil {
		b.Fatal(err)
	}
	resolver.typeCache.Store(cluster.TypeRef{ID: "items", Version: 1}, typeCacheEntry{t: typ})
	if name == "production-tidwall" {
		log, err := tidwal.Open(b.TempDir(), &tidwal.Options{SegmentSize: target, SegmentCacheSize: DefaultEventCacheSegments, SealedSegmentCompression: tidwal.CompressionZstd})
		if err != nil {
			b.Fatal(err)
		}
		resolver.eventLog = bindLegacyEventLog(tidwallbackend.OwnLegacy(log), nil)
		return &legacyReaderBenchmark{storage: resolver, fixture: log}
	}
	log, err := segmented.Create(b.TempDir(), 1, segmentlog.LogOptions{SegmentBytes: target, Cache: segmentlog.CacheOptions{RecentBytes: 160 << 20, HistoricalBytes: 160 << 20}})
	if err != nil {
		b.Fatal(err)
	}
	return &segmentedReaderBenchmark{adapter: &eventLogAdapter{log: log}, resolver: resolver}
}

// BenchmarkActualReaderEngines measures identical protobuf histories through the
// real production tidwall reader and the experimental segmented Actual reader.
// This fixture bypasses Raft and background services for both implementations.
func BenchmarkActualReaderEngines(b *testing.B) {
	const total = 16384
	for _, name := range []string{"production-tidwall", "segmented"} {
		b.Run(name, func(b *testing.B) {
			store := newReaderBenchmarkStore(b, name)
			b.Cleanup(func() {
				if err := store.close(); err != nil {
					b.Error(err)
				}
			})
			for first := 1; first <= total; first += 64 {
				records := make([][]byte, 64)
				for i := range records {
					records[i] = experimentEntry(b, uint64((first+i)*10), pb.EntryNormal, experimentRow("key", string(make([]byte, 4096))))
				}
				if err := store.append(records, uint64((first+63)*10)); err != nil {
					b.Fatal(err)
				}
			}
			for _, work := range []struct {
				name         string
				after, count int
			}{
				{"catch-up", 0, total},
				{"historical-window", 128, 128},
				{"near-head-window", total - 128, 128},
			} {
				b.Run(work.name, func(b *testing.B) {
					consume := func() error {
						reader, err := store.reader(uint64(work.after * 10))
						if err != nil {
							return err
						}
						if closer, ok := reader.(io.Closer); ok {
							defer func() { _ = closer.Close() }()
						}
						for i := 1; i <= work.count; i++ {
							actual, err := reader.Read()
							if err != nil {
								return err
							}
							want := uint64((work.after + i) * 10)
							if actual == nil || actual.Index != want || len(actual.Entities) != 1 {
								return fmt.Errorf("unexpected Actual at index %d", want)
							}
						}
						return nil
					}
					if err := consume(); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.SetBytes(int64(work.count) * 4096)
					b.ResetTimer()
					for b.Loop() {
						if err := consume(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(work.count), "actuals/op")
				})
			}
		})
	}
}
