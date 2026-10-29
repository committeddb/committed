package wal

import (
	"fmt"
	"io"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// readerBenchmarkStore shares fixture writes and reader construction; timed
// consumption uses the application's existing db.ActualReader interface.
// The production implementation uses Storage's Reader.
type readerBenchmarkStore interface {
	append([][]byte, uint64) error
	reader(uint64) (db.ActualReader, error)
	close() error
}

type productionReaderBenchmark struct {
	storage *Storage
}

func (s *productionReaderBenchmark) append(records [][]byte, last uint64) error {
	batch := make([]eventlog.Record, 0, len(records))
	for _, raw := range records {
		var entry pb.Entry
		if err := proto.Unmarshal(raw, &entry); err != nil {
			return err
		}
		batch = append(batch, eventlog.Record{ID: entry.GetIndex(), Payload: raw})
	}
	if err := s.storage.eventLog.managed.Append(batch); err != nil {
		return err
	}
	s.storage.appliedIndex.Store(last)
	return nil
}

func (s *productionReaderBenchmark) reader(after uint64) (db.ActualReader, error) {
	return &Reader{s: s.storage, raftIndex: after}, nil
}
func (s *productionReaderBenchmark) close() error { return s.storage.eventLog.Close() }

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
	log, err := segmented.Create(b.TempDir(), 1, segmentlog.LogOptions{SegmentBytes: target, Cache: segmentlog.CacheOptions{RecentBytes: 160 << 20, HistoricalBytes: 160 << 20}})
	if err != nil {
		b.Fatal(err)
	}
	if name == "production" {
		resolver.eventLog = bindEventLog(log)
		return &productionReaderBenchmark{storage: resolver}
	}
	return &segmentedReaderBenchmark{adapter: &eventLogAdapter{log: log}, resolver: resolver}
}

// BenchmarkActualReaderEngines measures identical protobuf histories through the
// production Reader and the experimental adapter's Actual reader, both over
// the segmented engine.
// This fixture bypasses Raft and background services for both implementations.
func BenchmarkActualReaderEngines(b *testing.B) {
	const total = 16384
	for _, name := range []string{"production", "segmented"} {
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
