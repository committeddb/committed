package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type workloadCompressor struct {
	eventlog.SealedCompressor
	completed atomic.Uint64
	failed    atomic.Uint64
}

func (c *workloadCompressor) CompressNextSealed() (bool, error) {
	did, err := c.SealedCompressor.CompressNextSealed()
	if err != nil {
		c.failed.Add(1)
	}
	if did && err == nil {
		c.completed.Add(1)
	}
	return did, err
}

func streamingWorkloadStore(t testing.TB, backend string, extra ...Option) (*Storage, *workloadCompressor) {
	t.Helper()
	options := []Option{WithSealerIdleInterval(time.Millisecond)}
	if backend == "segmented" {
		options = append(options, WithSegmentedEventLog(segmentlog.LogOptions{
			Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault},
			Cache:    segmentlog.CacheOptions{RecentBytes: 160 << 20, HistoricalBytes: 160 << 20},
		}))
	}
	s, err := Open(t.TempDir(), nil, nil, nil, append(options, extra...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	s.eventMu.Lock()
	compressor := &workloadCompressor{SealedCompressor: s.eventLog.compressor}
	s.eventLog.compressor = compressor
	s.eventMu.Unlock()
	return s, compressor
}

func applyWorkloadBatch(s *Storage, entries []*pb.Entry) error {
	if err := s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: entries[len(entries)-1].Index}, entries, &pb.Snapshot{}); err != nil {
		return err
	}
	return s.ApplyCommittedBatch(entries)
}

type streamingMeasurement struct {
	write         time.Duration
	batches       []time.Duration
	live, catchup time.Duration // slowest completion from the common start
	actuals       uint64
}

type streamingReaderResult struct {
	elapsed time.Duration
	catchup bool
	count   uint64
	err     error
}

// runStreamingWorkload starts all readers before writing the suffix. Readers
// independently follow the applied watermark; the writer never waits for them.
// prefix and catchupStarts are slice positions, including registration at zero.
// Omitted historical starts default to the first user record for every reader.
func runStreamingWorkload(ctx context.Context, s *Storage, entries []*pb.Entry, expected []*clusterpb.LogRow, prefix, liveReaders, catchupReaders int, catchupStarts ...int) (result streamingMeasurement, err error) {
	if len(catchupStarts) != 0 && len(catchupStarts) != catchupReaders {
		return result, errors.New("historical starting positions must match reader count")
	}
	for _, first := range catchupStarts {
		if first < 1 || first > prefix {
			return result, errors.New("historical starting position outside preloaded history")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	results := make(chan streamingReaderResult, liveReaders+catchupReaders)
	ready := make(chan struct{}, liveReaders+catchupReaders)
	start := make(chan struct{})
	var started time.Time
	for i := range liveReaders + catchupReaders {
		catchup := i < catchupReaders
		first := prefix
		if catchup {
			first = 1
			if len(catchupStarts) != 0 {
				first = catchupStarts[i]
			}
		}
		reader := s.ReaderAt(entries[first-1].GetIndex())
		workers.Go(func() {
			outcome := streamingReaderResult{catchup: catchup}
			defer func() {
				outcome.err = errors.Join(outcome.err, reader.(io.Closer).Close())
				outcome.elapsed = time.Since(started)
				results <- outcome
			}()
			ready <- struct{}{}
			<-start
			for next := first; next < len(entries); {
				if outcome.err = ctx.Err(); outcome.err != nil {
					return
				}
				actual, err := reader.Read()
				if errors.Is(err, io.EOF) {
					select {
					case <-ctx.Done():
					case <-time.After(time.Millisecond):
					}
					continue
				}
				if err != nil {
					outcome.err = err
					return
				}
				want := expected[next]
				if actual == nil || actual.Index != entries[next].GetIndex() || len(actual.Entities) != 1 ||
					!bytes.Equal(actual.Entities[0].Key, want.Key) || !bytes.Equal(actual.Entities[0].Data, want.Data) {
					outcome.err = fmt.Errorf("reader expected exact record at index %d", entries[next].GetIndex())
					return
				}
				outcome.count++
				next++
			}
		})
	}
	for range liveReaders + catchupReaders {
		<-ready
	}
	started = time.Now()
	close(start)
	for first := prefix; first < len(entries); first += 256 {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		before := time.Now()
		if err := applyWorkloadBatch(s, entries[first:min(first+256, len(entries))]); err != nil {
			return result, err
		}
		result.batches = append(result.batches, time.Since(before))
	}
	result.write = time.Since(started)
	for range liveReaders + catchupReaders {
		outcome := <-results
		if outcome.err != nil {
			return result, outcome.err
		}
		result.actuals += outcome.count
		if outcome.catchup {
			result.catchup = max(result.catchup, outcome.elapsed)
		} else {
			result.live = max(result.live, outcome.elapsed)
		}
	}
	return result, nil
}

func streamingExpectedRows(t testing.TB, entries []*pb.Entry) []*clusterpb.LogRow {
	t.Helper()
	expected := make([]*clusterpb.LogRow, len(entries))
	for i := 1; i < len(entries); i++ {
		var proposal clusterpb.LogProposal
		require.NoError(t, proto.Unmarshal(entries[i].Data, &proposal))
		expected[i] = proposal.LogEntities[0].GetRow()
	}
	return expected
}

func TestStreamingWorkload(t *testing.T) {
	entries, _, _ := workloadEntries(t, 1024)
	expected := streamingExpectedRows(t, entries)
	for _, backend := range []string{"tidwall", "segmented"} {
		for _, scenario := range []struct {
			name    string
			starts  []int
			actuals uint64
		}{
			{"same-start", []int{1, 1}, 8*512 + 2*1024},
			{"staggered", []int{1, 257}, 8*512 + 1024 + 768},
		} {
			t.Run(backend+"/"+scenario.name, func(t *testing.T) {
				s, _ := streamingWorkloadStore(t, backend)
				require.NoError(t, applyWorkloadBatch(s, entries[:513]))
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				result, err := runStreamingWorkload(ctx, s, entries, expected, 513, 8, 2, scenario.starts...)
				require.NoError(t, err)
				require.Equal(t, scenario.actuals, result.actuals)
				require.Len(t, result.batches, 2)
			})
		}
	}
}

// BenchmarkStreamingWorkload measures durable writes with 128 live readers,
// four historical readers and the production background compression worker.
// Run with -benchtime=1x. Setup/preloading and shutdown are excluded.
func BenchmarkStreamingWorkload(b *testing.B) {
	benchmarkStreamingWorkload(b, 16384, 8193, false)
}

// BenchmarkStreamingCachePressure compares default caches with budgets smaller
// than the history, using historical readers at different starting positions.
func BenchmarkStreamingCachePressure(b *testing.B) {
	for _, cache := range []struct {
		name        string
		constrained bool
	}{{"default", false}, {"constrained", true}} {
		b.Run(cache.name, func(b *testing.B) {
			benchmarkStreamingWorkload(b, 32768, 24577, cache.constrained, 1, 8193, 16385, 20481)
		})
	}
}

func benchmarkStreamingWorkload(b *testing.B, records, prefix int, constrained bool, catchupStarts ...int) {
	entries, _, _ := workloadEntries(b, records)
	expected := streamingExpectedRows(b, entries)
	if len(catchupStarts) == 0 {
		catchupStarts = []int{1, 1, 1, 1}
	}
	wantActuals := 128 * (len(entries) - prefix)
	for _, first := range catchupStarts {
		wantActuals += len(entries) - first
	}
	for _, backend := range []string{"tidwall", "segmented"} {
		b.Run(backend, func(b *testing.B) {
			totals := map[string]float64{}
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				var options []Option
				if constrained {
					if backend == "tidwall" {
						options = append(options, WithEventCacheSegments(2))
					} else {
						options = append(options, WithSegmentedEventLog(segmentlog.LogOptions{
							Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault},
							Cache:    segmentlog.CacheOptions{RecentBytes: 32 << 20, HistoricalBytes: 32 << 20},
						}))
					}
				}
				s, compressor := streamingWorkloadStore(b, backend, options...)
				for first := 0; first < prefix; first += 256 {
					require.NoError(b, applyWorkloadBatch(s, entries[first:min(first+256, prefix)]))
				}
				before := compressor.completed.Load()
				ctx, cancel := context.WithTimeout(b.Context(), 3*time.Minute)
				b.StartTimer()
				result, err := runStreamingWorkload(ctx, s, entries, expected, prefix, 128, 4, catchupStarts...)
				b.StopTimer()
				cancel()
				require.NoError(b, err)
				require.Equal(b, uint64(wantActuals), result.actuals)
				completed := compressor.completed.Load() - before
				require.Positive(b, completed, "workload must overlap background compression")
				totals["compressed-segments"] += float64(completed)
				require.NoError(b, s.Close())
				require.Zero(b, compressor.failed.Load(), "background compression failed")
				slices.Sort(result.batches)
				for name, i := range map[string]int{"p50": (len(result.batches)*50 - 1) / 100, "p95": (len(result.batches)*95 - 1) / 100, "max": len(result.batches) - 1} {
					totals["batch-"+name+"-ms"] += float64(result.batches[i]) / float64(time.Millisecond)
				}
				totals["write-completion-ms"] += float64(result.write) / float64(time.Millisecond)
				totals["live-completion-ms"] += float64(result.live) / float64(time.Millisecond)
				totals["catchup-completion-ms"] += float64(result.catchup) / float64(time.Millisecond)
				totals["actuals"] += float64(result.actuals)
				b.StartTimer()
			}
			for unit, total := range totals {
				b.ReportMetric(total/float64(b.N), unit)
			}
		})
	}
}
