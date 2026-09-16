package eventlog_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// Identical logical history and batch sizes are used across implementations.
// Physical segment boundaries can differ because framing and rotation differ.
func benchmarkHistory(b *testing.B, backend backend, count int) (string, eventlog.EventLog) {
	b.Helper()
	path := b.TempDir()
	log, err := backend.create(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = log.Close() })
	rng := rand.New(rand.NewPCG(7, 19))
	records := make([]eventlog.Record, 0, 31)
	for i := range count {
		data := make([]byte, 1024)
		for j := range data {
			data[j] = "abcdefghijklmnopqrstuvwxyz0123456789"[rng.IntN(36)]
		}
		records = append(records, eventlog.Record{ID: uint64(i * 10), Payload: data})
		if len(records) == cap(records) || i == count-1 {
			if err := log.Append(records); err != nil {
				b.Fatal(err)
			}
			records = records[:0]
		}
	}
	if _, err := log.Reclaim(b.Context()); err != nil {
		b.Fatal(err)
	}
	return path, log
}

// BenchmarkEventLogHistory measures local warm-filesystem behavior. Open includes
// verification and sync; seek excludes initial construction/open. Each append
// sample starts with an independently constructed history so history growth does
// not change its workload during benchmark calibration. Fixture work is untimed.
func BenchmarkEventLogHistory(b *testing.B) {
	for _, backend := range backendsWithSegmentBytes(32 << 10) {
		b.Run(backend.name, func(b *testing.B) {
			for _, count := range []int{31 * 4, 31 * 32} {
				b.Run(fmt.Sprintf("records=%d", count), func(b *testing.B) {
					b.Run("reopen", func(b *testing.B) {
						path, log := benchmarkHistory(b, backend, count)
						if err := log.Close(); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							reopened, err := backend.open(path)
							if err != nil {
								b.Fatal(err)
							}
							if err := reopened.Close(); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(count*1024), "history-B")
					})
					b.Run("seek-middle", func(b *testing.B) {
						_, log := benchmarkHistory(b, backend, count)
						id := uint64((count / 2) * 10)
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							record, err := log.Seek(id)
							if err != nil || record.ID != id || len(record.Payload) != 1024 {
								b.Fatalf("seek %d: %v", id, err)
							}
						}
						b.ReportMetric(float64(count*1024), "history-B")
					})
					b.Run("append", func(b *testing.B) {
						record := eventlog.Record{ID: uint64(count * 10), Payload: make([]byte, 1024)}
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							b.StopTimer()
							_, log := benchmarkHistory(b, backend, count)
							b.StartTimer()
							err := log.Append([]eventlog.Record{record})
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							head, has, err := log.LastAppended()
							if err != nil || !has || head != record.ID {
								b.Fatal(head, has, err)
							}
							if err := log.Close(); err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						b.ReportMetric(float64(count*1024), "history-B")
					})
				})
			}
		})
	}
}
