package eventlog_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// BenchmarkEventLogScale measures each phase of a complete on-disk lifecycle.
// Use -benchtime=1x: every iteration builds a fresh history outside the timer.
func BenchmarkEventLogScale(b *testing.B) {
	const payloadSize = 4096
	const batchSize = 255 // 255 segmented frames nearly fill a 1 MiB segment.
	for _, backend := range backendsWithSegmentBytes(1 << 20) {
		b.Run(backend.name, func(b *testing.B) {
			for _, batches := range []int{8, 64} {
				b.Run(fmt.Sprintf("batches=%d", batches), func(b *testing.B) {
					count := batches * batchSize
					durations := make(map[string]time.Duration)
					var removedBytes uint64
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						path := b.TempDir()
						log, err := backend.create(path)
						if err != nil {
							b.Fatal(err)
						}
						b.Cleanup(func() {
							if log != nil {
								_ = log.Close()
							}
						})
						rng := rand.New(rand.NewPCG(7, 19))
						batch := make([]eventlog.Record, batchSize)
						buildStart := time.Now()
						for batchIndex := range batches {
							for i := range batch {
								payload := make([]byte, payloadSize)
								for j := range payload {
									payload[j] = "abcdefghijklmnopqrstuvwxyz0123456789"[rng.IntN(36)]
								}
								batch[i] = eventlog.Record{ID: uint64((batchIndex*batchSize + i) * 10), Payload: payload}
							}
							if err := log.Append(batch); err != nil {
								b.Fatal(err)
							}
						}
						durations["build"] += time.Since(buildStart)
						if _, err := log.Reclaim(b.Context()); err != nil {
							b.Fatal(err)
						}
						if err := log.Close(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
						start := time.Now()
						log, err = backend.open(path)
						if err != nil {
							b.Fatal(err)
						}
						durations["reopen"] += time.Since(start)
						appended := eventlog.Record{ID: uint64(count * 10), Payload: bytes.Repeat([]byte("new!"), payloadSize/4)}
						start = time.Now()
						if err := log.Append([]eventlog.Record{appended}); err != nil {
							b.Fatal(err)
						}
						durations["append"] += time.Since(start)
						target := uint64((count / 2) * 10)
						start = time.Now()
						result, err := log.Rewrite(b.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, r.ID != target, nil })
						if err != nil || !result.Published || result.ChangedRecords != 1 {
							b.Fatal(result, err)
						}
						durations["scrub"] += time.Since(start)
						start = time.Now()
						reclaimed, err := log.Reclaim(b.Context())
						if err != nil {
							b.Fatal(err)
						}
						removedBytes += reclaimed.RemovedBytes
						durations["reclaim"] += time.Since(start)
						start = time.Now()
						seen := 0
						err = log.Scan(b.Context(), eventlog.Coverage{Start: 0, End: appended.ID + 1}, func(r eventlog.Record) error {
							expected := uint64(seen * 10)
							if expected >= target {
								expected += 10
							}
							if r.ID != expected || len(r.Payload) != payloadSize {
								return fmt.Errorf("unexpected survivor %d at position %d", r.ID, seen)
							}
							seen++
							return nil
						})
						if err != nil || seen != count {
							b.Fatal(seen, err)
						}
						durations["scan"] += time.Since(start)
						b.StopTimer()
						if err := log.Close(); err != nil {
							b.Fatal(err)
						}
						log, err = backend.open(path)
						if err != nil {
							b.Fatal(err)
						}
						if head, ok, err := log.LastAppended(); err != nil || !ok || head != appended.ID {
							b.Fatal(head, ok, err)
						}
						if _, err := log.Read(target); !errors.Is(err, eventlog.ErrNotFound) {
							b.Fatal("erased ID survived recovery", err)
						}
						if r, err := log.Read(appended.ID); err != nil || !bytes.Equal(r.Payload, appended.Payload) {
							b.Fatal("append lost after recovery", err)
						}
						if err := log.Close(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
					for phase, duration := range durations {
						b.ReportMetric(float64(duration.Nanoseconds())/float64(b.N)/1e6, phase+"-ms/op")
					}
					b.ReportMetric(float64(count*payloadSize), "history-B")
					b.ReportMetric(float64(removedBytes)/float64(b.N), "reclaimed-B/op")
				})
			}
		})
	}
}
