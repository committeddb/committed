package eventlog_test

import (
	"encoding/binary"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// BenchmarkEventLogCachedReads measures steady reads through the shared storage
// boundary, with full-sized segments. Construction, initial acquisition, and
// teardown are untimed. This is not an Actual-decoding or concurrent benchmark.
func BenchmarkEventLogCachedReads(b *testing.B) {
	const segmentBytes = 20 << 20
	const perSegment = segmentBytes / 4096
	const count = 4*perSegment + perSegment/2
	factories := []struct {
		name   string
		create func(string) (eventlog.EventLog, error)
	}{
		{"segmented-uncached", func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 0, segmentlog.LogOptions{SegmentBytes: segmentBytes})
		}},
		{"segmented-cached", func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 0, segmentlog.LogOptions{SegmentBytes: segmentBytes, Cache: segmentlog.CacheOptions{RecentBytes: 64 << 20, HistoricalBytes: 64 << 20}})
		}},
		{"tidwall-default-cache", func(path string) (eventlog.EventLog, error) {
			return tidwall.Create(path, 0, tidwall.Options{SegmentBytes: segmentBytes})
		}},
	}
	for _, factory := range factories {
		b.Run(factory.name, func(b *testing.B) {
			log, err := factory.create(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = log.Close() })
			records := make([]eventlog.Record, 64)
			for first := 0; first < count; first += len(records) {
				for i := range records {
					id := uint64((first + i) * 10)
					payload := make([]byte, 4080)
					binary.LittleEndian.PutUint64(payload, id)
					records[i] = eventlog.Record{ID: id, Payload: payload}
				}
				if err := log.Append(records); err != nil {
					b.Fatal(err)
				}
			}
			check := func(r eventlog.Record, id uint64, err error) {
				b.Helper()
				if err != nil || r.ID != id || len(r.Payload) != 4080 || binary.LittleEndian.Uint64(r.Payload) != id {
					b.Fatalf("read %d: got %d, %v", id, r.ID, err)
				}
			}
			for _, window := range []struct {
				name  string
				first int
			}{
				{"active-tail", count - 128},
				{"recent-sealed", 3*perSegment + 128},
				{"historical", 128},
			} {
				b.Run(window.name, func(b *testing.B) {
					// Warm the same logical window in every implementation. Sequential IDs
					// model advancing cursors; wrap only within this fixed 128-record window.
					for i := range 128 {
						id := uint64((window.first + i) * 10)
						r, err := log.Seek(id)
						check(r, id, err)
					}
					b.ReportAllocs()
					b.SetBytes(4080)
					b.ResetTimer()
					i := 0
					for b.Loop() {
						id := uint64((window.first + i%128) * 10)
						r, err := log.Seek(id)
						check(r, id, err)
						i++
					}
				})
			}
			b.Run("historical-scan-64", func(b *testing.B) {
				scan := func() {
					next := uint64(1280)
					err := log.Scan(b.Context(), eventlog.Coverage{Start: 1280, End: 1920}, func(r eventlog.Record) error {
						check(r, next, nil)
						next += 10
						return nil
					})
					if err != nil || next != 1920 {
						b.Fatal(next, err)
					}
				}
				scan()
				b.ReportAllocs()
				b.SetBytes(64 * 4080)
				b.ResetTimer()
				for b.Loop() {
					scan()
				}
			})
		})
	}
}
