package eventlog_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type appendTiming struct {
	total, maximum time.Duration
	count          int
}

func (m *appendTiming) add(d time.Duration) {
	m.total += d
	m.maximum = max(m.maximum, d)
	m.count++
}

type rolloverComparison struct {
	ordinary, boundary appendTiming
	reopen             time.Duration
}

// Test-only physical observation: both plain backends keep existing payload
// filenames during append and create one new file when they cross a boundary.
// Directory observation is outside append timing and is not a storage contract.
func appendPayloadFiles(t testing.TB, path string) map[string]bool {
	t.Helper()
	names := make(map[string]bool)
	err := filepath.WalkDir(path, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		tail := strings.HasPrefix(name, "tail-") && strings.HasSuffix(name, ".active")
		dense := len(name) == 20 && strings.Trim(name, "0123456789") == ""
		if tail || dense {
			names[p] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func measureRolloverComparison(t testing.TB, backend backend, batchSize, batches int) rolloverComparison {
	t.Helper()
	path := t.TempDir()
	log, err := backend.create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if log != nil {
			_ = log.Close()
		}
	})
	files := appendPayloadFiles(t, path)
	if len(files) != 1 {
		t.Fatal("expected one initial payload file", files)
	}
	payload := func(id uint64) []byte {
		p := bytes.Repeat([]byte("r"), 4080)
		binary.LittleEndian.PutUint64(p, id)
		return p
	}
	var next uint64
	var metrics rolloverComparison
	for range batches {
		records := make([]eventlog.Record, batchSize)
		for i := range records {
			records[i] = eventlog.Record{ID: next, Payload: payload(next)}
			next += 3
		}
		started := time.Now()
		err := log.Append(records)
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		after := appendPayloadFiles(t, path)
		for name := range files {
			if !after[name] {
				t.Fatal("append removed a payload file", name)
			}
		}
		switch len(after) - len(files) {
		case 0:
			metrics.ordinary.add(elapsed)
		case 1:
			metrics.boundary.add(elapsed)
		default:
			t.Fatal("fixture must cross at most one boundary per batch", len(files), len(after))
		}
		files = after
	}
	if metrics.boundary.count < 2 || metrics.ordinary.count == 0 {
		t.Fatal("fixture did not exercise repeated rollover and ordinary append", metrics)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	log, err = backend.open(path)
	metrics.reopen = time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok, err := log.LastAppended(); err != nil || !ok || id != next-3 {
		t.Fatal("lost append frontier", id, ok, err)
	}
	var expected uint64
	err = log.Scan(t.Context(), eventlog.Coverage{End: next}, func(r eventlog.Record) error {
		if r.ID != expected || !bytes.Equal(r.Payload, payload(expected)) {
			return fmt.Errorf("record mismatch at %d: got %d", expected, r.ID)
		}
		expected += 3
		return nil
	})
	if err != nil || expected != next {
		t.Fatal("recovered history mismatch", expected, next, err)
	}
	// Continue the recovered history and verify that progress persists again.
	if err := log.Append([]eventlog.Record{{ID: next, Payload: payload(next)}}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = backend.open(path)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := log.Read(next); err != nil || !bytes.Equal(r.Payload, payload(next)) {
		t.Fatal("lost post-recovery append", err)
	}
	return metrics
}

func TestEventLogRolloverComparison(t *testing.T) {
	// The first two variants are plain: compression would add different work to
	// tidwall rollover while segmented compression applies only during rewrites.
	for _, backend := range backendsWithSegmentBytes(64 << 10)[:2] {
		t.Run(backend.name, func(t *testing.T) { measureRolloverComparison(t, backend, 8, 10) })
	}
}

// BenchmarkEventLogRollover compares equal logical histories and synchronous
// append batches. Physical framing and exact rotation positions differ.
func BenchmarkEventLogRollover(b *testing.B) {
	for _, backend := range backendsWithSegmentBytes(20 << 20)[:2] {
		b.Run(backend.name, func(b *testing.B) {
			var total rolloverComparison
			for b.Loop() {
				m := measureRolloverComparison(b, backend, 64, 320)
				total.ordinary.total += m.ordinary.total
				total.ordinary.count += m.ordinary.count
				total.ordinary.maximum = max(total.ordinary.maximum, m.ordinary.maximum)
				total.boundary.total += m.boundary.total
				total.boundary.count += m.boundary.count
				total.boundary.maximum = max(total.boundary.maximum, m.boundary.maximum)
				total.reopen += m.reopen
			}
			for name, timing := range map[string]appendTiming{"ordinary": total.ordinary, "boundary": total.boundary} {
				b.ReportMetric(float64(timing.total.Nanoseconds())/float64(timing.count)/1e6, name+"-mean-ms")
				b.ReportMetric(float64(timing.maximum.Nanoseconds())/1e6, name+"-max-ms")
				b.ReportMetric(float64(timing.count)/float64(b.N), name+"-batches/op")
			}
			b.ReportMetric(float64(total.reopen.Nanoseconds())/float64(b.N)/1e6, "reopen-ms/op")
		})
	}
}
