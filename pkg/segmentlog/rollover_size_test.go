package segmentlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
)

type rolloverSizeMetrics struct {
	fullRecovery, boundary, smallRecovery  time.Duration
	install, commit, fill                  time.Duration
	fillWrite, fillSync                    time.Duration
	slowestFill, slowestWrite, slowestSync time.Duration
}

// timedTailIO measures elapsed I/O calls, including time descheduled while in
// those calls. It does not isolate physical device latency.
type timedTailIO struct {
	TailFile
	write, sync   time.Duration
	writes, syncs int
}

func (f *timedTailIO) WriteAt(p []byte, off int64) (int, error) {
	started := time.Now()
	n, err := f.TailFile.WriteAt(p, off)
	f.write += time.Since(started)
	f.writes++
	return n, err
}

func (f *timedTailIO) Sync() error {
	started := time.Now()
	err := f.TailFile.Sync()
	f.sync += time.Since(started)
	f.syncs++
	return err
}

type timedRolloverInstall struct {
	fileInstaller
	elapsed *time.Duration
}

func (i timedRolloverInstall) Install(name string, write func(io.Writer) error) (durablefs.Result, error) {
	started := time.Now()
	result, err := i.fileInstaller.Install(name, write)
	*i.elapsed += time.Since(started)
	return result, err
}

// measureRolloverSize fills real active tails, then measures full-tail recovery,
// boundary append, and recovery with one new active record. Construction and
// verification remain outside these phase timers. The workload is warm-cache.
func measureRolloverSize(t testing.TB, target, samples int) rolloverSizeMetrics {
	t.Helper()
	const frameBytes = 4096
	if target < frameBytes || target%frameBytes != 0 || samples < 1 {
		t.Fatal("invalid rollover fixture")
	}
	l, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: target})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if l != nil {
			_ = l.Close()
		}
	})
	path := l.path
	payload := func(id uint64) []byte {
		b := bytes.Repeat([]byte{'r'}, frameBytes-16)
		binary.LittleEndian.PutUint64(b, id)
		return b
	}
	reopen := func() time.Duration {
		t.Helper()
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		l, err = OpenLog(path, Options{})
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		return elapsed
	}
	var m rolloverSizeMetrics
	var next uint64
	for sample := range samples {
		// A previous boundary leaves one record in the active tail. Reuse it;
		// every measured append must rotate an exactly full original-byte budget.
		for l.framed < uint64(target) {
			count := min(uint64(64), (uint64(target)-l.framed)/frameBytes)
			batch := make([]Record, 0, count)
			for range count {
				batch = append(batch, Record{next, payload(next)})
				next++
			}
			timed := &timedTailIO{TailFile: l.file}
			l.tail.file = timed
			started := time.Now()
			if err = l.Append(batch); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			l.tail.file = l.file
			if timed.writes != 1 || timed.syncs != 1 {
				t.Fatal("fill batch must perform one ordinary write and sync", timed.writes, timed.syncs)
			}
			m.fill += elapsed
			m.fillWrite += timed.write
			m.fillSync += timed.sync
			if elapsed > m.slowestFill {
				m.slowestFill, m.slowestWrite, m.slowestSync = elapsed, timed.write, timed.sync
			}
		}
		m.fullRecovery += reopen()
		if l.framed != uint64(target) {
			t.Fatal("recovery changed original byte budget", l.framed)
		}
		before, err := l.InspectCatalog()
		if err != nil || len(before.Segments) != sample {
			t.Fatal("unexpected history before rollover", err)
		}
		info, err := os.Stat(filepath.Join(path, before.Active.File))
		if err != nil {
			t.Fatal(err)
		}
		record := Record{next, payload(next)}
		l.dir = timedRolloverInstall{l.dir, &m.install}
		store := l.catalog.(*boltCatalog)
		commit := store.commit
		store.commit = func(fn func(*bolt.Tx) error) error {
			started := time.Now()
			err := commit(fn)
			m.commit += time.Since(started)
			return err
		}
		started := time.Now()
		if err = l.Append([]Record{record}); err != nil {
			t.Fatal(err)
		}
		m.boundary += time.Since(started)
		next++
		after, err := l.InspectCatalog()
		if err != nil || len(after.Segments) != sample+1 {
			t.Fatal("measured append did not rotate", err)
		}
		closed := after.Segments[sample]
		if closed.File != before.Active.File || closed.Coverage != (Coverage{before.Active.Start, record.ID}) || closed.Count != uint64(target/frameBytes) || closed.TailBytes != info.Size() {
			t.Fatal("incorrect closed range", closed)
		}
		closedInfo, err := os.Stat(filepath.Join(path, closed.File))
		if err != nil || !os.SameFile(info, closedInfo) || info.Size() != closedInfo.Size() {
			t.Fatal("replaced predecessor", err)
		}
		m.smallRecovery += reopen()
		if r, err := l.Read(record.ID); err != nil || !bytes.Equal(r.Payload, record.Payload) {
			t.Fatal("lost boundary record", r.ID, err)
		}
		if id, ok, err := l.LastAppended(); err != nil || !ok || id != next-1 {
			t.Fatal("wrong append frontier", id, ok, err)
		}
	}
	var seen uint64
	if err = l.Scan(t.Context(), Coverage{0, next}, func(r Record) error {
		if r.ID != seen || !bytes.Equal(r.Payload, payload(seen)) {
			return fmt.Errorf("unexpected record %d, want %d", r.ID, seen)
		}
		seen++
		return nil
	}); err != nil || seen != next {
		t.Fatal("history mismatch", seen, next, err)
	}
	if err = l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRolloverSizeWorkload(t *testing.T) { measureRolloverSize(t, 64<<10, 3) }

func BenchmarkRolloverSize(b *testing.B) {
	const samples = 5
	for _, target := range []int{1 << 20, 20 << 20, 32 << 20} {
		b.Run(fmt.Sprintf("MiB=%d", target>>20), func(b *testing.B) {
			for b.Loop() {
				m := measureRolloverSize(b, target, samples)
				b.ReportMetric(float64(m.fill.Nanoseconds())/samples/1e6, "tail-fill-ms")
				b.ReportMetric(float64(m.fillWrite.Nanoseconds())/samples/1e6, "fill-write-ms")
				b.ReportMetric(float64(m.fillSync.Nanoseconds())/samples/1e6, "fill-sync-ms")
				b.ReportMetric(float64((m.fill-m.fillWrite-m.fillSync).Nanoseconds())/samples/1e6, "fill-other-ms")
				b.ReportMetric(float64(m.slowestFill.Nanoseconds())/1e6, "slowest-fill-ms")
				b.ReportMetric(float64(m.slowestWrite.Nanoseconds())/1e6, "slowest-fill-write-ms")
				b.ReportMetric(float64(m.slowestSync.Nanoseconds())/1e6, "slowest-fill-sync-ms")
				b.ReportMetric(float64((m.slowestFill-m.slowestWrite-m.slowestSync).Nanoseconds())/1e6, "slowest-fill-other-ms")
				b.ReportMetric(float64(m.fullRecovery.Nanoseconds())/samples/1e6, "full-tail-reopen-ms")
				b.ReportMetric(float64(m.boundary.Nanoseconds())/samples/1e6, "boundary-ms")
				b.ReportMetric(float64(m.install.Nanoseconds())/samples/1e6, "tail-install-ms")
				b.ReportMetric(float64(m.commit.Nanoseconds())/samples/1e6, "metadata-commit-ms")
				b.ReportMetric(float64((m.boundary-m.install-m.commit).Nanoseconds())/samples/1e6, "boundary-other-ms")
				b.ReportMetric(float64(m.smallRecovery.Nanoseconds())/samples/1e6, "small-tail-reopen-ms")
			}
		})
	}
}
