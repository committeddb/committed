package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/durablefs"
)

type tailWriteProbe struct {
	fileInstaller
	calls, failAt int
	written       int64
	short         bool
	failure       error
}

type probedTailWriter struct {
	io.Writer
	probe *tailWriteProbe
}

func (w probedTailWriter) Write(p []byte) (int, error) {
	w.probe.calls++
	if w.probe.calls == w.probe.failAt {
		if w.probe.short {
			return w.Writer.Write(p[:len(p)-1])
		}
		return 0, w.probe.failure
	}
	n, err := w.Writer.Write(p)
	w.probe.written += int64(n)
	return n, err
}

func (p *tailWriteProbe) Install(name string, write func(io.Writer) error) (durablefs.Result, error) {
	return p.fileInstaller.Install(name, func(w io.Writer) error { return write(probedTailWriter{w, p}) })
}

func tailRewriteFixture(t testing.TB, count int) *Log {
	t.Helper()
	l, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: 20 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for start := 0; start < count; start += 64 {
		records := make([]Record, min(64, count-start))
		for i := range records {
			records[i] = Record{uint64(start + i), bytes.Repeat([]byte("v"), 4080)}
		}
		if err := l.Append(records); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

func changeFirstTailRecord(r Record) ([]byte, bool, error) {
	if r.ID == 0 {
		r.Payload[0] = 'w'
	}
	return r.Payload, true, nil
}

func TestTailRewriteFlushFailures(t *testing.T) {
	for _, tc := range []struct {
		name          string
		count, failAt int
	}{{"final", 2, 1}, {"full-buffer", 80, 1}, {"final-after-full", 80, 2}} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/short=%t", tc.name, short), func(t *testing.T) {
				l := tailRewriteFixture(t, tc.count)
				before, err := l.InspectCatalog()
				if err != nil {
					t.Fatal(err)
				}
				injected := errors.New("flush failed")
				probe := &tailWriteProbe{fileInstaller: l.dir, failAt: tc.failAt, short: short, failure: injected}
				l.dir = probe
				result, err := l.Rewrite(t.Context(), 1, changeFirstTailRecord)
				want := injected
				if short {
					want = io.ErrShortWrite
				}
				if !errors.Is(err, want) || !errors.Is(err, ErrLogPoisoned) || result.Published || probe.calls != tc.failAt {
					t.Fatal(result, probe.calls, err)
				}
				l = reopenLog(t, l)
				after, err := l.InspectCatalog()
				if err != nil || after.Revision != before.Revision || *after.Active != *before.Active {
					t.Fatal("selected failed replacement", after, err)
				}
				for i := range tc.count {
					r, err := l.Read(uint64(i))
					if err != nil || !bytes.Equal(r.Payload, bytes.Repeat([]byte("v"), 4080)) {
						t.Fatal("lost original record", i, err)
					}
				}
				if err := l.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func measureTailRewrite(t testing.TB, count int) (time.Duration, int, int64) {
	t.Helper()
	l := tailRewriteFixture(t, count)
	probe := &tailWriteProbe{fileInstaller: l.dir}
	l.dir = probe
	started := time.Now()
	result, err := l.Rewrite(t.Context(), 1, changeFirstTailRecord)
	elapsed := time.Since(started)
	if err != nil || !result.Published || !result.TailChanged || result.ChangedSegments != 0 {
		t.Fatal(result, err)
	}
	catalog, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(l.path, catalog.Active.File))
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.New()
	if err := WriteTailHeader(expected, 0); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		r := Record{uint64(i), bytes.Repeat([]byte("v"), 4080)}
		if i == 0 {
			r.Payload[0] = 'w'
		}
		_, _ = expected.Write(encodeTailGroup([]Record{r}, 4096))
	}
	actual := sha256.Sum256(raw)
	if !bytes.Equal(actual[:], expected.Sum(nil)) || probe.written != int64(len(raw)) {
		t.Fatal("changed encoded bytes or byte accounting")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLog(l.path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if id, ok, err := reopened.LastAppended(); err != nil || !ok || id != uint64(count-1) {
		t.Fatal(id, ok, err)
	}
	if err := reopened.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Append([]Record{{uint64(count), []byte("after rewrite")}}); err != nil {
		t.Fatal(err)
	}
	return elapsed, probe.calls, probe.written
}

func TestTailRewriteBuffersWrites(t *testing.T) {
	_, calls, size := measureTailRewrite(t, 80)
	if calls != 2 || size != 32+80*4144 {
		t.Fatal(calls, size)
	}
}

func BenchmarkTailRewriteWrites(b *testing.B) {
	for b.Loop() {
		elapsed, calls, size := measureTailRewrite(b, 5120)
		b.ReportMetric(float64(elapsed.Nanoseconds())/1e6, "rewrite-ms")
		b.ReportMetric(float64(calls), "file-writes/op")
		b.ReportMetric(float64(size), "replacement-B/op")
	}
}
