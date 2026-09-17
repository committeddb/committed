package segmentlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/committeddb/committed/internal/durablefs"
)

func newLog(t *testing.T, target int) *Log {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("unsupported durability platform")
	}
	log, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: target, Encoding: Options{Compression: ZstdDefault}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return log
}

func reopenLog(t *testing.T, log *Log) *Log {
	t.Helper()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenLog(log.path, log.encoding)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close() })
	return next
}

type rangeDigest struct {
	Coverage Coverage
	Digest   [32]byte
	Count    uint64
}

func logLayout(t *testing.T, log *Log) []rangeDigest {
	t.Helper()
	c, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	result := make([]rangeDigest, 0, len(c.Segments))
	for _, s := range c.Segments {
		result = append(result, rangeDigest{s.Coverage, s.SHA256, s.Count})
	}
	return result
}

func TestLogBatchIndependentRotation(t *testing.T) {
	records := []Record{{0, []byte("zero")}, {10, bytes.Repeat([]byte("a"), 30)}, {12, bytes.Repeat([]byte("b"), 120)}, {100, []byte("hundred")}, {110, []byte("next")}, {999, bytes.Repeat([]byte("z"), 30)}}
	var baseline []rangeDigest
	for _, batch := range []int{len(records), 1, 2, 3} {
		log := newLog(t, 70)
		for i := 0; i < len(records); i += batch {
			end := min(i+batch, len(records))
			if err := log.Append(records[i:end]); err != nil {
				t.Fatal(err)
			}
			// Reopening between batches must not reset original framed-byte accounting.
			log = reopenLog(t, log)
		}
		got := logLayout(t, log)
		// Original coverage/counts are batch-independent; retained append-group
		// framing (and therefore file digests) reflects the supplied batches.
		for i := range got {
			got[i].Digest = [32]byte{}
		}
		if baseline == nil {
			baseline = got
		} else if !reflect.DeepEqual(got, baseline) {
			t.Fatalf("batch %d changed ranges/counts: %v vs %v", batch, got, baseline)
		}
		if len(got) != 3 || got[0].Coverage != (Coverage{0, 11}) || got[1].Coverage != (Coverage{11, 13}) || got[2].Coverage != (Coverage{13, 111}) {
			t.Fatal("unexpected original ranges", got)
		}
		for _, want := range records {
			r, err := log.Read(want.ID)
			if err != nil || !reflect.DeepEqual(r, want) {
				t.Fatal(r, want, err)
			}
		}
		for _, tc := range []struct{ id, want uint64 }{{1, 10}, {11, 12}, {13, 100}, {111, 999}} {
			r, err := log.Seek(tc.id)
			if err != nil || r.ID != tc.want {
				t.Fatal("seek gap", tc, r, err)
			}
		}
		if _, err := log.Read(11); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := log.Seek(1000); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
}

func TestLogInvalidBatchIsNotPartiallyWritten(t *testing.T) {
	log := newLog(t, 40)
	if err := log.Append([]Record{{1, []byte("first")}}); err != nil {
		t.Fatal(err)
	}
	before, _ := log.tail.State()
	catalog, _ := log.catalog.Current()
	for _, records := range [][]Record{nil, {{2, []byte("valid prefix")}, {1, nil}}, {{2, nil}, {2, nil}}, {{^uint64(0), nil}}} {
		if err := log.Append(records); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	after, _ := log.tail.State()
	next, _ := log.catalog.Current()
	if before != after || catalog.Revision != next.Revision {
		t.Fatal("invalid batch mutated log")
	}
	if err := log.Append([]Record{{2, nil}}); err != nil {
		t.Fatal("invalid batch poisoned log", err)
	}
}

type rotationPublisher struct {
	catalogPublisher
	failAt, calls int
	after         bool
	boom          error
}

func (p *rotationPublisher) Replace(name string, w func(io.Writer) error) (durablefs.Result, error) {
	p.calls++
	if p.calls == p.failAt && !p.after {
		return durablefs.Result{}, p.boom
	}
	result, err := p.catalogPublisher.Replace(name, w)
	if err != nil {
		return result, err
	}
	if p.calls == p.failAt && p.after {
		return durablefs.Result{Installed: true}, errors.Join(durablefs.ErrUncertain, p.boom)
	}
	return result, nil
}

func TestLogRotationFailurePreservesDurablePrefix(t *testing.T) {
	for _, after := range []bool{false, true} {
		log := newLog(t, 40)
		if err := log.Append([]Record{{1, []byte("first")}}); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("catalog failure")
		pub := &rotationPublisher{catalogPublisher: log.catalog.(*CatalogStore).pub, failAt: 2, after: after, boom: boom}
		log.catalog.(*CatalogStore).pub = pub
		err := log.Append([]Record{{2, []byte("second")}, {3, []byte("third")}, {4, []byte("fourth")}})
		if !errors.Is(err, ErrLogPoisoned) || !errors.Is(err, boom) {
			t.Fatal(err)
		}
		count := pub.calls
		if err := log.Append([]Record{{5, nil}}); !errors.Is(err, ErrLogPoisoned) || pub.calls != count {
			t.Fatal("continued after failure", err)
		}
		if _, err := log.Seek(0); !errors.Is(err, ErrLogPoisoned) {
			t.Fatal("read uncertain layout", err)
		}
		log = reopenLog(t, log)
		// ID 1 was acknowledged; ID 2 was synced during a call that later failed.
		for _, id := range []uint64{1, 2} {
			if _, err := log.Read(id); err != nil {
				t.Fatal("lost durable prefix", id, err)
			}
		}
		if _, err := log.Read(3); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := log.Append([]Record{{3, []byte("third")}, {4, []byte("fourth")}}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []uint64{1, 2, 3, 4} {
			if _, err := log.Read(id); err != nil {
				t.Fatal(id, err)
			}
		}
	}
}

func TestLogTailFailureAndIncompleteRecovery(t *testing.T) {
	for _, short := range []bool{false, true} {
		log := newLog(t, 100)
		if err := log.Append([]Record{{1, []byte("first")}}); err != nil {
			t.Fatal(err)
		}
		f := &faultyTail{File: log.file, short: short}
		if !short {
			f.syncErr = errors.New("sync failed")
		}
		log.tail.file = f
		err := log.Append([]Record{{2, []byte("second")}})
		if !errors.Is(err, ErrLogPoisoned) {
			t.Fatal(err)
		}
		c, _ := log.catalog.Current()
		path := filepath.Join(log.path, c.Active.File)
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		recovered, err := OpenLog(log.path, log.encoding)
		if short {
			if !errors.Is(err, ErrIncompleteTail) {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("recovery modified incomplete suffix", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if _, err := recovered.Read(2); err != nil {
				t.Fatal("lost complete unacknowledged append", err)
			}
		}
	}
}

func TestLogEncodingChangePreservesExistingFiles(t *testing.T) {
	log := newLog(t, 40)
	if err := log.Append([]Record{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}); err != nil {
		t.Fatal(err)
	}
	old := logLayout(t, log)
	c, _ := log.catalog.Current()
	info, err := os.Stat(filepath.Join(log.path, c.Segments[0].File))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenLog(log.path, Options{Compression: ZstdBest})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.Append([]Record{{4, []byte("four")}}); err != nil {
		t.Fatal(err)
	}
	got := logLayout(t, next)
	if !reflect.DeepEqual(got[:len(old)], old) {
		t.Fatal("reencoded unrelated segments")
	}
	after, err := os.Stat(filepath.Join(log.path, c.Segments[0].File))
	if err != nil || !os.SameFile(info, after) || !info.ModTime().Equal(after.ModTime()) {
		t.Fatal("changed original file", err)
	}
	current, _ := next.catalog.Current()
	if current.SegmentBytes != 40 {
		t.Fatal("lost persisted target")
	}
}

func TestLogCreationAndClose(t *testing.T) {
	log := newLog(t, 40)
	if _, err := CreateLog(log.path, 0, LogOptions{}); !errors.Is(err, ErrLocked) {
		t.Fatal("reinitialized existing directory", err)
	}
	if _, err := log.Read(0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Append([]Record{{1, nil}}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := log.Seek(0); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	for _, opts := range []LogOptions{{SegmentBytes: -1}, {SegmentBytes: 1}, {SegmentBytes: maxGroupBytes + 1}, {Encoding: Options{Compression: 255}}, {Encoding: Options{BlockSize: 16}}} {
		dir := t.TempDir()
		if _, err := CreateLog(dir, 0, opts); !errors.Is(err, ErrInvalid) {
			t.Fatal(opts, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatal("invalid options wrote output", err)
		}
	}
}

func TestLogConcurrentReadAndAppend(t *testing.T) {
	log := newLog(t, 60)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				if _, err := log.Seek(0); err != nil && !errors.Is(err, ErrNotFound) {
					t.Error(err)
				}
			}
		})
	}
	for id := uint64(1); id <= 20; id++ {
		if err := log.Append([]Record{{id, []byte("value")}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for id := uint64(1); id <= 20; id++ {
		if _, err := log.Read(id); err != nil {
			t.Fatal(err)
		}
	}
}

type rotationInstaller struct {
	fileInstaller
	calls, failAt int
	after         bool
	boom          error
}

func (p *rotationInstaller) Install(name string, w func(io.Writer) error) (durablefs.Result, error) {
	p.calls++
	if p.calls == p.failAt && !p.after {
		return durablefs.Result{}, p.boom
	}
	result, err := p.fileInstaller.Install(name, w)
	if err != nil {
		return result, err
	}
	if p.calls == p.failAt && p.after {
		return result, p.boom
	}
	return result, nil
}

func TestLogPreparedFileFailuresKeepOldTail(t *testing.T) {
	for _, at := range []int{1} {
		for _, after := range []bool{false, true} {
			log := newLog(t, 40)
			if err := log.Append([]Record{{1, []byte("first")}}); err != nil {
				t.Fatal(err)
			}
			before, _ := log.catalog.Current()
			boom := errors.New("install failure")
			log.dir = &rotationInstaller{fileInstaller: log.dir, failAt: at, after: after, boom: boom}
			if err := log.Append([]Record{{2, []byte("second")}}); !errors.Is(err, ErrLogPoisoned) || !errors.Is(err, boom) {
				t.Fatal(err)
			}
			// An orphaned empty replacement tail does not change recovery selection
			// while CURRENT still references the old append file.
			log = reopenLog(t, log)
			got, _ := log.catalog.Current()
			if got.Revision != before.Revision || got.Active.File != before.Active.File {
				t.Fatal("adopted orphaned work", got)
			}
			if _, err := log.Read(1); err != nil {
				t.Fatal(err)
			}
			if _, err := log.Read(2); !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
			if err := log.Append([]Record{{2, []byte("second")}}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestLogEmptyRangesAndPersistedTarget(t *testing.T) {
	log := newLog(t, 40)
	dir := log.path
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCatalogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := store.Current()
	bad := cloneCatalog(current)
	bad.Revision++
	bad.SegmentBytes = 80
	if err := store.Publish(current.Revision, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("changed rotation target", err)
	}
	// Prepare a generic catalog with a known empty historical range and a new tail.
	d, err := durablefs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Install("later.active", func(w io.Writer) error { return WriteTailHeader(w, 1000) }); err != nil {
		t.Fatal(err)
	}
	next := cloneCatalog(current)
	next.Revision++
	next.Generation++
	next.Segments = []SegmentRef{{Coverage: Coverage{0, 1000}}}
	next.Active = &TailRef{File: "later.active", Start: 1000}
	if err := store.Publish(current.Revision, next); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenLog(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.Append([]Record{{999, nil}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := recovered.Append([]Record{{1010, []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	if r, err := recovered.Seek(0); err != nil || r.ID != 1010 {
		t.Fatal(r, err)
	}
}
