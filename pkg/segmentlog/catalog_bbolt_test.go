package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func newBoltLog(t *testing.T, target int) *Log {
	t.Helper()
	l, e := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: target, Encoding: Options{Compression: ZstdDefault}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func reopenBoltLog(t *testing.T, l *Log) *Log {
	t.Helper()
	if e := l.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := OpenLog(l.path, l.encoding)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = next.Close() })
	return next
}

func TestBoltLogLifecycle(t *testing.T) {
	l := newBoltLog(t, 48)
	for i := uint64(0); i < 10; i++ {
		if e := l.Append([]Record{{i * 10, nil}}); e != nil {
			t.Fatal(e)
		}
	}
	before, e := l.catalog.Current()
	if e != nil {
		t.Fatal(e)
	}
	if len(before.Segments) != 3 {
		t.Fatal(before)
	}
	first := before.Segments[0]
	untouched := before.Segments[1]
	data := readBytes(t, filepath.Join(l.path, untouched.File))
	result, e := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID > 20 && r.ID != 90, nil })
	if e != nil || !result.Published || !result.TailChanged || result.EmptiedSegments != 1 {
		t.Fatal(result, e)
	}
	after, e := l.catalog.Current()
	if e != nil {
		t.Fatal(e)
	}
	if after.Segments[0].File != "" || after.Segments[0].Coverage != first.Coverage || after.Segments[1] != untouched || !bytes.Equal(data, readBytes(t, filepath.Join(l.path, untouched.File))) {
		t.Fatal(after)
	}
	l = reopenBoltLog(t, l)
	if id, ok, e := l.LastAppended(); e != nil || !ok || id != 90 {
		t.Fatal(id, ok, e)
	}
	if r, e := l.Seek(0); e != nil || r.ID != 30 {
		t.Fatal(r, e)
	}
	if _, e = l.Read(90); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	var ids []uint64
	if e = l.Scan(t.Context(), Coverage{31, 80}, func(r Record) error { ids = append(ids, r.ID); return nil }); e != nil || !reflect.DeepEqual(ids, []uint64{40, 50, 60, 70}) {
		t.Fatal(ids, e)
	}
	if _, e = l.Rewrite(t.Context(), 2, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }); e != nil {
		t.Fatal(e)
	}
	noOp, e := l.catalog.Current()
	if e != nil || !reflect.DeepEqual(after.Segments, noOp.Segments) {
		t.Fatal(noOp, e)
	}
	reclaimed, e := l.Reclaim(t.Context())
	if e != nil || reclaimed.RemovedFiles != 2 {
		t.Fatal(reclaimed, e)
	}
	if _, e = os.Stat(filepath.Join(l.path, first.File)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
	if _, e = l.Reclaim(t.Context()); e != nil {
		t.Fatal(e)
	}
	if e = l.Append([]Record{{100, nil}, {110, nil}, {120, nil}}); e != nil {
		t.Fatal(e)
	}
	l = reopenBoltLog(t, l)
	if id, ok, e := l.LastAppended(); e != nil || !ok || id != 120 {
		t.Fatal(id, ok, e)
	}
	for _, name := range []string{"CURRENT"} {
		if _, e := os.Stat(filepath.Join(l.path, name)); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("unexpected flat catalog", e)
		}
	}
}

func TestBoltLogCommitFailures(t *testing.T) {
	for _, operation := range []string{"rollover", "rewrite"} {
		for _, stage := range []string{"before", "inside", "after"} {
			t.Run(operation+"/"+stage, func(t *testing.T) {
				l := newBoltLog(t, 32)
				if e := l.Append([]Record{{0, nil}, {10, nil}}); e != nil {
					t.Fatal(e)
				}
				s := l.catalog.(*boltCatalog)
				boom := errors.New("commit failure")
				s.commit = func(fn func(*bolt.Tx) error) error {
					if stage == "before" {
						return boom
					}
					if stage == "inside" {
						return s.db.Update(func(tx *bolt.Tx) error {
							if e := fn(tx); e != nil {
								return e
							}
							return boom
						})
					}
					if e := s.db.Update(fn); e != nil {
						return e
					}
					return boom
				}
				var e error
				if operation == "rollover" {
					e = l.Append([]Record{{20, nil}})
				} else {
					_, e = l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 0, nil })
				}
				if !errors.Is(e, boom) || !errors.Is(e, ErrLogPoisoned) {
					t.Fatal(e)
				}
				if e = l.Append([]Record{{30, nil}}); !errors.Is(e, ErrLogPoisoned) {
					t.Fatal(e)
				}
				l = reopenBoltLog(t, l)
				_, e = l.Read(0)
				if operation == "rewrite" && stage == "after" {
					if !errors.Is(e, ErrNotFound) {
						t.Fatal(e)
					}
				} else if e != nil {
					t.Fatal(e)
				}
				if _, e = l.Read(10); e != nil {
					t.Fatal(e)
				}
				_, e = l.Read(20)
				committedAppend := operation == "rollover" && stage == "after"
				if committedAppend {
					if e != nil {
						t.Fatal(e)
					}
				} else if !errors.Is(e, ErrNotFound) {
					t.Fatal(e)
				}
				if _, e = l.Reclaim(t.Context()); e != nil {
					t.Fatal(e)
				}
				next := uint64(20)
				if committedAppend {
					next = 30
				}
				if e = l.Append([]Record{{next, nil}}); e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}

func TestBoltLogReclaimRetriesAfterQueueFailure(t *testing.T) {
	l := newBoltLog(t, 32)
	if e := l.Append([]Record{{0, nil}, {10, nil}, {20, nil}}); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 0, nil }); e != nil {
		t.Fatal(e)
	}
	s := l.catalog.(*boltCatalog)
	boom := errors.New("queue commit failed")
	s.commit = func(func(*bolt.Tx) error) error { return boom }
	if r, e := l.Reclaim(t.Context()); !errors.Is(e, boom) || r.RemovedFiles != 1 {
		t.Fatal(r, e)
	}
	l = reopenBoltLog(t, l)
	if r, e := l.Reclaim(t.Context()); e != nil || r.RemovedFiles != 0 {
		t.Fatal(r, e)
	}
	if _, e := l.Read(10); e != nil {
		t.Fatal(e)
	}
}

func TestBoltLogCorruptionAndExplicitOpen(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		dir := t.TempDir()
		if _, e := OpenLog(dir, Options{}); !errors.Is(e, os.ErrNotExist) {
			t.Fatal(e)
		}
		entries, e := os.ReadDir(dir)
		if e != nil || len(entries) != 0 {
			t.Fatal(entries, e)
		}
	})
	for _, mode := range []string{"header", "range", "active", "closed", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			l := newBoltLog(t, 16)
			if e := l.Append([]Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}}); e != nil {
				t.Fatal(e)
			}
			s := l.catalog.(*boltCatalog)
			c, e := l.catalog.Current()
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "header", "range":
				e = s.db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket(boltStateBucket)
					k := boltHeaderKey
					if mode == "range" {
						b = tx.Bucket(boltRangesBucket)
						k = rangeKey(c.Segments[1].Coverage.Start)
					}
					v := bytes.Clone(b.Get(k))
					v[len(v)-1] ^= 1
					return b.Put(k, v)
				})
			case "active":
				var b bytes.Buffer
				if e = WriteTailHeader(&b, c.Active.Start+1); e == nil {
					e = os.WriteFile(filepath.Join(l.path, c.Active.File), b.Bytes(), 0o600)
				}
			case "symlink":
				path := filepath.Join(l.path, c.Segments[1].File)
				if e = os.Rename(path, path+".hidden"); e == nil {
					e = os.Symlink(path+".hidden", path)
				}
			case "closed":
				e = os.Remove(filepath.Join(l.path, c.Segments[1].File))
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = l.Close(); e != nil {
				t.Fatal(e)
			}
			next, e := OpenLog(l.path, Options{})
			if mode == "header" || mode == "active" {
				if !errors.Is(e, ErrCorrupt) {
					if next != nil {
						_ = next.Close()
					}
					t.Fatal(e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer next.Close()
			_, e = next.Read(10)
			if (mode == "range" || mode == "symlink") && !errors.Is(e, ErrCorrupt) {
				t.Fatal(e)
			}
			if mode == "closed" && !errors.Is(e, os.ErrNotExist) {
				t.Fatal(e)
			}
		})
	}
}

func TestBoltAtomicMultiRangeRewrite(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			l := newBoltLog(t, 32)
			if e := l.Append([]Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}, {40, nil}}); e != nil {
				t.Fatal(e)
			}
			s := l.catalog.(*boltCatalog)
			boom := errors.New("interrupted rewrite")
			s.commit = func(fn func(*bolt.Tx) error) error {
				if committed {
					if e := s.db.Update(fn); e != nil {
						return e
					}
					return boom
				}
				return s.db.Update(func(tx *bolt.Tx) error {
					if e := fn(tx); e != nil {
						return e
					}
					return boom
				})
			}
			if _, e := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID%20 != 0, nil }); !errors.Is(e, boom) {
				t.Fatal(e)
			}
			l = reopenBoltLog(t, l)
			for _, id := range []uint64{0, 20, 40} {
				_, e := l.Read(id)
				if committed {
					if !errors.Is(e, ErrNotFound) {
						t.Fatal(id, e)
					}
				} else if e != nil {
					t.Fatal(id, e)
				}
			}
			for _, id := range []uint64{10, 30} {
				if _, e := l.Read(id); e != nil {
					t.Fatal(id, e)
				}
			}
			if id, ok, e := l.LastAppended(); e != nil || !ok || id != 40 {
				t.Fatal(id, ok, e)
			}
			c, e := l.catalog.head()
			if e != nil || (c.Generation == 1) != committed {
				t.Fatal(c, e)
			}
			result, e := l.Reclaim(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			want := uint64(0)
			if committed {
				want = 3
			}
			if result.RemovedFiles != want {
				t.Fatal(result, want)
			}
			if e = l.Verify(t.Context()); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestBoltScanSerializesRewrite(t *testing.T) {
	l := newBoltLog(t, 32)
	if e := l.Append([]Record{{0, nil}, {10, nil}, {20, nil}}); e != nil {
		t.Fatal(e)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	once := sync.OnceFunc(func() { close(release) })
	defer once()
	scanned := make(chan error, 1)
	rewritten := make(chan error, 1)
	go func() {
		scanned <- l.Scan(t.Context(), Coverage{0, 21}, func(r Record) error {
			if r.ID == 0 {
				close(entered)
				<-release
			}
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("scan stalled")
	}
	if l.mu.TryLock() {
		l.mu.Unlock()
		t.Fatal("scan released layout ownership")
	}
	go func() {
		_, e := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 0, nil })
		rewritten <- e
	}()
	once()
	if e := awaitOperation(t, scanned); e != nil {
		t.Fatal(e)
	}
	if e := awaitOperation(t, rewritten); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Read(0); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
}
