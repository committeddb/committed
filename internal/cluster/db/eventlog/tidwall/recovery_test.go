package tidwall

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/durablefs"
)

type failPublisher struct {
	publisher
	after bool
	err   error
}

func (f failPublisher) Replace(name string, w func(io.Writer) error) (durablefs.Result, error) {
	if !f.after {
		return durablefs.Result{}, f.err
	}
	r, e := f.publisher.Replace(name, w)
	return r, errors.Join(e, f.err)
}

func TestPublicationFailureSelectsCompleteGeneration(t *testing.T) {
	for _, after := range []bool{false, true} {
		path := t.TempDir()
		log, e := Create(path, 1, Options{SegmentBytes: 128})
		if e != nil {
			t.Fatal(e)
		}
		if e = log.Append([]eventlog.Record{{ID: 1, Payload: []byte("old")}, {ID: 10, Payload: []byte("old")}}); e != nil {
			t.Fatal(e)
		}
		boom := errors.New("publication failure")
		log.pub = failPublisher{publisher: log.pub, after: after, err: boom}
		r, e := log.Rewrite(t.Context(), 1, func(record eventlog.Record) ([]byte, bool, error) { return []byte("new"), record.ID != 10, nil })
		if !errors.Is(e, boom) || r.Published {
			t.Fatal(r, e)
		}
		if e = log.Close(); e != nil {
			t.Fatal(e)
		}
		log, e = Open(path)
		if e != nil {
			t.Fatal(e)
		}
		wantGeneration := uint64(0)
		if after {
			wantGeneration = 1
		}
		if generation, err := log.Generation(); err != nil || generation != wantGeneration {
			t.Fatal("recovered generation", generation, wantGeneration, err)
		}
		want := "old"
		if after {
			want = "new"
		}
		record, e := log.Read(1)
		if e != nil || string(record.Payload) != want {
			t.Fatal(record, e)
		}
		if _, e = log.Read(10); after && !errors.Is(e, eventlog.ErrNotFound) {
			t.Fatal(e)
		}
		if id, ok, e := log.LastAppended(); e != nil || !ok || id != 10 {
			t.Fatal(id, ok, e)
		}
		if _, e = log.Reclaim(t.Context()); e != nil {
			t.Fatal(e)
		}
		if e = log.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestMissingCurrentDoesNotAdoptOrphan(t *testing.T) {
	path := t.TempDir()
	log, e := Create(path, 1, Options{})
	if e != nil {
		t.Fatal(e)
	}
	if e = log.Append([]eventlog.Record{{ID: 1}}); e != nil {
		t.Fatal(e)
	}
	if e = log.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(path, "CURRENT")); e != nil {
		t.Fatal(e)
	}
	if opened, e := Open(path); e == nil {
		_ = opened.Close()
		t.Fatal("adopted orphan")
	}
}

func TestReclaimRetainsUnknownEntries(t *testing.T) {
	path := t.TempDir()
	log, e := Create(path, 1, Options{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = log.Close() })
	if e := log.Append([]eventlog.Record{{ID: 1, Payload: []byte("old")}}); e != nil {
		t.Fatal(e)
	}
	old := log.state.Directory
	if _, e := log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) { return []byte("new"), true, nil }); e != nil {
		t.Fatal(e)
	}
	unknown := filepath.Join(path, old, "keep.txt")
	if e := os.WriteFile(unknown, []byte("unrelated"), 0o600); e != nil {
		t.Fatal(e)
	}
	r, e := log.Reclaim(t.Context())
	if e != nil || r.SkippedEntries != 1 || r.RemovedFiles == 0 {
		t.Fatal(r, e)
	}
	if b, e := os.ReadFile(unknown); e != nil || string(b) != "unrelated" {
		t.Fatal(e)
	}
	record, e := log.Read(1)
	if e != nil || string(record.Payload) != "new" {
		t.Fatal(record, e)
	}
}

func TestCorruptCurrentAndReferencedDataFailClosed(t *testing.T) {
	for _, target := range []string{"CURRENT", "payload"} {
		t.Run(target, func(t *testing.T) {
			path := t.TempDir()
			log, e := Create(path, 1, Options{})
			if e != nil {
				t.Fatal(e)
			}
			if e := log.Append([]eventlog.Record{{ID: 1, Payload: []byte("payload")}}); e != nil {
				t.Fatal(e)
			}
			dataDir := log.state.Directory
			if e := log.Close(); e != nil {
				t.Fatal(e)
			}
			file := filepath.Join(path, "CURRENT")
			if target == "payload" {
				entries, e := os.ReadDir(filepath.Join(path, dataDir))
				if e != nil {
					t.Fatal(e)
				}
				file = filepath.Join(path, dataDir, entries[0].Name())
			}
			b, e := os.ReadFile(file)
			if e != nil {
				t.Fatal(e)
			}
			b[len(b)/2] ^= 1
			if e := os.WriteFile(file, b, 0o600); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(path)
			if e == nil {
				_ = reopened.Close()
				t.Fatal("accepted corruption")
			}
			// A failed open releases ownership; the next attempt reports corruption too.
			reopened, e = Open(path)
			if e == nil {
				_ = reopened.Close()
				t.Fatal("accepted corruption on retry")
			}
			if errors.Is(e, eventlog.ErrLocked) {
				t.Fatal("failed open leaked lock")
			}
		})
	}
}
