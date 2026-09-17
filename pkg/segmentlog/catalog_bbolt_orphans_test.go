package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func orphanFile(t *testing.T, l *Log, start uint64) string {
	t.Helper()
	name, err := uniqueName("tail", start, ".active")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(l.path, name), []byte("unpublished"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestBoltOrphanSweepBatches(t *testing.T) {
	l := newBoltLog(t, 32)
	records := []Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}, {40, nil}}
	if err := l.Append(records); err != nil {
		t.Fatal(err)
	}
	// Include names sharing live starts, starts within live ranges, and beyond
	// the current tail, as well as incomplete installation files.
	for i := range 300 {
		orphanFile(t, l, uint64(i))
	}
	for i := range 3 {
		if err := os.WriteFile(filepath.Join(l.path, fmt.Sprintf(".segmentlog-%d", i)), []byte("temp"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unknown := []string{"notes", ".segmentlog-invalid", "catalog-00000000000000000001-" + fmt.Sprintf("%064d", 0) + ".manifest"}
	for _, name := range unknown {
		if err := os.WriteFile(filepath.Join(l.path, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	directory, err := uniqueName("segment", 0, ".seg")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(l.path, directory), 0o700); err != nil {
		t.Fatal(err)
	}
	link, err := uniqueName("tail", 0, ".active")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("notes", filepath.Join(l.path, link)); err != nil {
		t.Fatal(err)
	}
	// Prevent accidental use of the full diagnostic catalog snapshot.
	l.catalog = noFullCatalog{l.catalog}
	result, err := l.ReclaimOrphans(t.Context())
	if err != nil || result.RemovedFiles != 303 || result.RemovedBytes != 300*11+3*4 {
		t.Fatal(result, err)
	}
	for _, name := range append(unknown, directory, link) {
		if _, err = os.Lstat(filepath.Join(l.path, name)); err != nil {
			t.Fatal("removed preserved entry", name, err)
		}
	}
	if result, err = l.ReclaimOrphans(t.Context()); err != nil || result.RemovedFiles != 0 {
		t.Fatal(result, err)
	}
	checkBoltCrashRecords(t, l, records, false)
	if err = l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBoltOrphanSweepRejectsMetadataDamage(t *testing.T) {
	for _, damage := range []string{"checksum", "filename", "gap", "count", "extra"} {
		t.Run(damage, func(t *testing.T) {
			l := newBoltLog(t, 16)
			if err := l.Append([]Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}}); err != nil {
				t.Fatal(err)
			}
			name := orphanFile(t, l, 10)
			s := l.catalog.(*boltCatalog)
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(boltRangesBucket)
				k := rangeKey(1)
				switch damage {
				case "checksum":
					return b.Put(k, []byte("damaged"))
				case "filename":
					ref, err := decodeBoltRef(k, b.Get(k))
					if err != nil {
						return err
					}
					// A checksummed reference must not select a name at another
					// start: otherwise point lookup could mistake it for an orphan.
					ref.File = name
					return putBoltRef(tx, ref)
				case "gap":
					return b.Delete(k)
				case "extra":
					return putBoltRef(tx, SegmentRef{Coverage: Coverage{100, 101}})
				default:
					h, err := readBoltHeader(tx)
					if err != nil {
						return err
					}
					h.Ranges++
					return putBoltHeader(tx, h)
				}
			}); err != nil {
				t.Fatal(err)
			}
			if err := l.Verify(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("verification missed damaged metadata", err)
			}
			if result, err := l.ReclaimOrphans(t.Context()); !errors.Is(err, ErrCorrupt) || result.RemovedFiles != 0 {
				t.Fatal(result, err)
			}
			if _, err := os.Stat(filepath.Join(l.path, name)); err != nil {
				t.Fatal("deleted before validation", err)
			}
		})
	}
}

func TestBoltOrphanSweepRetry(t *testing.T) {
	for _, mode := range []string{"cancel", "before", "after", "crash"} {
		t.Run(mode, func(t *testing.T) {
			l := newBoltLog(t, 32)
			for i := range 3 {
				orphanFile(t, l, uint64(i))
			}
			if mode == "crash" {
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				runBoltCrash(t, l.path, "orphans", "first-removal")
			} else {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				remover := &failingRemover{fileRemover: l.remover, failAt: 1, after: mode == "after", boom: errors.New("remove failed")}
				if mode == "cancel" {
					remover.failAt, remover.cancel = 0, cancel
				}
				l.remover = remover
				_, err := l.ReclaimOrphans(ctx)
				if mode == "cancel" {
					if !errors.Is(err, context.Canceled) || l.poison != nil {
						t.Fatal(err, l.poison)
					}
				} else if !errors.Is(err, ErrLogPoisoned) {
					t.Fatal(err)
				}
			}
			l = reopenBoltLog(t, l)
			want := uint64(2)
			if mode == "before" {
				want = 3
			}
			if result, err := l.ReclaimOrphans(t.Context()); err != nil || result.RemovedFiles != want {
				t.Fatal(result, err)
			}
			if err := l.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBoltOrphanSweepBeforeRetirement(t *testing.T) {
	l := newBoltLog(t, 32)
	records := []Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}, {40, nil}}
	if err := l.Append(records); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Rewrite(t.Context(), 1, eraseEvenTens); err != nil {
		t.Fatal(err)
	}
	if result, err := l.ReclaimOrphans(t.Context()); err != nil || result.RemovedFiles != 3 {
		t.Fatal(result, err)
	}
	l = reopenBoltLog(t, l)
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal("missing retired files must not fail verification", err)
	}
	if err := l.catalog.(*boltCatalog).db.View(func(tx *bolt.Tx) error {
		if got := tx.Bucket(boltRetiredBucket).Stats().KeyN; got != 3 {
			t.Errorf("sweep changed retirement queue: %d", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := l.Reclaim(t.Context()); err != nil || result.RemovedFiles != 0 {
		t.Fatal(result, err)
	}
	if err := l.catalog.(*boltCatalog).db.View(func(tx *bolt.Tx) error {
		if k, _ := tx.Bucket(boltRetiredBucket).Cursor().First(); k != nil {
			t.Error("retirement queue not drained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	checkBoltCrashRecords(t, l, records, true)
}
