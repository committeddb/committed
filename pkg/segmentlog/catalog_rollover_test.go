package segmentlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func prepareTestRollover(t *testing.T, log *Log) *preparedRollover {
	t.Helper()
	c, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	state, digest, err := log.tail.rolloverState()
	if err != nil {
		t.Fatal(err)
	}
	s := segmentStorage{path: log.path, installer: log.dir}
	p, err := s.prepareRollover(c, state, digest, []Record{{state.Last + 10, nil}}, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.file.Close() })
	return p
}

func TestPreparedRolloverPublishesMetadataOnly(t *testing.T) {
	log := newLog(t, 32)
	if err := log.Append([]Record{{0, nil}, {10, nil}}); err != nil {
		t.Fatal(err)
	}
	p := prepareTestRollover(t, log)
	// Deliberately break exclusive ownership to make any payload reopening fail.
	// Preparation, not publication, owns file work. Recovery must still check it.
	for _, name := range []string{p.closed.File, p.active.File} {
		if err := os.Rename(filepath.Join(log.path, name), filepath.Join(log.path, name+".hidden")); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.catalog.publishRollover(p); err != nil {
		t.Fatal(err)
	}
	if err := log.catalog.publishRollover(p); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLog(log.path, log.encoding); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery trusted preparation", err)
	}
	for _, name := range []string{p.closed.File, p.active.File} {
		if err := os.Rename(filepath.Join(log.path, name+".hidden"), filepath.Join(log.path, name)); err != nil {
			t.Fatal(err)
		}
	}
	log = reopenLog(t, log)
	if err := log.Append([]Record{{30, nil}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{0, 10, 20, 30} {
		if _, err := log.Read(id); err != nil {
			t.Fatal(id, err)
		}
	}
}

func TestPreparedRolloverRejectsWrongLayout(t *testing.T) {
	for _, mode := range []string{"other-store", "stale"} {
		t.Run(mode, func(t *testing.T) {
			log := newLog(t, 32)
			if err := log.Append([]Record{{0, nil}}); err != nil {
				t.Fatal(err)
			}
			p := prepareTestRollover(t, log)
			store := log.catalog.(*boltCatalog)
			if mode == "other-store" {
				store = newLog(t, 32).catalog.(*boltCatalog)
			} else {
				if err := store.db.Update(func(tx *bolt.Tx) error {
					h, err := readBoltHeader(tx)
					if err != nil {
						return err
					}
					h.Catalog.Revision++
					return putBoltHeader(tx, h)
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.publishRollover(p); !errors.Is(err, ErrCatalogConflict) {
				t.Fatal("accepted mismatched preparation", err)
			}
		})
	}
}

func TestPreparedRolloverPublicationFailures(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			log := newLog(t, 32)
			if err := log.Append([]Record{{0, nil}, {10, nil}}); err != nil {
				t.Fatal(err)
			}
			before, err := log.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			boom := errors.New("publication failure")
			failMetadataCommit(log, 1, stage == "after", boom)
			if err := log.Append([]Record{{20, nil}}); !errors.Is(err, boom) || !errors.Is(err, ErrLogPoisoned) {
				t.Fatal(err)
			}
			log = reopenLog(t, log)
			after, err := log.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			wantRevision := before.Revision
			if stage == "after" {
				wantRevision++
			}
			if after.Revision != wantRevision {
				t.Fatal("wrong recovered selection", after.Revision, wantRevision)
			}
			for _, id := range []uint64{0, 10} {
				if _, err := log.Read(id); err != nil {
					t.Fatal(id, err)
				}
			}
			_, err = log.Read(20)
			if stage == "after" {
				if err != nil {
					t.Fatal("lost committed first group", err)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatal("adopted unpublished first group", err)
			}
		})
	}
}
