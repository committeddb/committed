package segmentlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func prepareTestRollover(t *testing.T, log *Log) *preparedRollover {
	t.Helper()
	c, err := log.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	state, err := log.tail.State()
	if err != nil {
		t.Fatal(err)
	}
	s := segmentStorage{path: log.path, installer: log.dir}
	p, err := s.prepareRollover(c, log.file, state)
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
	pub := &failingPublisher{catalogPublisher: log.catalog.(*CatalogStore).pub, at: "sync", boom: errors.New("redundant directory sync")}
	log.catalog.(*CatalogStore).pub = pub
	if err := log.catalog.(*CatalogStore).publishRollover(p); err != nil {
		t.Fatal(err)
	}
	if len(pub.calls) != 2 || pub.calls[0] != "install" || pub.calls[1] != "replace" {
		t.Fatal("unexpected publication protocol", pub.calls)
	}
	if err := log.catalog.(*CatalogStore).publishRollover(p); !errors.Is(err, ErrInvalid) {
		t.Fatal("reused preparation", err)
	}
	if _, err := OpenCatalogStore(log.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery trusted preparation", err)
	}
	for _, name := range []string{p.closed.File, p.active.File} {
		if err := os.Rename(filepath.Join(log.path, name+".hidden"), filepath.Join(log.path, name)); err != nil {
			t.Fatal(err)
		}
	}
	log = reopenLog(t, log)
	if err := log.Append([]Record{{20, nil}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{0, 10, 20} {
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
			store := log.catalog.(*CatalogStore)
			if mode == "other-store" {
				store = newLog(t, 32).catalog.(*CatalogStore)
			} else {
				c, err := store.Current()
				if err != nil {
					t.Fatal(err)
				}
				c.Revision++
				if err := store.Publish(c.Revision-1, c); err != nil {
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
	for _, stage := range []string{"install", "replace-before", "replace-after"} {
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
			log.catalog.(*CatalogStore).pub = &failingPublisher{catalogPublisher: log.catalog.(*CatalogStore).pub, at: stage, boom: boom}
			if err := log.Append([]Record{{20, nil}}); !errors.Is(err, boom) || !errors.Is(err, ErrLogPoisoned) {
				t.Fatal(err)
			}
			log = reopenLog(t, log)
			after, err := log.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			wantRevision := before.Revision
			if stage == "replace-after" {
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
			if _, err := log.Read(20); !errors.Is(err, ErrNotFound) {
				t.Fatal("appended before publication succeeded", err)
			}
		})
	}
}
