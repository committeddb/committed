package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/committeddb/committed/internal/durablefs"
)

func rotatedLog(t *testing.T) *Log {
	t.Helper()
	log := newLog(t, 40)
	for id := uint64(1); id <= 6; id++ {
		if err := log.Append([]Record{{id, []byte("value")}}); err != nil {
			t.Fatal(err)
		}
	}
	return log
}

func referencedNames(t *testing.T, log *Log) map[string]bool {
	t.Helper()
	c, name, err := loadCatalog(log.path)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{"CURRENT": true, name: true, c.Active.File: true}
	for _, s := range c.Segments {
		if s.File != "" {
			names[s.File] = true
		}
	}
	return names
}

func readBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReclaimObsoleteFilesPreservesCurrent(t *testing.T) {
	log := rotatedLog(t)
	live := referencedNames(t, log)
	type original struct {
		info os.FileInfo
		data []byte
	}
	originalFiles := map[string]original{}
	for name := range live {
		path := filepath.Join(log.path, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		originalFiles[name] = original{info, readBytes(t, path)}
	}
	temp, err := os.CreateTemp(log.path, ".segmentlog-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := temp.WriteString("unpublished partial output"); err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := catalogName(999, [32]byte{9})
	if err := os.WriteFile(filepath.Join(log.path, orphan), []byte("unpublished future catalog"), 0o600); err != nil {
		t.Fatal(err)
	}
	var expectedFiles, expectedBytes uint64
	entries, err := os.ReadDir(log.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !live[entry.Name()] {
			info, err := entry.Info()
			if err != nil {
				t.Fatal(err)
			}
			expectedFiles++
			expectedBytes += uint64(info.Size())
		}
	}
	result, err := log.Reclaim(context.Background())
	if err != nil || result.RemovedFiles != expectedFiles || result.RemovedBytes != expectedBytes || result.SkippedEntries != 0 {
		t.Fatal(result, expectedFiles, expectedBytes, err)
	}
	if expectedFiles == 0 {
		t.Fatal("test did not create obsolete files")
	}
	entries, err = os.ReadDir(log.path)
	if err != nil || len(entries) != len(live) {
		t.Fatal("unexpected remaining set", entries, err)
	}
	for name, before := range originalFiles {
		path := filepath.Join(log.path, name)
		after, err := os.Stat(path)
		if err != nil || !os.SameFile(before.info, after) || !before.info.ModTime().Equal(after.ModTime()) || !bytes.Equal(before.data, readBytes(t, path)) {
			t.Fatal("changed referenced file", name, err)
		}
	}
	again, err := log.Reclaim(context.Background())
	if err != nil || again != (ReclaimResult{}) {
		t.Fatal("not idempotent", again, err)
	}
	log = reopenLog(t, log)
	for id := uint64(1); id <= 6; id++ {
		if _, err := log.Read(id); err != nil {
			t.Fatal("lost live record", id, err)
		}
	}
	if err := log.Append([]Record{{7, []byte("later")}}); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimPreservesUnknownEntries(t *testing.T) {
	log := rotatedLog(t)
	unknown := []string{"notes.txt", "custom.seg", ".segmentlog-not-ours", "tail-99999999999999999999-00000000000000000000000000000000.active"}
	for _, name := range unknown {
		if err := os.WriteFile(filepath.Join(log.path, name), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	directory, err := uniqueName("segment", 5000, ".seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(log.path, directory), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(log.path, directory, "nested"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link, err := uniqueName("tail", 5000, ".active")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("notes.txt", filepath.Join(log.path, link)); err != nil {
		t.Fatal(err)
	}
	result, err := log.Reclaim(context.Background())
	if err != nil || result.SkippedEntries != uint64(len(unknown)+2) {
		t.Fatal(result, err)
	}
	for _, name := range append(unknown, directory, link) {
		if _, err := os.Lstat(filepath.Join(log.path, name)); err != nil {
			t.Fatal("removed unrecognized/nonregular entry", name, err)
		}
	}
	if string(readBytes(t, filepath.Join(log.path, "notes.txt"))) != "keep" {
		t.Fatal("followed symlink")
	}
}

type failingRemover struct {
	fileRemover
	calls, failAt int
	after         bool
	boom          error
	cancel        context.CancelFunc
}

func (r *failingRemover) Remove(name string) (bool, error) {
	r.calls++
	if r.calls == r.failAt && !r.after {
		return false, r.boom
	}
	removed, err := r.fileRemover.Remove(name)
	if err != nil {
		return removed, err
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.calls == r.failAt && r.after {
		return removed, errors.Join(durablefs.ErrUncertain, r.boom)
	}
	return removed, nil
}

func TestReclaimFailureAndRestart(t *testing.T) {
	for _, after := range []bool{false, true} {
		log := rotatedLog(t)
		boom := errors.New("remove failed")
		remover := &failingRemover{fileRemover: log.remover, failAt: 2, after: after, boom: boom}
		log.remover = remover
		result, err := log.Reclaim(context.Background())
		want := uint64(1)
		if after {
			want = 2
		}
		if result.RemovedFiles != want || !errors.Is(err, ErrLogPoisoned) || !errors.Is(err, boom) {
			t.Fatal(result, err)
		}
		count := remover.calls
		if _, err := log.Reclaim(context.Background()); !errors.Is(err, ErrLogPoisoned) || remover.calls != count {
			t.Fatal("continued after uncertain cleanup", err)
		}
		log = reopenLog(t, log)
		if _, err := log.Reclaim(context.Background()); err != nil {
			t.Fatal("could not resume", err)
		}
		for id := uint64(1); id <= 6; id++ {
			if _, err := log.Read(id); err != nil {
				t.Fatal("lost live history", id, err)
			}
		}
		entries, err := os.ReadDir(log.path)
		if err != nil || len(entries) != len(referencedNames(t, log)) {
			t.Fatal("left known orphans", err)
		}
	}
}

func TestReclaimCancellation(t *testing.T) {
	log := rotatedLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := log.Reclaim(ctx); !errors.Is(err, context.Canceled) || result != (ReclaimResult{}) {
		t.Fatal(result, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	log.remover = &failingRemover{fileRemover: log.remover, cancel: cancel}
	result, err := log.Reclaim(ctx)
	if !errors.Is(err, context.Canceled) || result.RemovedFiles != 1 {
		t.Fatal(result, err)
	}
	if _, err := log.Read(1); err != nil {
		t.Fatal("cancellation poisoned log", err)
	}
	if _, err := log.Reclaim(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimRefusesDamagedCurrentState(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		log := rotatedLog(t)
		entries, err := os.ReadDir(log.path)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(log.path, "CURRENT")
		if !pointer {
			c, _ := log.catalog.Current()
			path = filepath.Join(log.path, c.Segments[0].File)
		}
		if err := os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
			t.Fatal(err)
		}
		result, err := log.Reclaim(context.Background())
		if !errors.Is(err, ErrLogPoisoned) || result.RemovedFiles != 0 {
			t.Fatal(result, err)
		}
		after, err := os.ReadDir(log.path)
		if err != nil || !reflect.DeepEqual(entries, after) {
			t.Fatal("deleted despite failed verification", err)
		}
	}
}

func TestCatalogClonePreservesCanonicalEmptySlice(t *testing.T) {
	c := Catalog{History: [16]byte{1}, Revision: 1, Segments: []SegmentRef{}}
	before, err := encodeCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	after, err := encodeCatalog(cloneCatalog(c))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("clone changed canonical bytes", err)
	}
}

func TestReclaimConcurrentRotation(t *testing.T) {
	log := rotatedLog(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		for id := uint64(7); id <= 10; id++ {
			if err := log.Append([]Record{{id, []byte("value")}}); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Go(func() {
		for range 3 {
			if _, err := log.Reclaim(context.Background()); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()
	for id := uint64(1); id <= 10; id++ {
		if _, err := log.Read(id); err != nil {
			t.Fatal("cleanup raced rotation", id, err)
		}
	}
}

func TestReclaimRequiresMatchingConfirmedCatalog(t *testing.T) {
	log := rotatedLog(t)
	// Simulate a mismatch between memory and the pointer recovered from disk.
	log.catalog.(*CatalogStore).current.Generation++
	result, err := log.Reclaim(context.Background())
	if result.RemovedFiles != 0 || !errors.Is(err, ErrCatalogConflict) || !errors.Is(err, ErrLogPoisoned) {
		t.Fatal(result, err)
	}
}
