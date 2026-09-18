package segmentlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManagedCachedReadsAndDiskVerification(t *testing.T) {
	l := rotatedLog(t)
	l.cache = newSegmentCache(0, 1<<20)
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	ref := c.Segments[0]
	first, err := l.Seek(ref.Coverage.Start)
	if err != nil {
		t.Fatal(err)
	}
	want := string(first.Payload)
	if len(first.Payload) > 0 {
		first.Payload[0] ^= 0xff
	}
	path := filepath.Join(l.path, ref.File)
	hidden := path + ".hidden"
	if err := os.Rename(path, hidden); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(hidden, path); err != nil {
			t.Error(err)
		}
	}()
	// A warmed source is independent of disk/file handles. Explicit verification
	// must still inspect the selected file even when all read data is cached.
	again, err := l.Read(first.ID)
	if err != nil || string(again.Payload) != want {
		t.Fatal(again, err)
	}
	var seen uint64
	if err := l.Scan(t.Context(), ref.Coverage, func(r Record) error {
		seen++
		if len(r.Payload) > 0 {
			r.Payload[0] ^= 0xff
		}
		return nil
	}); err != nil || seen != ref.Count {
		t.Fatal(seen, err)
	}
	if err := l.Verify(t.Context()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("verification served cached bytes", err)
	}
	stats := l.cache.stats()
	if stats.Misses != 1 || stats.Hits != 2 || stats.HistoricalEntries != 1 {
		t.Fatal(stats)
	}
}

func TestManagedCacheRewriteIdentity(t *testing.T) {
	l := rotatedLog(t)
	l.cache = newSegmentCache(0, 1<<20)
	before, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range before.Segments {
		if _, err := l.Seek(ref.Coverage.Start); err != nil {
			t.Fatal(err)
		}
	}
	old := before.Segments[0]
	held, ok := l.cache.acquire(old)
	if !ok {
		t.Fatal("missing source")
	}
	original, err := held.Seek(old.Coverage.Start)
	if err != nil {
		t.Fatal(err)
	}
	result, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
		if r.ID == original.ID {
			r.Payload[0] ^= 0xff
		}
		return r.Payload, true, nil
	})
	if err != nil || result.ChangedSegments != 1 {
		t.Fatal(result, err)
	}
	if _, ok := l.cache.acquire(old); ok {
		t.Fatal("retired revision retained")
	}
	for _, ref := range before.Segments[1:] {
		if _, ok := l.cache.acquire(ref); !ok {
			t.Fatal("unchanged revision evicted")
		}
	}
	if r, err := held.Seek(original.ID); err != nil || string(r.Payload) != string(original.Payload) {
		t.Fatal("transform modified old snapshot", r, err)
	}
	if _, err := l.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	changed, err := l.Read(original.ID)
	if err != nil || changed.Payload[0] != original.Payload[0]^0xff {
		t.Fatal(changed, err)
	}
	// The replacement is now indexed and must also be safely cacheable.
	after, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.cache.acquire(after.Segments[0]); !ok {
		t.Fatal("replacement not cached")
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if l.cache != nil {
		t.Fatal("close retained cache")
	}
	if _, err := l.Read(original.ID); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestManagedCacheRejectsCorruptMiss(t *testing.T) {
	l := rotatedLog(t)
	l.cache = newSegmentCache(0, 1<<20)
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	ref := c.Segments[0]
	path := filepath.Join(l.path, ref.File)
	data := readBytes(t, path)
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := l.Scan(t.Context(), ref.Coverage, func(Record) error { calls++; return nil }); !errors.Is(err, ErrCorrupt) || calls != 0 {
		t.Fatal(calls, err)
	}
	if stats := l.cache.stats(); stats.HistoricalEntries != 0 {
		t.Fatal(stats)
	}
}
