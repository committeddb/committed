package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// The layout snapshot lists every sealed segment as it is on disk, the tail,
// and the tail's committed length; under concurrent writes the bytes below
// that length never change, and compression shows up as .zst paths.
func TestLayoutSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seedEntries(t, l, 1, 200)

	lay, err := l.LayoutSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(lay.Sealed) == 0 {
		t.Fatalf("tiny segments must have sealed some: %+v", lay)
	}
	if lay.LastIndex != 200 {
		t.Fatalf("last index %d", lay.LastIndex)
	}
	fi, err := os.Stat(lay.Tail.Path)
	if err != nil {
		t.Fatal(err)
	}
	if lay.TailLen != fi.Size() {
		t.Fatalf("tail len %d vs file %d", lay.TailLen, fi.Size())
	}
	for i, s := range lay.Sealed {
		if _, err := os.Stat(s.Path); err != nil {
			t.Fatalf("sealed %d: %v", i, err)
		}
		if i > 0 && s.Index <= lay.Sealed[i-1].Index {
			t.Fatalf("sealed segments out of order at %d", i)
		}
	}
	if lay.Tail.Index <= lay.Sealed[len(lay.Sealed)-1].Index {
		t.Fatalf("tail index %d not past the last sealed %d", lay.Tail.Index, lay.Sealed[len(lay.Sealed)-1].Index)
	}

	// Under a concurrent writer, a snapshot's tail prefix is stable: the
	// bytes below TailLen read the same later.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); seedEntries(t, l, 201, 300) }()
	snap, err := l.LayoutSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := os.ReadFile(snap.Tail.Path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(prefix)) < snap.TailLen {
		t.Fatalf("tail file shorter (%d) than the snapshot's committed length (%d)", len(prefix), snap.TailLen)
	}
	prefix = prefix[:snap.TailLen]
	wg.Wait()
	later, err := os.ReadFile(snap.Tail.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(later[:snap.TailLen], prefix) {
		t.Fatal("bytes below the fenced tail length changed under later writes")
	}

	compressAll(t, l)
	lay, err = l.LayoutSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range lay.Sealed {
		if !strings.HasSuffix(s.Path, zstExt) || !IsCompressedSegmentPath(s.Path) {
			t.Fatalf("sealed %s not reported as compressed after compressAll", s.Path)
		}
	}
	if IsCompressedSegmentPath(lay.Tail.Path) {
		t.Fatalf("the tail is never compressed: %s", lay.Tail.Path)
	}
	verifyEntries(t, l, 1, 500)
}
