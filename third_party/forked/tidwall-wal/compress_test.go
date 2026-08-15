package wal

// committeddb fork patch: sealed-segment compression tests.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func compressOpts() *Options {
	return &Options{
		NoSync:                   true,
		SegmentSize:              512, // tiny segments so tests cycle often
		SealedSegmentCompression: CompressionZstd,
	}
}

// seedEntries writes n entries of compressible JSON-ish payloads.
func seedEntries(t *testing.T, l *Log, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		data := fmt.Sprintf(`{"widget_id":%d,"widget_name":"widget-%d","widget_state":"active","padding":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i, i)
		if err := l.Write(uint64(i), []byte(data)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

func verifyEntries(t *testing.T, l *Log, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		data, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		want := fmt.Sprintf(`{"widget_id":%d,`, i)
		if !strings.HasPrefix(string(data), want) {
			t.Fatalf("read %d: wrong entry: %s", i, data)
		}
	}
}

func compressAll(t *testing.T, l *Log) int {
	t.Helper()
	n := 0
	for {
		did, err := l.CompressNextSealed()
		if err != nil {
			t.Fatalf("compress: %v", err)
		}
		if !did {
			return n
		}
		n++
	}
}

func countByExt(t *testing.T, dir string) (plain, zst int) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		switch {
		case len(e.Name()) == 20:
			plain++
		case strings.HasSuffix(e.Name(), zstExt):
			zst++
		}
	}
	return plain, zst
}

// TestCompressSealedRoundTrip: seal-compress a multi-segment log, read every
// entry back through the mixed format, reopen, read again.
func TestCompressSealedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, l, 1, 200)

	n := compressAll(t, l)
	if n < 2 {
		t.Fatalf("expected several sealed segments to compress, got %d", n)
	}
	plain, zst := countByExt(t, dir)
	if plain != 1 {
		t.Fatalf("only the tail should stay plain, got %d plain (%d zst)", plain, zst)
	}
	if zst != n {
		t.Fatalf("compressed files %d != compressions %d", zst, n)
	}

	// Reads decompress transparently, live and after reopen.
	verifyEntries(t, l, 1, 200)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	verifyEntries(t, l, 1, 200)

	// The log stays writable and future seals compress too.
	seedEntries(t, l, 201, 100)
	compressAll(t, l)
	verifyEntries(t, l, 1, 300)
}

// TestCompressionDisabledByDefault: a default-options log never compresses
// and CompressNextSealed refuses.
func TestCompressionDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &Options{NoSync: true, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seedEntries(t, l, 1, 100)
	if _, err := l.CompressNextSealed(); err != ErrCompressionDisabled {
		t.Fatalf("want ErrCompressionDisabled, got %v", err)
	}
	if _, zst := countByExt(t, dir); zst != 0 {
		t.Fatal("no segment may compress without the option")
	}
}

// TestCrashLeftoverHealing: load() deletes an orphan .SEAL temp, and prefers
// the .zst when the plain sibling's cleanup was lost.
func TestCrashLeftoverHealing(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, l, 1, 200)
	compressAll(t, l)

	// Fabricate the two crash windows on the FIRST compressed segment:
	// (a) an orphan .SEAL temp, (b) a stale plain sibling.
	var firstZst string
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), zstExt) {
			firstZst = e.Name()
			break
		}
	}
	if firstZst == "" {
		t.Fatal("no compressed segment")
	}
	plainName := strings.TrimSuffix(firstZst, zstExt)
	if err := os.WriteFile(filepath.Join(dir, plainName+zstSealExt), []byte("torn"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, plainName), []byte("stale plain"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	verifyEntries(t, l, 1, 200)
	if _, err := os.Stat(filepath.Join(dir, plainName+zstSealExt)); !os.IsNotExist(err) {
		t.Fatal("orphan .SEAL temp must be deleted on open")
	}
	if _, err := os.Stat(filepath.Join(dir, plainName)); !os.IsNotExist(err) {
		t.Fatal("stale plain sibling must be deleted on open")
	}
}

// TestCompressedTailRecovery: if the last on-disk segment is compressed (the
// plain tail vanished), open establishes lastIndex from it and starts a
// fresh plain tail — the tail-is-always-plain invariant self-heals.
func TestCompressedTailRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, l, 1, 200)
	compressAll(t, l)
	// A few more writes guarantee the tail holds entries (a write always
	// lands in the tail, cycling first if full); compress whatever sealed.
	seedEntries(t, l, 201, 3)
	compressAll(t, l)

	// Removing the (non-empty) plain tail must lose exactly its entries: the
	// recovery re-establishes lastIndex from the compressed predecessor.
	l.mu.RLock()
	tail := l.segments[len(l.segments)-1]
	tailPath, tailIndex := tail.path, tail.index
	prevCompressed := isCompressedPath(l.segments[len(l.segments)-2].path)
	l.mu.RUnlock()
	if isCompressedPath(tailPath) {
		t.Fatal("the tail must be plain")
	}
	if !prevCompressed {
		t.Fatal("test setup: the segment before the tail must be compressed")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tailPath); err != nil {
		t.Fatal(err)
	}

	l, err = Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	li, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if li != tailIndex-1 {
		t.Fatalf("lastIndex %d, want %d (everything before the removed tail)", li, tailIndex-1)
	}
	verifyEntries(t, l, 1, int(li))
	// Writable at li+1.
	if err := l.Write(li+1, []byte(`{"widget_id":-1,"after":"recovery"}`)); err != nil {
		t.Fatal(err)
	}
}

// TestDecompressDirRoundTrip: the offline downgrade door restores the plain
// format byte-for-byte (the pre-compression binary can read the result).
func TestDecompressDirRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, l, 1, 200)
	compressAll(t, l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := DecompressDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("expected several segments decompressed, got %d", n)
	}
	if _, zst := countByExt(t, dir); zst != 0 {
		t.Fatal("no .zst may remain after DecompressDir")
	}

	// A log opened WITHOUT compression support semantics (default options —
	// the old binary's view) reads everything.
	l, err = Open(dir, &Options{NoSync: true, SegmentSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	verifyEntries(t, l, 1, 200)
}

// TestTruncateAcrossCompressedSegments: truncations decompress-and-rewrite
// through the same tables as reads; the rewritten segment is plain and the
// log stays consistent.
func TestTruncateAcrossCompressedSegments(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seedEntries(t, l, 1, 300)
	compressAll(t, l)

	// Front-truncate to an index INSIDE a compressed segment.
	l.mu.RLock()
	cut := l.segments[1].index + 1
	l.mu.RUnlock()
	if err := l.TruncateFront(cut); err != nil {
		t.Fatal(err)
	}
	verifyEntries(t, l, int(cut), 300-int(cut)+1)
	if fi, err := l.FirstIndex(); err != nil || fi != cut {
		t.Fatalf("firstIndex %d want %d (%v)", fi, cut, err)
	}

	// Back-truncate to an index inside a (still-)compressed segment.
	l2 := l
	l2.mu.RLock()
	backCut := l2.segments[len(l2.segments)-2].index
	l2.mu.RUnlock()
	if err := l.TruncateBack(backCut); err != nil {
		t.Fatal(err)
	}
	verifyEntries(t, l, int(cut), int(backCut-cut)+1)
	if li, err := l.LastIndex(); err != nil || li != backCut {
		t.Fatalf("lastIndex %d want %d (%v)", li, backCut, err)
	}
	// Still writable and future seals still compress.
	seedEntries(t, l, int(backCut)+1, 50)
	compressAll(t, l)
	verifyEntries(t, l, int(cut), int(backCut)-int(cut)+51)
}

// TestCompressSwapGuardAgainstConcurrentTruncation pins the swap's identity
// guard: a TruncateBack that rewrites the source segment (same name, same
// index, new content) during the lock-free encode must cause the swap to
// DISCARD the stale encoding — installing it would resurrect the truncated
// entries. The test hook runs the truncation exactly in the race window.
func TestCompressSwapGuardAgainstConcurrentTruncation(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, compressOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seedEntries(t, l, 1, 300)

	// Pick the truncation point INSIDE the first sealed segment, so its
	// rewrite keeps the same name and index with different content.
	l.mu.RLock()
	firstSealed := l.segments[0]
	cut := firstSealed.index + 1
	l.mu.RUnlock()

	truncated := false
	compressSealedTestHook = func() {
		if !truncated {
			truncated = true
			if terr := l.TruncateBack(cut); terr != nil {
				t.Errorf("truncate in race window: %v", terr)
			}
		}
	}
	defer func() { compressSealedTestHook = nil }()

	// The first compression attempt encodes the pre-truncation content and
	// MUST discard it (the segment was rewritten underneath).
	if _, err := l.CompressNextSealed(); err != nil {
		t.Fatal(err)
	}
	compressSealedTestHook = nil

	// No stale .zst may exist, and the log's contents are the truncated set.
	if _, zst := countByExt(t, dir); zst != 0 {
		t.Fatal("the stale encoding must be discarded, not installed")
	}
	li, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if li != cut {
		t.Fatalf("lastIndex %d want %d", li, cut)
	}
	verifyEntries(t, l, 1, int(cut))
	if _, err := l.Read(cut + 1); err != ErrNotFound && err != ErrOutOfRange {
		t.Fatalf("entry past the truncation must not resurrect; got err=%v", err)
	}

	// Compression proceeds correctly on the post-truncation content.
	seedEntries(t, l, int(cut)+1, 100)
	compressAll(t, l)
	verifyEntries(t, l, 1, int(cut)+100)
}
