package wal

import "os"

// committeddb fork patch: a consistent view of the log's files for a
// peer-to-peer fetch or a live backup — the log owns its segment files and
// serializes writes under l.mu, so it is the one place that can list the
// sealed segments, the tail file, and the tail's committed length atomically.

// SegmentFile is one on-disk segment as of a LayoutSnapshot: its path
// (plain or compressed, as it is on disk) and the index of its first entry.
type SegmentFile struct {
	Path  string
	Index uint64
}

// Layout is a consistent view of the log's files. Sealed segments never
// change in place (compression swaps a plain file for its .zst twin, which
// a caller holds off by other means); the tail's bytes below TailLen never
// change either — later writes only append past it. A tail that rolls to a
// new segment after the snapshot is simply a sealed segment the snapshot
// listed as the tail, still valid up to TailLen.
type Layout struct {
	Sealed    []SegmentFile
	Tail      SegmentFile
	TailLen   int64 // bytes of the tail file committed at the snapshot
	LastIndex uint64
}

// LayoutSnapshot returns the log's layout, taken under the write lock so it
// is consistent with a completed write batch: every byte the tail file holds
// at TailLen was written by a batch that finished before the snapshot.
func (l *Log) LayoutSnapshot() (Layout, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return Layout{}, ErrClosed
	}
	if l.corrupt {
		return Layout{}, ErrCorrupt
	}
	n := len(l.segments)
	lay := Layout{Sealed: make([]SegmentFile, 0, n-1), LastIndex: l.lastIndex}
	for _, s := range l.segments[:n-1] {
		lay.Sealed = append(lay.Sealed, SegmentFile{Path: s.path, Index: s.index})
	}
	tail := l.segments[n-1]
	lay.Tail = SegmentFile{Path: tail.path, Index: tail.index}
	// The tail's in-memory buffer is the file's content: writeBatch appends to
	// the buffer and writes the same bytes to the file under l.mu, so under the
	// read lock the two lengths agree. The stat is a belt-and-braces check
	// that never reports more than is on disk.
	lay.TailLen = int64(len(tail.ebuf))
	if fi, err := os.Stat(tail.path); err == nil && fi.Size() < lay.TailLen {
		lay.TailLen = fi.Size()
	}
	return lay, nil
}

// IsCompressedSegmentPath reports whether a segment path names a compressed
// (.zst) segment.
func IsCompressedSegmentPath(path string) bool { return isCompressedPath(path) }
