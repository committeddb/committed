package wal

// committeddb fork patch: sealed-segment zstd compression.
//
// Segments are immutable once the log cycles past them, which makes them the
// natural compression unit: the active tail is always PLAIN (the write path
// is untouched — no compression ever runs under a Write), and sealed
// segments compress out-of-band via CompressNextSealed, driven by the
// embedding storage's background sealer. A compressed segment keeps its
// 20-digit name with a ".zst" extension; mixed logs (plain + compressed
// segments) read transparently, so upgrades need no rewrite and the sweep
// compresses a pre-existing backlog incrementally.
//
// Crash safety: a segment compresses to "<name>.zst.SEAL" first (fsync'd),
// then renames to "<name>.zst" (dir fsync'd), then the plain file is
// removed. load() heals every interruption point: a leftover .SEAL is an
// incomplete seal (deleted; the plain segment is intact), and a plain file
// alongside its .zst is a completed seal whose cleanup was lost (the plain
// file is deleted; the .zst was durable before the rename).
//
// The read path decompresses inside loadSegmentEntriesLocked — the segment
// cache therefore holds DECOMPRESSED tables, so cache RAM sizing and every
// downstream read semantics are unchanged; only bytes at rest shrink. The
// zstd frame carries its own checksum, and the embedding storage's per-entry
// CRC framing rides INSIDE the compressed stream, so entry-level corruption
// attribution survives compression.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// CompressionCodec selects the sealed-segment compression for a log.
type CompressionCodec byte

const (
	// CompressionNone leaves sealed segments plain (the pre-fork format).
	CompressionNone CompressionCodec = 0
	// CompressionZstd compresses sealed segments with zstd at the default
	// level (level 3: measured ~7-9x on JSON-payload event logs at
	// >500 MB/s encode, >1 GB/s decode).
	CompressionZstd CompressionCodec = 1
)

const (
	zstExt     = ".zst"
	zstSealExt = ".zst.SEAL"
)

// Shared stateless codec instances: EncodeAll/DecodeAll on these are
// concurrency-safe, and constructing them per call would dominate small
// segment costs.
var (
	zstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDec, _ = zstd.NewReader(nil)
)

// ErrCompressionDisabled is returned by CompressNextSealed on a log opened
// without SealedSegmentCompression.
var ErrCompressionDisabled = errors.New("log opened without sealed-segment compression")

// compressSealedTestHook, when set (tests only), runs in the window between
// the lock-free encode and the swap lock — the window a concurrent
// truncation can rewrite the source segment in. It exists to pin the swap's
// identity guard deterministically.
var compressSealedTestHook func()

func isCompressedPath(path string) bool {
	return strings.HasSuffix(path, zstExt)
}

// maybeDecompressSegment returns the segment's entry bytes: the file content
// for a plain segment, the decoded stream for a compressed one. The zstd
// frame checksum makes torn/bit-flipped compressed segments fail loudly here.
func maybeDecompressSegment(path string, raw []byte) ([]byte, error) {
	if !isCompressedPath(path) {
		return raw, nil
	}
	out, err := zstdDec.DecodeAll(raw, nil)
	if err != nil {
		return nil, ErrCorrupt
	}
	return out, nil
}

// CompressNextSealed compresses the OLDEST plain sealed segment (never the
// active tail) and swaps it in place. It returns compressed=false when every
// sealed segment is already compressed — the embedding storage drives this
// in a background loop until done, which doubles as the day-one backfill
// sweep over a pre-compression backlog.
//
// The expensive work (read + encode + write + fsync of the .SEAL file) runs
// WITHOUT the log lock — sealed segments are immutable, so the source can't
// change underneath. Only the swap (rename + path update) takes the write
// lock, re-verifying the segment still exists at the same path (a concurrent
// truncation may have removed it — the temp file is discarded then).
func (l *Log) CompressNextSealed() (compressed bool, err error) {
	if l.opts.SealedSegmentCompression == CompressionNone {
		return false, ErrCompressionDisabled
	}

	// Pick the oldest plain sealed segment under the read lock.
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return false, ErrClosed
	}
	if l.corrupt {
		l.mu.RUnlock()
		return false, ErrCorrupt
	}
	var srcPath string
	var srcIndex uint64
	for _, s := range l.segments[:len(l.segments)-1] {
		if !isCompressedPath(s.path) {
			srcPath, srcIndex = s.path, s.index
			break
		}
	}
	l.mu.RUnlock()
	if srcPath == "" {
		return false, nil
	}

	// Capture the source file's identity BEFORE reading: the swap below must
	// prove the segment was not rewritten while we encoded without the lock
	// (a truncation rewrites a segment under the SAME name and index — an
	// (index, path) recheck alone would install a stale .zst that resurrects
	// truncated entries). atomicWrite-style rewrites create a new inode, so
	// os.SameFile plus the size is a complete identity check.
	preInfo, err := os.Stat(srcPath)
	if err != nil {
		return false, err
	}
	raw, err := os.ReadFile(srcPath)
	if err != nil {
		return false, err
	}
	enc := zstdEnc.EncodeAll(raw, make([]byte, 0, len(raw)/4))

	tmpPath := srcPath + zstSealExt // "<plain>.zst.SEAL"
	finalPath := srcPath + zstExt
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, l.opts.FilePerms)
	if err != nil {
		return false, err
	}
	if _, err := tmp.Write(enc); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return false, err
	}
	// The fsync is deliberately NOT gated on opts.NoSync: compression
	// REPLACES an already-durable plain segment (which is removed after the
	// rename), so a torn .zst here is data loss, not a lost unacked append —
	// the one thing the NoSync contract never waives. The scrub's NoSync
	// rewrite log relies on this: its pre-swap compression drain must hand
	// the swap durable files.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return false, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}

	if compressSealedTestHook != nil {
		compressSealedTestHook()
	}

	// Swap under the write lock: the rename is the commit point.
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		_ = os.Remove(tmpPath)
		return false, ErrClosed
	}
	var target *segment
	for _, s := range l.segments[:len(l.segments)-1] {
		if s.index == srcIndex && s.path == srcPath {
			target = s
			break
		}
	}
	if target != nil {
		// The name and index alone don't prove content identity: a
		// truncation rewrites a segment under the same name (new inode,
		// different content). Require the very file we encoded.
		curInfo, serr := os.Stat(srcPath)
		if serr != nil || !os.SameFile(preInfo, curInfo) || curInfo.Size() != preInfo.Size() {
			target = nil
		}
	}
	if target == nil {
		// A truncation removed or rewrote the segment while we encoded —
		// discard our work; the next call re-picks (and re-encodes the
		// rewritten content).
		_ = os.Remove(tmpPath)
		return true, nil
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}
	if !l.opts.NoSync {
		if err := syncDir(l.path); err != nil {
			return false, err
		}
	}
	target.path = finalPath
	// Cleanup of the plain file is best-effort: if lost to a crash, load()
	// deletes it on the next open (the .zst existing is the commit).
	_ = os.Remove(srcPath)
	return true, nil
}

// DecompressDir rewrites every compressed segment in a CLOSED log directory
// back to the plain format — the offline downgrade door: run this (via
// `committed wal decompress`) before starting a pre-compression binary
// against the data dir, because older builds do not recognize ".zst"
// segments. Returns how many segments were rewritten. Leftover ".zst.SEAL"
// temporaries are removed. Must not run against a live log.
func DecompressDir(dir string) (int, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(name, zstSealExt) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return n, err
			}
			continue
		}
		if len(name) != 20+len(zstExt) || !strings.HasSuffix(name, zstExt) {
			continue
		}
		src := filepath.Join(dir, name)
		raw, err := os.ReadFile(src)
		if err != nil {
			return n, err
		}
		plain, err := zstdDec.DecodeAll(raw, nil)
		if err != nil {
			return n, ErrCorrupt
		}
		plainPath := filepath.Join(dir, strings.TrimSuffix(name, zstExt))
		tmpPath := plainPath + ".DECOMP"
		tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
		if err != nil {
			return n, err
		}
		if _, err := tmp.Write(plain); err != nil {
			_ = tmp.Close()
			return n, err
		}
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return n, err
		}
		if err := tmp.Close(); err != nil {
			return n, err
		}
		if err := os.Rename(tmpPath, plainPath); err != nil {
			return n, err
		}
		if err := os.Remove(src); err != nil {
			return n, err
		}
		if err := syncDir(dir); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
