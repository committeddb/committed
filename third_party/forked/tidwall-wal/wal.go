package wal

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tinylru"
)

var (
	// ErrCorrupt is returns when the log is corrupt.
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is returned when an operation cannot be completed because
	// the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrNotFound is returned when an entry is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrOutOfRange is returned from TruncateFront() and TruncateBack() when
	// the index not in the range of the log's first and last index. Or, this
	// may be returned when the caller is attempting to remove *all* entries;
	// The log requires that at least one entry exists following a truncate.
	ErrOutOfRange = errors.New("out of range")

	// ErrEmptyLog is returned by Open() when the `AllowEmpty` option was not
	// provided and log has been emptied due to the use of TruncateFront() or
	// TruncateBack().
	ErrEmptyLog = errors.New("empty log")
)

// LogFormat is the format of the log files.
type LogFormat byte

const (
	// Binary format writes entries in binary. This is the default and, unless
	// a good reason otherwise, should be used in production.
	Binary LogFormat = 0
	// JSON format writes entries as JSON lines. This causes larger, human
	// readable files.
	JSON LogFormat = 1
)

// Options for Log
type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default 20 MB
	SegmentSize int
	// LogFormat is the format of the log files. Default Binary
	LogFormat LogFormat
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default 1
	SegmentCacheSize int
	// NoCopy allows for the Read() operation to return the raw underlying data
	// slice. This is an optimization to help minimize allocations. When this
	// option is set, do not modify the returned data because it may affect
	// other Read calls. Default false
	NoCopy bool
	// AllowEmpty allows for a log to have all entries removed through the use
	// of TruncateFront() or TruncateBack(). Otherwise without this option,
	// at least one entry must always remain following a truncate operation.
	// Default false
	//
	// Warning: using this option changes the behavior of the log in the
	// following ways:
	// - An empty log will always have the FirstIndex() be equal to
	//   LastIndex()+1.
	// - For a newly created log that has no entries, FirstIndex() and
	//   LastIndex() return 1 and 0, respectively.
	//   Without AllowEmpty, both return 0.
	AllowEmpty bool
	// Perms represents the datafiles modes and permission bits
	DirPerms  os.FileMode
	FilePerms os.FileMode
	// SealedSegmentCompression compresses segments once the log cycles past
	// them (the active tail always stays plain). committeddb fork patch —
	// see compress.go for the format, crash-safety, and downgrade story.
	// Default CompressionNone preserves the pre-fork on-disk format.
	SealedSegmentCompression CompressionCodec
}

// DefaultOptions for Open().
var DefaultOptions = &Options{
	NoSync:           false,    // Fsync after every write
	SegmentSize:      20971520, // 20 MB log segment files
	LogFormat:        Binary,   // Binary format is small and fast
	SegmentCacheSize: 2,        // Number of cached in-memory segments
	NoCopy:           false,    // Make a new copy of data for every Read call
	AllowEmpty:       false,    // Do not allow empty log. 1+ entries required
	DirPerms:         0750,     // Permissions for the created directories
	FilePerms:        0640,     // Permissions for the created data files
}

// Log represents a write ahead log
type Log struct {
	mu         sync.RWMutex
	path       string      // absolute path to log directory
	opts       Options     // log options
	closed     bool        // log is closed
	corrupt    bool        // log may be corrupt
	segments   []*segment  // all known log segments
	firstIndex uint64      // index of the first entry in log
	lastIndex  uint64      // index of the last entry in log
	sfile      *os.File    // tail segment file handle
	wbatch     Batch       // reusable write batch
	scache     tinylru.LRU // segment entries cache
	// committeddb fork patch: count of segment-file parses since Open.
	// Test observability for the cache policy (see SegmentLoads); atomic
	// because the read path parses under a segment dmu, not the log mu.
	loads atomic.Int64
}

// segment represents a single segment file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	// committeddb fork patch: dmu guards ebuf/epos against the concurrent-read
	// data race. Read runs under the LOG's shared lock (mu.RLock — many readers
	// at once), but the read path itself MUTATES shared segment state: lazily
	// loading a segment's table, and nil-ing an EVICTED segment's table to keep
	// the cache bounded. Two concurrent readers therefore raced — one's eviction
	// destroyed the table another was mid-indexing, panicking the process
	// ("index out of range [k] with length 0"; crashed a production node under
	// its first-ever concurrent readers). Every read-path touch of ebuf/epos now
	// holds dmu: shared to check+use, exclusive to load or clear. Writer-side
	// touches (writeBatch/cycle on the tail, truncations, clearCache) run under
	// mu.Lock, which already excludes all readers, and so stay direct. Lock
	// order is always mu BEFORE dmu, one segment's dmu at a time — no nesting,
	// no deadlock. An eviction victim is never the segment being inserted.
	dmu  sync.RWMutex
	ebuf []byte // cached entries buffer
	epos []bpos // cached entries positions in buffer
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

// Open a new write ahead log
func Open(path string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.SegmentCacheSize <= 0 {
		opts.SegmentCacheSize = DefaultOptions.SegmentCacheSize
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultOptions.SegmentSize
	}
	if opts.DirPerms == 0 {
		opts.DirPerms = DefaultOptions.DirPerms
	}
	if opts.FilePerms == 0 {
		opts.FilePerms = DefaultOptions.FilePerms
	}

	var err error
	path, err = abs(path)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, opts: *opts}
	l.scache.Resize(l.opts.SegmentCacheSize)
	if err := os.MkdirAll(path, l.opts.DirPerms); err != nil {
		return nil, err
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func abs(path string) (string, error) {
	if path == ":memory:" {
		return "", errors.New("in-memory log not supported")
	}
	return filepath.Abs(path)
}

// syncDir fsyncs a directory's entry list so a segment-file create/rename inside
// it is durable. committeddb fork patch: upstream fsyncs segment *content* on
// every write but never the parent *directory*, so a newly created segment's
// directory entry can be lost on power loss while its already-fsync'd contents
// survive — silently dropping just-acked entries (P<R on restart). This mirrors
// etcd's WAL, which fileutil.Fsync's the dir on every segment cut. Callers gate
// it on !opts.NoSync (a fsync the NoSync contract opts out of) and RETURN the
// error, because a non-durable directory entry breaks the durability promise the
// acked write just made — it must abort, not be tolerated.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// SegmentCacheSize reports the resolved segment-cache capacity this log was
// opened with. committeddb fork patch: added so the embedding storage can
// assert its configured cache size survives every (re)open path — the scrub
// swap reopens the event log in-process, and an option dropped there would
// silently revert the cache until restart.
func (l *Log) SegmentCacheSize() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.opts.SegmentCacheSize
}

// SegmentLoads reports how many segment-file parses have run since Open.
// committeddb fork patch: test observability for the cache policy — the
// loaded-segment read fast path must not parse, and recency is
// LRU-by-load, so a re-parse is the observable signature of an eviction.
func (l *Log) SegmentLoads() int64 {
	return l.loads.Load()
}

func (l *Log) pushCache(segIdx int) {
	_, _, _, v, evicted :=
		l.scache.SetEvicted(segIdx, l.segments[segIdx])
	if evicted {
		s := v.(*segment)
		// committeddb fork patch: clearing an evicted segment's table requires
		// its exclusive lock — this runs on the READ path (under the log's
		// shared lock), so without it this write races other readers of s and
		// destroys a table mid-use (see segment.dmu). Waits until no reader
		// holds the table; a reader arriving after simply reloads.
		s.dmu.Lock()
		s.ebuf = nil
		s.epos = nil
		s.dmu.Unlock()
	}
}

// load all the segments. This operation also cleans up any START/END segments.
func (l *Log) load() error {
	fis, err := ioutil.ReadDir(l.path)
	if err != nil {
		return err
	}
	startIdx := -1
	endIdx := -1
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() || len(name) < 20 {
			continue
		}
		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}
		// committeddb fork patch (compression crash healing): a leftover
		// ".zst.SEAL" is an incomplete seal — the plain segment is intact,
		// so the temp is deleted and the sweep re-compresses later.
		if len(name) == 20+len(zstSealExt) && strings.HasSuffix(name, zstSealExt) {
			if err := os.Remove(filepath.Join(l.path, name)); err != nil {
				return err
			}
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		isZst := len(name) == 20+len(zstExt) && strings.HasSuffix(name, zstExt)
		if len(name) == 20 || isStart || isEnd || isZst {
			// committeddb fork patch: same-index coexistence healing. ReadDir
			// is lexicographic, so a same-index sibling was already appended
			// when the ".zst" arrives. Two crash windows produce one:
			//
			//   - plain + .zst: a completed seal whose plain-file cleanup was
			//     lost. The .zst was fsync-durable before its rename, so the
			//     PLAIN file is stale — replace it.
			//   - .START/.END + .zst: a truncation rewrote this segment and
			//     crashed before deleting the old compressed file. The marker
			//     is the newer truth — the .ZST is stale; deleting the marker
			//     would resurrect pre-truncation data.
			if isZst && len(l.segments) > 0 &&
				l.segments[len(l.segments)-1].index == index {
				prev := l.segments[len(l.segments)-1]
				if strings.HasSuffix(prev.path, ".START") || strings.HasSuffix(prev.path, ".END") {
					if err := os.Remove(filepath.Join(l.path, name)); err != nil {
						return err
					}
					continue
				}
				if err := os.Remove(prev.path); err != nil {
					return err
				}
				prev.path = filepath.Join(l.path, name)
				continue
			}
			if isStart {
				startIdx = len(l.segments)
			} else if isEnd && endIdx == -1 {
				endIdx = len(l.segments)
			}
			l.segments = append(l.segments, &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}
	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.firstIndex = 1
		l.lastIndex = 0
		l.sfile, err = os.OpenFile(l.segments[0].path,
			os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		if err != nil {
			return err
		}
		// committeddb fork patch: fsync the log directory so the genesis
		// segment's directory entry is durable before the first append into it
		// can be acked (see syncDir).
		if !l.opts.NoSync {
			return syncDir(l.path)
		}
		return nil
	}
	// Open existing log. Clean up log if START of END segments exists.
	if startIdx != -1 {
		if endIdx != -1 {
			// There should not be a START and END at the same time
			return ErrCorrupt
		}
		// Delete all files leading up to START
		for i := 0; i < startIdx; i++ {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[startIdx:]...)
		// Rename the START segment
		orgPath := l.segments[0].path
		finalPath := orgPath[:len(orgPath)-len(".START")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[0].path = finalPath
	}
	if endIdx != -1 {
		// Delete all files following END
		for i := len(l.segments) - 1; i > endIdx; i-- {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[:endIdx+1]...)
		if len(l.segments) > 1 && l.segments[len(l.segments)-2].index ==
			l.segments[len(l.segments)-1].index {
			// remove the segment prior to the END segment because it shares
			// the same starting index.
			l.segments[len(l.segments)-2] = l.segments[len(l.segments)-1]
			l.segments = l.segments[:len(l.segments)-1]
		}
		// Rename the END segment
		orgPath := l.segments[len(l.segments)-1].path
		finalPath := orgPath[:len(orgPath)-len(".END")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[len(l.segments)-1].path = finalPath
	}
	l.firstIndex = l.segments[0].index
	// committeddb fork patch: the tail must be appendable, so it is always
	// plain — but the last on-disk segment can be compressed (a truncation or
	// manual surgery removed the plain tail, or every segment predates a
	// restore). Load its entries to establish lastIndex, then start a fresh
	// plain tail after it, mirroring the genesis path.
	if isCompressedPath(l.segments[len(l.segments)-1].path) {
		cseg := l.segments[len(l.segments)-1]
		if err := l.loadSegmentEntries(cseg); err != nil {
			return err
		}
		lastIndex := cseg.index + uint64(len(cseg.epos)) - 1
		nseg := &segment{
			index: lastIndex + 1,
			path:  filepath.Join(l.path, segmentName(lastIndex+1)),
		}
		l.sfile, err = os.OpenFile(nseg.path,
			os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		if err != nil {
			return err
		}
		if !l.opts.NoSync {
			if err := syncDir(l.path); err != nil {
				return err
			}
		}
		l.segments = append(l.segments, nseg)
		l.lastIndex = lastIndex
		return nil
	}
	// Open the last segment for appending
	lseg := l.segments[len(l.segments)-1]
	l.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, l.opts.FilePerms)
	if err != nil {
		return err
	}
	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}
	// Load the last segment entries
	if err := l.loadSegmentEntries(lseg); err != nil {
		return err
	}
	l.lastIndex = lseg.index + uint64(len(lseg.epos)) - 1
	if l.lastIndex > 0 && l.firstIndex > l.lastIndex && !l.opts.AllowEmpty {
		return ErrEmptyLog
	}
	return nil
}

// segmentName returns a 20-byte textual representation of an index
// for lexical ordering. This is used for the file names of log segments.
func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

// Close the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if l.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return ErrCorrupt
	}
	return nil
}

// Write an entry to the log.
func (l *Log) Write(index uint64, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	l.wbatch.Clear()
	l.wbatch.Write(index, data)
	return l.writeBatch(&l.wbatch)
}

func (l *Log) appendEntry(dst []byte, index uint64, data []byte) (out []byte,
	epos bpos) {
	if l.opts.LogFormat == JSON {
		return appendJSONEntry(dst, index, data)
	}
	return appendBinaryEntry(dst, data)
}

// Cycle the old segment for a new segment.
func (l *Log) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	// cache the previous segment
	l.pushCache(len(l.segments) - 1)
	s := &segment{
		index: l.lastIndex + 1,
		path:  filepath.Join(l.path, segmentName(l.lastIndex+1)),
	}
	var err error
	l.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC,
		l.opts.FilePerms)
	if err != nil {
		return err
	}
	// committeddb fork patch: fsync the log directory so the newly created
	// segment's directory entry is durable before the append into it (whose
	// content fsync follows in writeBatch) is acked. Without this, a power loss
	// after a ~SegmentSize cycle keeps the content-fsync'd new segment on disk
	// but its unpersisted directory entry vanishes, silently losing the acked
	// entries it holds. Mirrors etcd's WAL cut(); see syncDir.
	if !l.opts.NoSync {
		if err := syncDir(l.path); err != nil {
			return err
		}
	}
	l.segments = append(l.segments, s)
	return nil
}

func appendJSONEntry(dst []byte, index uint64, data []byte) (out []byte,
	epos bpos) {
	// {"index":number,"data":string}
	pos := len(dst)
	dst = append(dst, `{"index":"`...)
	dst = strconv.AppendUint(dst, index, 10)
	dst = append(dst, `","data":`...)
	dst = appendJSONData(dst, data)
	dst = append(dst, '}', '\n')
	return dst, bpos{pos, len(dst)}
}

func appendJSONData(dst []byte, s []byte) []byte {
	if utf8.Valid(s) {
		b, _ := json.Marshal(*(*string)(unsafe.Pointer(&s)))
		dst = append(dst, '"', '+')
		return append(dst, b[1:]...)
	}
	dst = append(dst, '"', '$')
	dst = append(dst, base64.URLEncoding.EncodeToString(s)...)
	return append(dst, '"')
}

func appendBinaryEntry(dst []byte, data []byte) (out []byte, epos bpos) {
	// data_size + data
	pos := len(dst)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst, bpos{pos, len(dst)}
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

// Batch of entries. Used to write multiple entries at once using WriteBatch().
type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	index uint64
	size  int
}

// Write an entry to the batch
func (b *Batch) Write(index uint64, data []byte) {
	b.entries = append(b.entries, batchEntry{index, len(data)})
	b.datas = append(b.datas, data...)
}

// Clear the batch for reuse.
func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}

// WriteBatch writes the entries in the batch to the log in the order that they
// were added to the batch. The batch is cleared upon a successful return.
func (l *Log) WriteBatch(b *Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	if len(b.entries) == 0 {
		return nil
	}
	return l.writeBatch(b)
}

func (l *Log) writeBatch(b *Batch) error {
	// check that all indexes in batch are sane
	for i := 0; i < len(b.entries); i++ {
		if b.entries[i].index != l.lastIndex+uint64(i+1) {
			return ErrOutOfOrder
		}
	}
	// load the tail segment
	s := l.segments[len(l.segments)-1]
	if len(s.ebuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return err
		}
		s = l.segments[len(l.segments)-1]
	}

	mark := len(s.ebuf)
	datas := b.datas
	for i := 0; i < len(b.entries); i++ {
		data := datas[:b.entries[i].size]
		var epos bpos
		s.ebuf, epos = l.appendEntry(s.ebuf, b.entries[i].index, data)
		s.epos = append(s.epos, epos)
		if len(s.ebuf) >= l.opts.SegmentSize {
			// segment has reached capacity, cycle now
			if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
				return err
			}
			l.lastIndex = b.entries[i].index
			if err := l.cycle(); err != nil {
				return err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}
	if len(s.ebuf)-mark > 0 {
		if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
			return err
		}
		l.lastIndex = b.entries[len(b.entries)-1].index
	}
	if !l.opts.NoSync {
		if err := l.sfile.Sync(); err != nil {
			return err
		}
	}
	b.Clear()
	return nil
}

// FirstIndex returns the index of the first entry in the log.
// Returns zero when log has no entries.
// When using the `AllowEmpty` option and when the log is empty, this will
// return LastIndex+1, which is the next future index.
func (l *Log) FirstIndex() (index uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	if !l.opts.AllowEmpty && l.lastIndex == 0 {
		return 0, nil
	}
	return l.firstIndex, nil
}

// LastIndex returns the index of the last entry in the log.
// Returns zero when log has no entries.
// When using the `AllowEmpty` option and when the log is empty, this will
// return FirstIndex()-1, which is the last known deleted index.
func (l *Log) LastIndex() (index uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	if !l.opts.AllowEmpty && l.firstIndex == 0 {
		return 0, nil
	}
	return l.lastIndex, nil
}

// findSegment performs a bsearch on the segments
func (l *Log) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// loadSegmentEntries parses s's file and installs its table, taking the
// segment's exclusive lock for the caller. Exclusive-context callers (Open's
// load, truncations — all under the log's write lock) use this wrapper; the
// read path uses loadSegmentEntriesLocked under its own dmu handling
// (committeddb fork patch, see segment.dmu).
func (l *Log) loadSegmentEntries(s *segment) error {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	return l.loadSegmentEntriesLocked(s)
}

// loadSegmentEntriesLocked is loadSegmentEntries without the lock; the caller
// must hold s.dmu exclusively.
func (l *Log) loadSegmentEntriesLocked(s *segment) error {
	data, err := ioutil.ReadFile(s.path)
	if err != nil {
		return err
	}
	// committeddb fork patch: a compressed sealed segment decodes here, so
	// the cached table (ebuf/epos) always holds DECOMPRESSED bytes and every
	// downstream read path is format-oblivious.
	data, err = maybeDecompressSegment(s.path, data)
	if err != nil {
		return err
	}
	ebuf := data
	var epos []bpos
	var pos int
	for exidx := s.index; len(data) > 0; exidx++ {
		var n int
		if l.opts.LogFormat == JSON {
			n, err = loadNextJSONEntry(data)
		} else {
			n, err = loadNextBinaryEntry(data)
		}
		if err != nil {
			return err
		}
		data = data[n:]
		epos = append(epos, bpos{pos, pos + n})
		pos += n
	}
	s.ebuf = ebuf
	s.epos = epos
	l.loads.Add(1)
	return nil
}

func loadNextJSONEntry(data []byte) (n int, err error) {
	// {"index":number,"data":string}
	idx := bytes.IndexByte(data, '\n')
	if idx == -1 {
		return 0, ErrCorrupt
	}
	line := data[:idx]
	dres := gjson.Get(*(*string)(unsafe.Pointer(&line)), "data")
	if dres.Type != gjson.String {
		return 0, ErrCorrupt
	}
	return idx + 1, nil
}

func loadNextBinaryEntry(data []byte) (n int, err error) {
	// data_size + data
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}

// loadSegment loads the segment entries into memory, pushes it to the front
// of the lru cache, and returns the segment plus an IMMUTABLE SNAPSHOT of its
// table (ebuf/epos), captured inside the same lock hold that populated or
// validated it.
//
// committeddb fork patch: callers must use the RETURNED ebuf/epos, never
// s.ebuf/s.epos. Concurrent readers race the segment cache — another reader's
// eviction can nil a segment's fields at any moment after this returns — and
// reading the fields directly is both a data race and the production panic
// ("index out of range [k] with length 0"). The snapshot closes the gap
// completely: loads always publish freshly-built arrays and eviction only nils
// the FIELDS, so the arrays behind the returned slices are immutable and kept
// alive by the references — an eviction after capture is harmless, and a
// reader can never lose its table mid-use. No retry needed, no spurious
// failure possible. (An earlier revision returned only the segment and had
// Read re-check-and-retry; under heavy eviction churn the retry bound
// exhausted and misreported a HEALTHY log as corrupt.)
func (l *Log) loadSegment(index uint64) (s *segment, ebuf []byte, epos []bpos, err error) {
	// check the last segment first.
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		lseg.dmu.RLock()
		ebuf, epos = lseg.ebuf, lseg.epos
		lseg.dmu.RUnlock()
		return lseg, ebuf, epos, nil
	}
	// check the most recent cached segment
	var rseg *segment
	l.scache.Range(func(_, v interface{}) bool {
		c := v.(*segment)
		c.dmu.RLock()
		if index >= c.index && index < c.index+uint64(len(c.epos)) {
			rseg, ebuf, epos = c, c.ebuf, c.epos
		}
		c.dmu.RUnlock()
		return false
	})
	if rseg != nil {
		return rseg, ebuf, epos, nil
	}
	// find in the segment array
	idx := l.findSegment(index)
	s = l.segments[idx]
	// committeddb fork patch: already-loaded fast path under the segment's
	// READ lock, mutating nothing. The original flow took the exclusive dmu
	// AND pushed to the cache (tinylru's exclusive lock) on EVERY read —
	// two global serialization points per entry that collapsed N concurrent
	// replaying readers to ~1× single-reader throughput (measured: 17
	// readers aggregated 1.1× one reader; with this fast path, 3.5×).
	// Correctness matches the cached-segment path above: ebuf/epos are
	// captured under dmu (eviction nils them under dmu.Lock, so no torn
	// view), the arrays are immutable once published, and tiling validation
	// ran when the segment was populated — loaded implies validated.
	//
	// Deliberate trade-off: pushCache now runs only on populate (below), so
	// cache recency is LRU-by-load, not LRU-by-access. A straggler reader
	// camped in one segment no longer refreshes it and can see it evicted
	// after cache-size foreign loads, re-parsing it on the next read
	// (~tens of ms, page-cache-backed) — a bounded, rare cost accepted to
	// remove the per-entry serializers. TestSegmentCacheEvictsByLoadOrder
	// pins the policy.
	s.dmu.RLock()
	if len(s.epos) != 0 {
		ebuf, epos = s.ebuf, s.epos
		s.dmu.RUnlock()
		return s, ebuf, epos, nil
	}
	s.dmu.RUnlock()
	// Double-checked load under the segment's exclusive lock — concurrent
	// readers needing the same segment must not race the populate, and only
	// one of them parses the file; the rest wait and find it loaded.
	s.dmu.Lock()
	if len(s.epos) == 0 {
		// load the entries from cache
		if err := l.loadSegmentEntriesLocked(s); err != nil {
			s.dmu.Unlock()
			return nil, nil, nil, err
		}
		// committeddb fork patch: validate segment tiling. The binary format
		// stores no per-entry index — an entry's identity is pure arithmetic
		// (filename start + position within the file) — so a non-tail segment
		// file must parse to EXACTLY the entry count the next segment's start
		// implies. Open validates only the tail (lastIndex comes from its
		// parse); middle segments were trusted blind, so a file that is empty
		// or truncated at an entry boundary parsed "successfully" short and
		// the epos indexing in Read panicked the process on the first cold
		// historical read (the post-scrub crashloop). A file parsing LONG is
		// equally corrupt — every later entry in it would be misindexed and
		// reads would silently return the wrong entry. Error, never panic:
		// disk content is untrusted input. Deliberately not setting l.corrupt
		// (the sticky flag would also poison appends and reads of healthy
		// segments; a hole in history must not stop the tail of the log).
		if idx+1 < len(l.segments) {
			if want := l.segments[idx+1].index - s.index; uint64(len(s.epos)) != want {
				// Discard the partial parse: leaving epos populated would let
				// the next read take the already-loaded branch, skip this
				// validation, and serve entries from a mis-tiled segment.
				s.ebuf, s.epos = nil, nil
				s.dmu.Unlock()
				return nil, nil, nil, ErrCorrupt
			}
		}
	}
	ebuf, epos = s.ebuf, s.epos
	s.dmu.Unlock()
	// push the segment to the front of the cache
	l.pushCache(idx)
	return s, ebuf, epos, nil
}

// Read an entry from the log. Returns a byte slice containing the data entry.
func (l *Log) Read(index uint64) (data []byte, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return nil, ErrCorrupt
	} else if l.closed {
		return nil, ErrClosed
	}
	if index < l.firstIndex || index > l.lastIndex {
		return nil, ErrNotFound
	}
	s, ebuf, epos, err := l.loadSegment(index)
	if err != nil {
		return nil, err
	}
	// committeddb fork patch: decode from the immutable snapshot loadSegment
	// captured under the segment's lock — never from s.ebuf/s.epos, which a
	// concurrent reader's cache eviction can nil at any moment (the production
	// panic). The coverage guard is defensive: eviction cannot shorten the
	// snapshot, so a miss here is a genuine in-memory desync (e.g. lastIndex
	// disagreeing with the tail's table) and surfaces as ErrCorrupt, never a
	// panic — file and memory content are untrusted input to the index math.
	if index-s.index >= uint64(len(epos)) {
		return nil, ErrCorrupt
	}
	e := epos[index-s.index]
	edata := ebuf[e.pos:e.end]
	if l.opts.LogFormat == JSON {
		return readJSON(edata)
	}
	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, ErrCorrupt
	}
	if l.opts.NoCopy {
		data = edata[n : uint64(n)+size]
	} else {
		data = make([]byte, size)
		copy(data, edata[n:])
	}
	return data, nil
}

//go:noinline
func readJSON(edata []byte) ([]byte, error) {
	var data []byte
	s := gjson.Get(*(*string)(unsafe.Pointer(&edata)), "data").String()
	if len(s) > 0 && s[0] == '$' {
		var err error
		data, err = base64.URLEncoding.DecodeString(s[1:])
		if err != nil {
			return nil, ErrCorrupt
		}
	} else if len(s) > 0 && s[0] == '+' {
		data = make([]byte, len(s[1:]))
		copy(data, s[1:])
	} else {
		return nil, ErrCorrupt
	}
	return data, nil
}

// ClearCache clears the segment cache.
// This only frees internal buffers and the LRU cache and does not modify the
// contents of the log.
func (l *Log) ClearCache() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	l.clearCache()
	return nil
}

func (l *Log) clearCache() {
	l.scache.Range(func(_, v interface{}) bool {
		s := v.(*segment)
		s.ebuf = nil
		s.epos = nil
		return true
	})
	l.scache = tinylru.LRU{}
	l.scache.Resize(l.opts.SegmentCacheSize)
}

// atomicWrite performs an temp write + rename to ensure the file writing is
// and atomic operation. One os.WriteFile alone is not good enough.
func (l *Log) atomicWrite(name string, data []byte) error {
	// Create a TEMP file
	tempName := name + ".TEMP"
	defer os.RemoveAll(tempName)
	if err := func() error {
		f, err := os.OpenFile(tempName, os.O_CREATE|os.O_RDWR|os.O_TRUNC,
			l.opts.FilePerms)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.Write(data); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return f.Close()
	}(); err != nil {
		return err
	}
	// Rename the TEMP file to final name
	return os.Rename(tempName, name)
}

// TruncateFront truncates the front of the log by removing all entries that
// are before the provided `index`. In other words the entry at `index` becomes
// the first entry in the log.
//
// The `AllowEmpty` option may be used to allow for removing all entries in the
// log by providing `LastIndex+1` as the index. Otherwise without `AllowEmpty`,
// at least one entry must always remain following a truncate.
func (l *Log) TruncateFront(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.truncateFront(index)
}

func (l *Log) truncateFront(index uint64) (err error) {
	if index < l.firstIndex || index > l.lastIndex+1 {
		return ErrOutOfRange
	}
	if !l.opts.AllowEmpty && index == l.lastIndex+1 {
		return ErrOutOfRange
	}
	if index == l.firstIndex {
		// nothing to truncate
		return nil
	}
	var segIdx int
	var s *segment
	var ebuf []byte
	if index == l.lastIndex+1 {
		// Truncate all entries, only care about the last segment
		segIdx = len(l.segments) - 1
		s = l.segments[segIdx]
		ebuf = nil
	} else {
		segIdx = l.findSegment(index)
		var sebuf []byte
		var sepos []bpos
		s, sebuf, sepos, err = l.loadSegment(index)
		if err != nil {
			return err
		}
		epos := sepos[index-s.index:]
		ebuf = sebuf[epos[0].pos:]
	}
	// Create a START file contains the truncated segment.
	startName := filepath.Join(l.path, segmentName(index)+".START")
	if err = l.atomicWrite(startName, ebuf); err != nil {
		return fmt.Errorf("failed to create start segment: %w", err)
	}
	// The log was truncated but still needs some file cleanup. Any errors
	// following this message will not cause an on-disk data ocorruption, but
	// may cause an inconsistency with the current program, so we'll return
	// ErrCorrupt so the the user can attempt a recover by calling Close()
	// followed by Open().
	defer func() {
		if v := recover(); v != nil {
			err = ErrCorrupt
		}
		if err != nil {
			l.corrupt = true
		}
	}()
	if segIdx == len(l.segments)-1 {
		// Close the tail segment file
		if err = l.sfile.Close(); err != nil {
			return err
		}
	}
	// Delete truncated segment files
	for i := 0; i <= segIdx; i++ {
		if err = os.Remove(l.segments[i].path); err != nil {
			return err
		}
	}
	// Rename the START file to the final truncated segment name.
	newName := filepath.Join(l.path, segmentName(index))
	if err = os.Rename(startName, newName); err != nil {
		return err
	}
	// committeddb fork patch: fsync the log directory so the segment removals
	// and the START->final rename above are durable before returning success
	// (see syncDir). A failure marks the log corrupt via the deferred recover.
	if !l.opts.NoSync {
		if err = syncDir(l.path); err != nil {
			return err
		}
	}
	s.path = newName
	s.index = index
	if segIdx == len(l.segments)-1 {
		// Reopen the tail segment file
		l.sfile, err = os.OpenFile(newName, os.O_WRONLY, l.opts.FilePerms)
		if err != nil {
			return err
		}
		var n int64
		if n, err = l.sfile.Seek(0, 2); err != nil {
			return err
		}
		if n != int64(len(ebuf)) {
			err = errors.New("invalid seek")
			return err
		}
		// Load the last segment entries
		if err = l.loadSegmentEntries(s); err != nil {
			return err
		}
	}
	l.segments = append([]*segment{}, l.segments[segIdx:]...)
	l.firstIndex = index
	l.clearCache()
	return nil
}

// TruncateBack truncates the back of the log by removing all entries that
// are after the provided `index`. In other words the entry at `index` becomes
// the last entry in the log.
//
// The `AllowEmpty` option may be used to allow for removing all entries in the
// log by providing `FirstIndex()-1` as the index. Otherwise without
// `AllowEmpty`, at least one entry must always remain following a truncate.
func (l *Log) TruncateBack(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.truncateBack(index)
}

func (l *Log) truncateBack(index uint64) (err error) {
	if index < l.firstIndex-1 || index > l.lastIndex {
		return ErrOutOfRange
	}
	if !l.opts.AllowEmpty && index == l.firstIndex-1 {
		return ErrOutOfRange
	}
	if index == l.lastIndex {
		// nothing to truncate
		return nil
	}
	var segIdx int
	var s *segment
	var ebuf []byte
	if index == l.firstIndex-1 {
		// Truncate all entries, only care about the first segment
		segIdx = 0
		s = l.segments[segIdx]
		ebuf = nil
	} else {
		segIdx = l.findSegment(index)
		var sebuf []byte
		var sepos []bpos
		s, sebuf, sepos, err = l.loadSegment(index)
		if err != nil {
			return err
		}
		epos := sepos[:index-s.index+1]
		ebuf = sebuf[:epos[len(epos)-1].end]
	}
	// Create an END file contains the truncated segment.
	endName := filepath.Join(l.path, segmentName(s.index)+".END")
	if err = l.atomicWrite(endName, ebuf); err != nil {
		return fmt.Errorf("failed to create end segment: %w", err)
	}
	// The log was truncated but still needs some file cleanup. Any errors
	// following this message will not cause an on-disk data ocorruption, but
	// may cause an inconsistency with the current program, so we'll return
	// ErrCorrupt so the the user can attempt a recover by calling Close()
	// followed by Open().
	defer func() {
		if v := recover(); v != nil {
			err = ErrCorrupt
		}
		if err != nil {
			l.corrupt = true
		}
	}()

	// Close the tail segment file
	if err = l.sfile.Close(); err != nil {
		return err
	}
	// Delete truncated segment files
	for i := segIdx; i < len(l.segments); i++ {
		if err = os.Remove(l.segments[i].path); err != nil {
			return err
		}
	}
	// Rename the END file to the final truncated segment name.
	newName := filepath.Join(l.path, segmentName(s.index))
	if err = os.Rename(endName, newName); err != nil {
		return err
	}
	// committeddb fork patch: fsync the log directory so the segment removals
	// and the END->final rename above are durable before returning success
	// (see syncDir). A failure marks the log corrupt via the deferred recover.
	if !l.opts.NoSync {
		if err = syncDir(l.path); err != nil {
			return err
		}
	}
	// Reopen the tail segment file
	l.sfile, err = os.OpenFile(newName, os.O_WRONLY, l.opts.FilePerms)
	if err != nil {
		return err
	}
	var n int64
	n, err = l.sfile.Seek(0, 2)
	if err != nil {
		return err
	}
	if n != int64(len(ebuf)) {
		err = errors.New("invalid seek")
		return err
	}
	s.path = newName
	l.segments = append([]*segment{}, l.segments[:segIdx+1]...)
	l.lastIndex = index
	l.clearCache()
	if err = l.loadSegmentEntries(s); err != nil {
		return err
	}
	return nil
}

// Sync performs an fsync on the log. This is not necessary when the
// NoSync option is set to false.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.sfile.Sync()
}

// IsEmpty returns true if there are no entries in the log.
func (l *Log) IsEmpty() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return false, ErrCorrupt
	} else if l.closed {
		return false, ErrClosed
	}
	return (l.firstIndex == 0 && l.lastIndex == 0) ||
		l.firstIndex > l.lastIndex, nil
}
