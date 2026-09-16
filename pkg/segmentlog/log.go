package segmentlog

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/committeddb/committed/pkg/segmentlog/internal/durablefs"
	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

var (
	ErrLocked      = durablefs.ErrLocked
	ErrLogPoisoned = errors.New("segmentlog: log requires recovery after I/O failure")
	ErrClosed      = errors.New("segmentlog: log is closed")
)

// LogOptions configures creation. The rotation target is persisted in the
// catalog; encoding policy may change on reopen without rewriting existing files.
type LogOptions struct {
	// SegmentBytes targets original framed record bytes, excluding append-group
	// overhead. Zero selects 20 MiB. Maximum is 32 MiB. Oversized records stand alone.
	SegmentBytes int
	Encoding     Options
}

type fileInstaller interface {
	Install(string, func(io.Writer) error) (durablefs.Result, error)
}

// Log integrates a catalog, one active tail, and synchronous segment sealing.
// It holds an advisory directory lock across processes and instances until Close.
// Methods serialize; this prototype blocks reads/appends while sealing. The caller
// must not replace the directory or bypass ownership with lower-level writers.
// Rewrite publishes whole-log transformations; RewriteSealed limits their scope.
// Reclaim cleans obsolete managed files. Retirement for future pinned views
// remains pending.
type Log struct {
	mu       sync.Mutex
	path     string
	dir      fileInstaller
	remover  fileRemover
	catalog  *CatalogStore
	file     *os.File
	tail     *Tail
	framed   uint64
	encoding Options
	poison   error
	closed   bool
	lock     *durablefs.DirectoryLock
}

func checkLogEncoding(target uint64, encoding Options) error {
	if target < format.FrameOverhead || target > maxGroupBytes {
		return ErrInvalid
	}
	blockSize := encoding.BlockSize
	if blockSize == 0 {
		blockSize = 256 << 10
	}
	// A non-final block consumes >half its target (or holds one oversized
	// record). Ensure even adverse record packing fits the block-index limit.
	if blockSize < format.FrameOverhead || blockSize > format.MaxBlock || target > uint64(blockSize/2)*uint64(format.MaxBlocks-1) {
		return ErrInvalid
	}
	return WriteSegment(io.Discard, Coverage{0, 1}, func(yield func(Record, error) bool) {}, encoding)
}

// CreateLog initializes an empty, existing directory. The directory and its
// parents must already be durable. Failed initialization can leave artifacts;
// it never deletes existing files or silently reinitializes them.
func CreateLog(path string, start uint64, opts LogOptions) (result *Log, retErr error) {
	target := opts.SegmentBytes
	if target == 0 {
		target = 20 << 20
	}
	if target < 0 || start == ^uint64(0) {
		return nil, ErrInvalid
	}
	if err := checkLogEncoding(uint64(target), opts.Encoding); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	lock, err := durablefs.Lock(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result == nil {
			retErr = errors.Join(retErr, lock.Close())
		}
	}()
	dir, err := durablefs.Open(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, ErrCatalogConflict
	}
	var history [16]byte
	if _, err = rand.Read(history[:]); err != nil {
		return nil, err
	}
	name, err := uniqueName("tail", start, ".active")
	if err != nil {
		return nil, err
	}
	if _, err = dir.Install(name, func(w io.Writer) error { return WriteTailHeader(w, start) }); err != nil {
		return nil, err
	}
	c := Catalog{History: history, Revision: 1, Start: start, SegmentBytes: uint64(target), Active: &TailRef{File: name, Start: start}}
	store, err := CreateCatalogStore(path, c)
	if err != nil {
		return nil, err
	}
	result, retErr = attachLog(path, dir, store, opts.Encoding)
	if result != nil {
		result.lock = lock
	}
	return result, retErr
}

// OpenLog follows CURRENT and verifies all referenced history. It refuses
// incomplete tails without truncation. It ignores unreferenced artifacts and
// leaves them for future retirement/recovery policy. Encoding affects future
// sealing only. Catalogs without a managed rotation target are not adopted.
func OpenLog(path string, encoding Options) (result *Log, retErr error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	lock, err := durablefs.Lock(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result == nil {
			retErr = errors.Join(retErr, lock.Close())
		}
	}()
	dir, err := durablefs.Open(path)
	if err != nil {
		return nil, err
	}
	store, err := OpenCatalogStore(path)
	if err != nil {
		return nil, err
	}
	result, retErr = attachLog(path, dir, store, encoding)
	if result != nil {
		result.lock = lock
	}
	return result, retErr
}

func attachLog(path string, dir *durablefs.Dir, store *CatalogStore, encoding Options) (*Log, error) {
	c, err := store.Current()
	if err != nil {
		return nil, err
	}
	if c.SegmentBytes == 0 || c.Active == nil {
		return nil, ErrUnsupported
	}
	if err = checkLogEncoding(c.SegmentBytes, encoding); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(path, c.Active.File), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	tail, err := openTail(file, c.Active.Checkpoint)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	state, err := tail.State()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	framed := state.Framed

	// A managed tail can exceed the target only with one oversized record.
	if framed > c.SegmentBytes && (state.OriginalCount != 1 || framed > format.MaxPayload+format.FrameOverhead) {
		_ = file.Close()
		return nil, ErrCorrupt
	}
	return &Log{path: path, dir: dir, remover: dir, catalog: store, file: file, tail: tail, framed: framed, encoding: encoding}, nil
}

func uniqueName(prefix string, start uint64, suffix string) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%020d-%x%s", prefix, start, id, suffix), nil
}

func (l *Log) usable() error {
	if l.closed {
		return ErrClosed
	}
	return l.poison
}
func (l *Log) fail(err error) error { l.poison = errors.Join(ErrLogPoisoned, err); return l.poison }

// Append validates the whole input before writing. Groups are synced before
// success, rotating before the next record would exceed the persisted target.
// A batch spanning segments is not an atomic transaction: on I/O failure, some
// prefix may be durable. The handle is poisoned; reopen and reconcile stable IDs
// before replaying. IDs must strictly increase, including across calls/restarts.
func (l *Log) Append(records []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return err
	}
	if len(records) == 0 {
		return ErrInvalid
	}
	state, err := l.tail.State()
	if err != nil {
		return l.fail(err)
	}
	prev, has := state.Last, state.HasRecords
	for _, r := range records {
		if r.ID < state.Start || r.ID == ^uint64(0) || (has && r.ID <= prev) || len(r.Payload) > format.MaxPayload {
			return ErrInvalid
		}
		prev, has = r.ID, true
	}
	c, err := l.catalog.Current()
	if err != nil {
		return l.fail(err)
	}
	for i := 0; i < len(records); {
		size := uint64(len(records[i].Payload) + format.FrameOverhead)
		if l.framed > 0 && (l.framed >= c.SegmentBytes || size > c.SegmentBytes-l.framed) {
			if err := l.rotate(); err != nil {
				return l.fail(err)
			}
		}
		end := i
		bytes := l.framed
		for end < len(records) {
			n := uint64(len(records[end].Payload) + format.FrameOverhead)
			if end > i && (bytes >= c.SegmentBytes || n > c.SegmentBytes-bytes) {
				break
			}
			bytes += n
			end++
			if bytes >= c.SegmentBytes {
				break
			}
		}
		if err := l.tail.Append(records[i:end]); err != nil {
			return l.fail(err)
		}
		l.framed = bytes
		i = end
	}
	return nil
}

var errStopScan = errors.New("segmentlog: stop internal scan")

func tailRecords(file io.ReaderAt, end int64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		stopped := false
		_, err := ScanTail(file, end, func(r Record) error {
			if !yield(r, nil) {
				stopped = true
				return errStopScan
			}
			return nil
		})
		if err != nil && !stopped {
			yield(Record{}, err)
		}
	}
}

// rotate runs under the log mutex. It keeps the old catalog/tail authoritative
// until both the sealed replacement and new empty tail are durably installed.
func (l *Log) rotate() error {
	state, err := l.tail.State()
	if err != nil {
		return err
	}
	if !state.HasRecords {
		return ErrInvalid
	}
	c, err := l.catalog.Current()
	if err != nil {
		return err
	}
	if c.Revision == ^uint64(0) || len(c.Segments) >= format.MaxBlocks {
		return ErrInvalid
	}
	coverage := Coverage{Start: state.Start, End: state.Last + 1}
	ref := SegmentRef{Coverage: coverage}
	if state.Count > 0 {
		name, err := uniqueName("segment", coverage.Start, ".seg")
		if err != nil {
			return err
		}
		hash := sha256.New()
		if _, err = l.dir.Install(name, func(w io.Writer) error {
			return WriteSegment(io.MultiWriter(w, hash), coverage, tailRecords(l.file, state.End), l.encoding)
		}); err != nil {
			return err
		}
		ref.File, ref.Count = name, state.Count
		copy(ref.SHA256[:], hash.Sum(nil))
	}
	newName, err := uniqueName("tail", coverage.End, ".active")
	if err != nil {
		return err
	}
	if _, err = l.dir.Install(newName, func(w io.Writer) error { return WriteTailHeader(w, coverage.End) }); err != nil {
		return err
	}
	newFile, err := os.OpenFile(filepath.Join(l.path, newName), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	newTail, err := OpenTail(newFile)
	if err != nil {
		return errors.Join(err, newFile.Close())
	}
	c.Revision++
	c.Segments = append(c.Segments, ref)
	c.Active = &TailRef{File: newName, Start: coverage.End}
	if err = l.catalog.Publish(c.Revision-1, c); err != nil {
		return errors.Join(err, newFile.Close())
	}
	old := l.file
	l.file, l.tail, l.framed = newFile, newTail, 0
	return old.Close()
}

// Seek returns the first surviving record at or above id. Reads hold the log
// mutex; there are no long-lived file pins or iterators in this initial lifecycle.
func (l *Log) Seek(id uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return Record{}, err
	}
	c, err := l.catalog.Current()
	if err != nil {
		return Record{}, err
	}
	i := sort.Search(len(c.Segments), func(i int) bool { return c.Segments[i].Coverage.End > id })
	for ; i < len(c.Segments); i++ {
		ref := c.Segments[i]
		if ref.Count == 0 {
			continue
		}
		f, err := os.Open(filepath.Join(l.path, ref.File))
		if err != nil {
			return Record{}, err
		}
		rec, readErr := func() (Record, error) {
			info, err := f.Stat()
			if err != nil {
				return Record{}, err
			}
			s, err := OpenSegment(f, info.Size())
			if err != nil {
				return Record{}, err
			}
			return s.Seek(id)
		}()
		closeErr := f.Close()
		if closeErr != nil {
			return Record{}, errors.Join(readErr, closeErr)
		}
		if errors.Is(readErr, ErrNotFound) {
			continue
		}
		return rec, readErr
	}
	state, err := l.tail.State()
	if err != nil {
		return Record{}, err
	}
	var result Record
	_, err = ScanTail(l.file, state.End, func(r Record) error {
		if r.ID >= id {
			result = r
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return result, nil
	}
	if err != nil {
		return Record{}, err
	}
	return Record{}, ErrNotFound
}

func (l *Log) Read(id uint64) (Record, error) {
	r, err := l.Seek(id)
	if err == nil && r.ID != id {
		return Record{}, ErrNotFound
	}
	return r, err
}

// Close releases the active file and then directory ownership without forcing
// a new sealed boundary. Every
// successfully appended group was already synced. Close is idempotent.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return errors.Join(l.file.Close(), l.lock.Close())
}
