// Package tidwall implements eventlog.EventLog with dense tidwall sequences and
// whole-log replacement. Its experimental CURRENT/generation wrapper is not a
// production legacy directory and is never detected or activated automatically.
package tidwall

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	wal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/durablefs"
)

const maxPayload = 16 << 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type (
	Options struct {
		SegmentBytes int
		Compress     bool
	}
	manifest struct {
		Version                        uint64
		Directory                      string
		Start, Generation, Last, Count uint64
		HasAppends                     bool
		Options                        Options
	}
	publisher interface {
		Replace(string, func(io.Writer) error) (durablefs.Result, error)
	}
	Log struct {
		mu     sync.Mutex
		path   string
		dir    *durablefs.Dir
		pub    publisher
		lock   *durablefs.DirectoryLock
		data   *wal.Log
		state  manifest
		last   uint64
		has    bool
		poison error
		closed bool
	}
)

var _ eventlog.EventLog = (*Log)(nil)

func mapError(err error) error {
	for _, p := range [][2]error{{wal.ErrCorrupt, eventlog.ErrCorrupt}, {wal.ErrNotFound, eventlog.ErrCorrupt}, {wal.ErrClosed, eventlog.ErrClosed}, {durablefs.ErrLocked, eventlog.ErrLocked}, {durablefs.ErrUnsupported, eventlog.ErrUnsupported}} {
		if errors.Is(err, p[0]) {
			return errors.Join(p[1], err)
		}
	}
	return err
}

func (l *Log) fail(err error) error {
	l.poison = errors.Join(eventlog.ErrPoisoned, mapError(err))
	return l.poison
}

func (l *Log) usable() error {
	if l.closed {
		return eventlog.ErrClosed
	}
	return l.poison
}

func options(m manifest, prepare bool) *wal.Options {
	o := &wal.Options{SegmentSize: m.Options.SegmentBytes, AllowEmpty: true, NoSync: prepare}
	if m.Options.Compress {
		o.SealedSegmentCompression = wal.CompressionZstd
	}
	return o
}

func own(path string) (*Log, error) {
	lock, e := durablefs.Lock(path)
	if e != nil {
		return nil, mapError(e)
	}
	dir, e := durablefs.Open(path)
	if e != nil {
		return nil, errors.Join(mapError(e), lock.Close())
	}
	return &Log{path: path, dir: dir, pub: dir, lock: lock}, nil
}

func validDirectory(name string) bool {
	if !strings.HasPrefix(name, "generation-") || len(name) != 43 {
		return false
	}
	_, e := hex.DecodeString(name[11:])
	return e == nil
}

func newData(path string, m manifest, prepare bool) (string, *wal.Log, error) {
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil {
		return "", nil, e
	}
	name := "generation-" + hex.EncodeToString(id[:])
	p := filepath.Join(path, name)
	if e := os.Mkdir(p, 0o700); e != nil {
		return "", nil, e
	}
	log, e := wal.Open(p, options(m, prepare))
	return name, log, e
}

// Create requires an empty existing directory with durable parents. Failed work
// leaves orphans for explicit recovery; it never adopts an existing directory.
func Create(path string, start uint64, opts Options) (result *Log, err error) {
	if start == ^uint64(0) || opts.SegmentBytes < 0 {
		return nil, eventlog.ErrInvalid
	}
	l, e := own(path)
	if e != nil {
		return nil, e
	}
	defer func() {
		if result == nil {
			if l.data != nil {
				err = errors.Join(err, l.data.Close())
			}
			err = errors.Join(err, l.lock.Close())
		}
	}()
	entries, e := os.ReadDir(path)
	if e != nil {
		return nil, e
	}
	if len(entries) != 0 {
		return nil, eventlog.ErrInvalid
	}
	l.state = manifest{Version: 1, Start: start, Options: opts}
	name, data, e := newData(path, l.state, false)
	if e != nil {
		return nil, e
	}
	l.data = data
	l.state.Directory = name
	if e = l.syncData(data, name); e != nil {
		return nil, e
	}
	if e = l.publish(l.state); e != nil {
		return nil, e
	}
	return l, nil
}

func readManifest(path string) (manifest, error) {
	var m manifest
	info, e := os.Lstat(filepath.Join(path, "CURRENT"))
	if e != nil {
		return m, e
	}
	if !info.Mode().IsRegular() {
		return m, eventlog.ErrCorrupt
	}
	f, e := os.Open(filepath.Join(path, "CURRENT")) // #nosec G304 -- Fixed manifest name in the caller-selected, exclusively managed log directory.
	if e != nil {
		return m, e
	}
	raw, e := io.ReadAll(io.LimitReader(f, 4097))
	e = errors.Join(e, f.Close())
	if e != nil {
		return m, e
	}
	if len(raw) < 32 || len(raw) > 4096 {
		return m, eventlog.ErrCorrupt
	}
	digest := sha256.Sum256(raw[32:])
	if !bytes.Equal(digest[:], raw[:32]) {
		return m, eventlog.ErrCorrupt
	}
	if e = json.Unmarshal(raw[32:], &m); e != nil {
		return m, errors.Join(eventlog.ErrCorrupt, e)
	}
	canonical, e := json.Marshal(m)
	if e != nil {
		return m, e
	}
	if !bytes.Equal(canonical, raw[32:]) || m.Version != 1 || !validDirectory(m.Directory) || m.Start == ^uint64(0) || m.Options.SegmentBytes < 0 || (m.HasAppends && (m.Last < m.Start || m.Last == ^uint64(0))) || (!m.HasAppends && (m.Last != 0 || m.Count != 0)) {
		return m, eventlog.ErrCorrupt
	}
	return m, nil
}

// Open follows CURRENT only, verifies its referenced prefix, and recovers later
// durable appends. Missing CURRENT/data never cause fallback to another generation.
func Open(path string) (result *Log, err error) {
	l, e := own(path)
	if e != nil {
		return nil, e
	}
	defer func() {
		if result == nil {
			if l.data != nil {
				err = errors.Join(err, l.data.Close())
			}
			err = errors.Join(err, l.lock.Close())
		}
	}()
	m, e := readManifest(path)
	if e != nil {
		return nil, e
	}
	l.state = m
	p := filepath.Join(path, m.Directory)
	info, e := os.Lstat(p)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() {
		return nil, eventlog.ErrCorrupt
	}
	entries, e := os.ReadDir(p)
	if e != nil {
		return nil, e
	}
	if len(entries) == 0 {
		return nil, eventlog.ErrCorrupt
	}
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil {
			return nil, e
		}
		if !info.Mode().IsRegular() {
			return nil, eventlog.ErrCorrupt
		}
	}
	l.data, e = wal.Open(p, options(m, false))
	if e != nil {
		return nil, mapError(e)
	}
	l.last, l.has, e = l.verify()
	if e != nil {
		return nil, e
	}
	if e = l.syncData(l.data, m.Directory); e != nil {
		return nil, e
	}
	f, e := os.Open(filepath.Join(path, "CURRENT")) // #nosec G304 -- Fixed manifest name in the caller-selected, exclusively managed log directory.
	if e != nil {
		return nil, e
	}
	e = errors.Join(f.Sync(), f.Close())
	if e != nil {
		return nil, e
	}
	if e = l.dir.Sync(); e != nil {
		return nil, e
	}
	return l, nil
}

func encode(r eventlog.Record) []byte {
	b := make([]byte, 8, 12+len(r.Payload))
	binary.LittleEndian.PutUint64(b, r.ID)
	b = append(b, r.Payload...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
	return b
}

func decode(b []byte) (eventlog.Record, error) {
	if len(b) < 12 || len(b) > maxPayload+12 || crc32.Checksum(b[:len(b)-4], crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return eventlog.Record{}, eventlog.ErrCorrupt
	}
	id := binary.LittleEndian.Uint64(b)
	if id == ^uint64(0) {
		return eventlog.Record{}, eventlog.ErrCorrupt
	}
	return eventlog.Record{ID: id, Payload: bytes.Clone(b[8 : len(b)-4])}, nil
}

func (l *Log) at(seq uint64) (eventlog.Record, error) {
	b, e := l.data.Read(seq)
	if e != nil {
		return eventlog.Record{}, mapError(e)
	}
	return decode(b)
}

func (l *Log) verify() (uint64, bool, error) {
	end, e := l.data.LastIndex()
	if e != nil {
		return 0, false, mapError(e)
	}
	if end == ^uint64(0) || end < l.state.Count {
		return 0, false, eventlog.ErrCorrupt
	}
	last, has := l.state.Last, l.state.HasAppends
	var previous uint64
	for i := uint64(1); i <= end; i++ {
		r, e := l.at(i)
		if e != nil {
			return 0, false, e
		}
		if r.ID < l.state.Start || (i > 1 && r.ID <= previous) || (i <= l.state.Count && r.ID > l.state.Last) || (i > l.state.Count && has && r.ID <= last) {
			return 0, false, eventlog.ErrCorrupt
		}
		previous = r.ID
		if i > l.state.Count {
			last, has = r.ID, true
		}
	}
	return last, has, nil
}

func (l *Log) syncData(log *wal.Log, name string) error {
	if e := log.Sync(); e != nil {
		return e
	}
	if l.state.Options.Compress {
		for {
			did, e := log.CompressNextSealed()
			if e != nil {
				return e
			}
			if !did {
				break
			}
		}
	}
	// Sync every current file, including compressed outputs, before publishing.
	path := filepath.Join(l.path, name)
	entries, e := os.ReadDir(path)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		f, e := os.Open(filepath.Join(path, entry.Name())) // #nosec G304 -- Entry names come from ReadDir of the exclusively managed generation directory.
		if e != nil {
			return e
		}
		e = errors.Join(f.Sync(), f.Close())
		if e != nil {
			return e
		}
	}
	d, e := durablefs.Open(path)
	if e != nil {
		return e
	}
	if e = d.Sync(); e != nil {
		return e
	}
	return l.dir.Sync()
}

func (l *Log) publish(m manifest) error {
	raw, e := json.Marshal(m)
	if e != nil {
		return e
	}
	hash := sha256.Sum256(raw)
	raw = append(hash[:], raw...)
	_, e = l.pub.Replace("CURRENT", func(w io.Writer) error {
		n, e := w.Write(raw)
		if e == nil && n != len(raw) {
			return io.ErrShortWrite
		}
		return e
	})
	return e
}

func (l *Log) Append(records []eventlog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return e
	}
	if len(records) == 0 {
		return eventlog.ErrInvalid
	}
	last, has := l.last, l.has
	for _, r := range records {
		if r.ID < l.state.Start || r.ID == ^uint64(0) || len(r.Payload) > maxPayload || (has && r.ID <= last) {
			return eventlog.ErrInvalid
		}
		last, has = r.ID, true
	}
	seq, e := l.data.LastIndex()
	if e != nil {
		return l.fail(e)
	}
	if uint64(len(records)) >= ^uint64(0)-seq {
		return eventlog.ErrInvalid
	}
	batch := new(wal.Batch)
	for _, r := range records {
		seq++
		batch.Write(seq, encode(r))
	}
	if e := l.data.WriteBatch(batch); e != nil {
		return l.fail(e)
	}
	l.last, l.has = last, has
	return nil
}

func (l *Log) LastAppended() (uint64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return 0, false, e
	}
	return l.last, l.has, nil
}

func (l *Log) seek(id uint64) (uint64, eventlog.Record, error) {
	last, e := l.data.LastIndex()
	if e != nil {
		return 0, eventlog.Record{}, mapError(e)
	}
	if last == ^uint64(0) {
		return 0, eventlog.Record{}, eventlog.ErrCorrupt
	}
	lo, hi := uint64(1), last+1
	for lo < hi {
		mid := lo + (hi-lo)/2
		r, e := l.at(mid)
		if e != nil {
			return 0, eventlog.Record{}, e
		}
		if r.ID >= id {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo > last {
		return 0, eventlog.Record{}, eventlog.ErrNotFound
	}
	r, e := l.at(lo)
	return lo, r, e
}

func (l *Log) Seek(id uint64) (eventlog.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return eventlog.Record{}, e
	}
	_, r, e := l.seek(id)
	return r, e
}

func (l *Log) Read(id uint64) (eventlog.Record, error) {
	r, e := l.Seek(id)
	if e == nil && r.ID != id {
		return eventlog.Record{}, eventlog.ErrNotFound
	}
	return r, e
}

func (l *Log) Scan(ctx context.Context, b eventlog.Coverage, visit func(eventlog.Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return e
	}
	if ctx == nil || visit == nil || b.Start > b.End {
		return eventlog.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if b.Start == b.End {
		return nil
	}
	seq, _, e := l.seek(b.Start)
	if errors.Is(e, eventlog.ErrNotFound) {
		return nil
	}
	if e != nil {
		return e
	}
	last, e := l.data.LastIndex()
	if e != nil {
		return mapError(e)
	}
	for ; seq <= last; seq++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		r, e := l.at(seq)
		if e != nil {
			return e
		}
		if r.ID >= b.End {
			return nil
		}
		if e = visit(r); e != nil {
			return e
		}
	}
	return ctx.Err()
}

// Tidwall retains its whole-rewrite exclusion. Acquire the caller lock before
// entering the backend so a reader holding it can finish its backend reads.
func (l *Log) RewriteWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, publication sync.Locker) (eventlog.RewriteResult, error) {
	if publication == nil {
		return eventlog.RewriteResult{}, eventlog.ErrInvalid
	}
	publication.Lock()
	defer publication.Unlock()
	return l.Rewrite(ctx, generation, transform)
}

func (l *Log) Rewrite(ctx context.Context, generation uint64, transform eventlog.Transform) (result eventlog.RewriteResult, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.usable(); e != nil {
		return result, e
	}
	if ctx == nil || transform == nil || generation <= l.state.Generation {
		return result, eventlog.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return result, e
	}
	if _, _, e := l.verify(); e != nil {
		return result, l.fail(e)
	}
	name, replacement, e := newData(l.path, l.state, true)
	if e != nil {
		return result, l.fail(e)
	}
	defer func() {
		if replacement != nil {
			err = errors.Join(err, replacement.Close())
		}
	}()
	last, e := l.data.LastIndex()
	if e != nil {
		return result, l.fail(e)
	}
	var count uint64
	for i := uint64(1); i <= last; i++ {
		if e = ctx.Err(); e != nil {
			return result, l.fail(e)
		}
		r, e := l.at(i)
		if e != nil {
			return result, l.fail(e)
		}
		original := bytes.Clone(r.Payload)
		payload, keep, e := transform(r)
		if e != nil {
			return result, l.fail(e)
		}
		if !keep || !bytes.Equal(original, payload) {
			result.ChangedRecords++
		}
		if keep {
			if len(payload) > maxPayload {
				return result, l.fail(eventlog.ErrInvalid)
			}
			count++
			if e = replacement.Write(count, encode(eventlog.Record{ID: r.ID, Payload: payload})); e != nil {
				return result, l.fail(e)
			}
		}
	}
	if e = ctx.Err(); e != nil {
		return result, l.fail(e)
	}
	if e = l.syncData(replacement, name); e != nil {
		return result, l.fail(e)
	}
	// Reopen with synchronous append options before the new directory becomes live.
	if e = replacement.Close(); e != nil {
		return result, l.fail(e)
	}
	replacement = nil
	replacement, e = wal.Open(filepath.Join(l.path, name), options(l.state, false))
	if e != nil {
		return result, l.fail(e)
	}
	next := l.state
	next.Directory = name
	next.Generation = generation
	next.Last = l.last
	next.HasAppends = l.has
	next.Count = count
	if e = ctx.Err(); e != nil {
		return result, l.fail(e)
	}
	if e = l.publish(next); e != nil {
		return result, l.fail(e)
	}
	old := l.data
	l.data = replacement
	replacement = nil
	l.state = next
	result.Published = true
	if e = old.Close(); e != nil {
		return result, l.fail(e)
	}
	return result, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return errors.Join(l.data.Close(), l.lock.Close())
}
