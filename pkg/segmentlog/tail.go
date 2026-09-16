package segmentlog

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

const (
	tailHeaderSize   = 32
	groupHeaderSize  = 32
	groupTrailerSize = 16
	maxGroupBytes    = 32 << 20
)

var (
	// ErrIncompleteTail identifies a short final group, not permission to discard
	// it. Recovery must establish external durability bounds before truncating.
	ErrIncompleteTail = errors.New("segmentlog: incomplete tail suffix")
	ErrTailPoisoned   = errors.New("segmentlog: tail requires recovery after I/O failure")
)

// TailState describes a validated prefix. End is the next byte offset; Count and
// Last describe records, not range coverage. HasRecords distinguishes ID zero
// from an empty tail. Start is the original lower ID bound.
type TailState struct {
	Start, Last, Count uint64
	End                int64
	HasRecords         bool
}

// WriteTailHeader prepares a new empty tail. The caller must durably install it
// before opening an appender. This function does not sync or publish w.
func WriteTailHeader(w io.Writer, start uint64) error {
	if start == ^uint64(0) {
		return ErrInvalid
	}
	h := make([]byte, tailHeaderSize)
	copy(h, "SLTAIL00")
	format.LE.PutUint64(h[12:], start)
	format.LE.PutUint32(h[28:], format.CRC(h[:28]))
	return writeFull(w, h)
}

// ScanTail verifies a captured file prefix. It never modifies the input. A
// complete group is validated before any of its records are passed to visit.
// On failure, state ends before that group (callback errors also stop there).
// Earlier groups may already have been visited, so callbacks must support replay.
// A partial header or body is reported as ErrIncompleteTail; a full invalid
// header, checksum failure, or invalid record ordering is ErrCorrupt. Neither
// classification alone proves a suffix is safe to discard. The input must remain
// immutable within size while scanning. A nil visitor verifies without delivery.
func ScanTail(r io.ReaderAt, size int64, visit func(Record) error) (state TailState, err error) {
	if size < tailHeaderSize {
		return state, ErrCorrupt
	}
	h := make([]byte, tailHeaderSize)
	if _, err = r.ReadAt(h, 0); err != nil {
		return state, err
	}
	if format.CRC(h[:28]) != format.LE.Uint32(h[28:]) {
		return state, ErrCorrupt
	}
	if string(h[:8]) != "SLTAIL00" || format.LE.Uint16(h[8:]) != 0 || format.LE.Uint16(h[10:]) != 0 || format.LE.Uint64(h[20:]) != 0 {
		return state, ErrUnsupported
	}
	state.Start = format.LE.Uint64(h[12:])
	state.End = tailHeaderSize
	if state.Start == ^uint64(0) {
		return state, ErrCorrupt
	}
	for state.End < size {
		if size-state.End < groupHeaderSize {
			return state, ErrIncompleteTail
		}
		header := make([]byte, groupHeaderSize)
		if _, err = r.ReadAt(header, state.End); err != nil {
			return state, err
		}
		if format.CRC(header[:28]) != format.LE.Uint32(header[28:]) {
			return state, ErrCorrupt
		}
		if string(header[:8]) != "SLGROUP0" || format.LE.Uint32(header[24:]) != 0 {
			return state, ErrUnsupported
		}
		length, count := format.LE.Uint32(header[8:]), format.LE.Uint32(header[12:])
		last := format.LE.Uint64(header[16:])
		if length < format.FrameOverhead || length > maxGroupBytes || count == 0 || count > length/format.FrameOverhead || last < state.Start || last == ^uint64(0) || (state.HasRecords && last <= state.Last) {
			return state, ErrCorrupt
		}
		total := int64(groupHeaderSize) + int64(length) + groupTrailerSize
		if total > size-state.End {
			return state, ErrIncompleteTail
		}
		body := make([]byte, int(length)+groupTrailerSize)
		if _, err = r.ReadAt(body, state.End+groupHeaderSize); err != nil {
			return state, err
		}
		trailer := body[length:]
		if string(trailer[:8]) != "SLEND000" || format.LE.Uint32(trailer[8:]) != length || format.CRCParts(header, body[:len(body)-4]) != format.LE.Uint32(trailer[12:]) {
			return state, ErrCorrupt
		}
		data := body[:length]
		var records []Record
		previous, has := state.Last, state.HasRecords
		for len(data) > 0 {
			id, payload, rest, e := format.Frame(data)
			if e != nil {
				return state, e
			}
			if id < state.Start || id == ^uint64(0) || (has && id <= previous) {
				return state, ErrCorrupt
			}
			records = append(records, Record{id, payload})
			previous, has, data = id, true, rest
		}
		if len(records) != int(count) || previous != last {
			return state, ErrCorrupt
		}
		if visit != nil {
			for _, record := range records {
				if err = visit(record); err != nil {
					return state, err
				}
			}
		}
		state.End += total
		state.Count += uint64(count)
		state.Last = last
		state.HasRecords = true
	}
	return state, nil
}

// TailFile is the narrow file contract needed for append and recovery scanning.
// The caller owns Close and exclusive mutation of the file, including its size.
// It must be opened for positioned writes (not O_APPEND).
type TailFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Sync() error
}

// Tail appends synchronized groups to one already durably installed file.
// It serializes its own methods; no other handle may mutate the file. Catalog
// membership, rotation, directory durability, and truncation are caller-owned.
type Tail struct {
	mu     sync.Mutex
	file   TailFile
	state  TailState
	poison error
}

// OpenTail validates the entire file and syncs recovered complete groups before
// returning an append handle. It refuses incomplete suffixes without modifying
// the file. A complete marker alone never counts as proof of a previous sync.
func OpenTail(f TailFile) (*Tail, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrInvalid
	}
	state, err := ScanTail(f, info.Size(), nil)
	if err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	return &Tail{file: f, state: state}, nil
}

// State returns only the last successfully synchronized state of this handle.
// After a failed append it returns that state with ErrTailPoisoned; recovery may
// subsequently discover a complete unacknowledged group beyond it.
func (t *Tail) State() (TailState, error) { t.mu.Lock(); defer t.mu.Unlock(); return t.state, t.poison }

// Append writes one bounded group and syncs before acknowledging success. Invalid
// input does not poison the handle; any write/short-write/sync failure does.
// IDs must increase across calls and stay at/above the tail's Start. Empty calls
// are invalid. Group payload frames may total at most 32 MiB. Callers must not
// mutate supplied payloads during the call.
func (t *Tail) Append(records []Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.poison != nil {
		return t.poison
	}
	if len(records) == 0 {
		return ErrInvalid
	}
	previous, has := t.state.Last, t.state.HasRecords
	size := 0
	for _, r := range records {
		if r.ID < t.state.Start || r.ID == ^uint64(0) || (has && r.ID <= previous) || len(r.Payload) > format.MaxPayload || len(r.Payload)+format.FrameOverhead > maxGroupBytes-size {
			return ErrInvalid
		}
		size += len(r.Payload) + format.FrameOverhead
		previous, has = r.ID, true
	}
	group := make([]byte, groupHeaderSize, groupHeaderSize+size+groupTrailerSize)
	copy(group, "SLGROUP0")
	format.LE.PutUint32(group[8:], uint32(size))
	format.LE.PutUint32(group[12:], uint32(len(records)))
	format.LE.PutUint64(group[16:], previous)
	format.LE.PutUint32(group[28:], format.CRC(group[:28]))
	for _, r := range records {
		group = format.AppendFrame(group, r.ID, r.Payload)
	}
	group = append(group, []byte("SLEND000")...)
	group = format.LE.AppendUint32(group, uint32(size))
	group = format.LE.AppendUint32(group, format.CRC(group))
	if t.state.End > int64(^uint64(0)>>1)-int64(len(group)) {
		return ErrInvalid
	}
	n, err := t.file.WriteAt(group, t.state.End)
	if err == nil && n != len(group) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = t.file.Sync()
	}
	if err != nil {
		t.poison = errors.Join(ErrTailPoisoned, err)
		return t.poison
	}
	t.state.End += int64(len(group))
	t.state.Last = previous
	t.state.Count += uint64(len(records))
	t.state.HasRecords = true
	return nil
}
