package segmentlog

import (
	"crypto/sha256"
	"errors"
	"hash"
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
// Last describe surviving records for standalone tails. Managed rewritten tails
// retain the original Last and HasRecords append frontier, even if Count is zero.
// OriginalCount and Framed retain pre-erasure rotation accounting.
type TailState struct {
	Start, Last, Count    uint64
	OriginalCount, Framed uint64
	End                   int64
	HasRecords            bool
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
// This standalone scanner reports physical records; managed recovery also applies
// the catalog checkpoint to restore pre-erasure append accounting.
func ScanTail(r io.ReaderAt, size int64, visit func(Record) error) (state TailState, err error) {
	return scanTail(r, size, nil, visit)
}

func scanTail(r io.ReaderAt, size int64, checkpoint *TailCheckpoint, visit func(Record) error) (TailState, error) {
	return scanTailChecked(r, size, checkpoint, nil, visit)
}

// scanManagedTail checks catalog ownership before any group can be delivered.
func scanManagedTail(r io.ReaderAt, size int64, ref TailRef, visit func(Record) error) (TailState, error) {
	return scanTailChecked(r, size, ref.Checkpoint, &ref.Start, visit)
}

func scanTailChecked(r io.ReaderAt, size int64, checkpoint *TailCheckpoint, start *uint64, visit func(Record) error) (state TailState, err error) {
	return scanTailHashed(r, size, checkpoint, start, visit, nil)
}

// scanTailHashed optionally hashes physical bytes during the existing validation
// pass. A failed scan never produces an appender or an authoritative digest.
func scanTailHashed(r io.ReaderAt, size int64, checkpoint *TailCheckpoint, start *uint64, visit func(Record) error, digest hash.Hash) (state TailState, err error) {
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
	if digest != nil {
		_, _ = digest.Write(h)
	}
	state.Start = format.LE.Uint64(h[12:])
	state.End = tailHeaderSize
	if state.Start == ^uint64(0) || (start != nil && state.Start != *start) {
		return state, ErrCorrupt
	}
	if checkpoint != nil && (!checkpoint.valid(state.Start) || checkpoint.End > size) {
		return state, ErrCorrupt
	}
	header := make([]byte, groupHeaderSize)
	var body []byte
	for {
		if checkpoint != nil {
			if state.End > checkpoint.End {
				return state, ErrCorrupt
			}
			if state.End == checkpoint.End {
				if state.Count > checkpoint.Count || (state.HasRecords && state.Last > checkpoint.Last) {
					return state, ErrCorrupt
				}
				state.Last, state.HasRecords = checkpoint.Last, true
				state.OriginalCount, state.Framed = checkpoint.Count, checkpoint.Framed
				checkpoint = nil
			}
		}
		if state.End == size {
			break
		}

		if size-state.End < groupHeaderSize {
			return state, ErrIncompleteTail
		}
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
		if checkpoint != nil && total > checkpoint.End-state.End {
			return state, ErrCorrupt
		}
		if total > size-state.End {
			return state, ErrIncompleteTail
		}
		// Validation-only scans retain no payloads, so reuse their group buffer.
		// Visitors may keep records after returning and need independent bytes.
		needed := int(length) + groupTrailerSize
		if visit != nil || cap(body) < needed {
			body = make([]byte, needed)
		} else {
			body = body[:needed]
		}
		if _, err = r.ReadAt(body, state.End+groupHeaderSize); err != nil {
			return state, err
		}
		trailer := body[length:]
		if string(trailer[:8]) != "SLEND000" || format.LE.Uint32(trailer[8:]) != length || format.CRCParts(header, body[:len(body)-4]) != format.LE.Uint32(trailer[12:]) {
			return state, ErrCorrupt
		}
		if digest != nil {
			_, _ = digest.Write(header)
			_, _ = digest.Write(body)
		}
		data := body[:length]
		var records []Record
		var decoded uint32
		previous, has := state.Last, state.HasRecords
		for len(data) > 0 {
			if decoded == count {
				return state, ErrCorrupt
			}
			id, payload, rest, e := format.Frame(data)
			if e != nil {
				return state, e
			}
			if id < state.Start || id == ^uint64(0) || (has && id <= previous) {
				return state, ErrCorrupt
			}
			if visit != nil {
				// The header bounds count by the group size. Allocate once, after
				// the first frame validates; verification needs no record list.
				if records == nil {
					records = make([]Record, 0, int(count))
				}
				records = append(records, Record{id, payload})
			}
			decoded++
			previous, has, data = id, true, rest
		}
		if decoded != count || previous != last {
			return state, ErrCorrupt
		}
		if state.Framed > ^uint64(0)-uint64(length) || state.OriginalCount > ^uint64(0)-uint64(count) {
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
		state.Framed += uint64(length)
		state.OriginalCount += uint64(count)
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
	digest hash.Hash
}

// OpenTail validates the entire file and syncs recovered complete groups before
// returning an append handle. It refuses incomplete suffixes without modifying
// the file. A complete marker alone never counts as proof of a previous sync.
func OpenTail(f TailFile) (*Tail, error) {
	return openTail(f, nil)
}

func openTail(f TailFile, checkpoint *TailCheckpoint) (*Tail, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrInvalid
	}
	digest := sha256.New()
	state, err := scanTailHashed(f, info.Size(), checkpoint, nil, nil, digest)
	if err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	return &Tail{file: f, state: state, digest: digest}, nil
}

// State returns only the last successfully synchronized state of this handle.
// After a failed append it returns that state with ErrTailPoisoned; recovery may
// subsequently discover a complete unacknowledged group beyond it.
func (t *Tail) State() (TailState, error) { t.mu.Lock(); defer t.mu.Unlock(); return t.state, t.poison }

// rolloverState captures synchronized progress and its physical-byte digest
// together. The caller must retain exclusive mutation through publication.
func (t *Tail) rolloverState() (TailState, [sha256.Size]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var sum [sha256.Size]byte
	if t.poison != nil {
		return t.state, sum, t.poison
	}
	t.digest.Sum(sum[:0])
	return t.state, sum, nil
}

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
	if t.state.Framed > ^uint64(0)-uint64(size) || t.state.OriginalCount > ^uint64(0)-uint64(len(records)) {
		return ErrInvalid
	}
	group := encodeTailGroup(records, size)
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
	_, _ = t.digest.Write(group)
	t.state.End += int64(len(group))
	t.state.Last = previous
	t.state.Count += uint64(len(records))
	t.state.OriginalCount += uint64(len(records))
	t.state.Framed += uint64(size)
	t.state.HasRecords = true
	return nil
}

// encodeTailGroup requires a nonempty, validated batch and its framed byte size.
func encodeTailGroup(records []Record, size int) []byte {
	group := make([]byte, groupHeaderSize, groupHeaderSize+size+groupTrailerSize)
	copy(group, "SLGROUP0")
	format.LE.PutUint32(group[8:], uint32(size))          // #nosec G115 -- Internal encoder receives positive framed size bounded by maxGroupBytes.
	format.LE.PutUint32(group[12:], uint32(len(records))) // #nosec G115 -- Validated records each consume FrameOverhead within maxGroupBytes.
	format.LE.PutUint64(group[16:], records[len(records)-1].ID)
	format.LE.PutUint32(group[28:], format.CRC(group[:28]))
	for _, r := range records {
		group = format.AppendFrame(group, r.ID, r.Payload)
	}
	group = append(group, []byte("SLEND000")...)
	group = format.LE.AppendUint32(group, uint32(size)) // #nosec G115 -- Same bounded framed size as the group header.
	group = format.LE.AppendUint32(group, format.CRC(group))

	return group
}
