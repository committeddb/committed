package segmentlog

import (
	"errors"
	"io"
	"iter"
	"sort"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

var (
	ErrCorrupt     = format.ErrCorrupt
	ErrUnsupported = format.ErrUnsupported
	ErrNotFound    = errors.New("segmentlog: record not found")
	ErrInvalid     = errors.New("segmentlog: invalid input")
)

// Record is an opaque payload at a stable ID. IDs need not be consecutive.
type Record struct {
	ID      uint64
	Payload []byte
}

// Coverage is the original half-open interval [Start, End) owned by a segment.
// Rewriting preserves it even when the first or last record is removed.
// The experimental encoding reserves MaxUint64 as an exclusive end bound.
type Coverage struct{ Start, End uint64 }

// Compression selects encoder effort. Each compressed block identifies its
// decoder independently, and incompressible blocks are stored plain.
type Compression uint8

const (
	NoCompression Compression = iota
	ZstdFast
	ZstdDefault
	ZstdBetter
	ZstdBest
)

// Options controls encoding policy, independently of record semantics.
type Options struct {
	// BlockSize is the target decoded block size. Zero selects 256 KiB.
	// Records larger than the target occupy a block alone (maximum 16 MiB payload).
	BlockSize int
	// Compression defaults to NoCompression. It does not change record IDs or
	// block boundaries. Encoder levels are policy, not required decoder features.
	Compression Compression
}

type block struct {
	first, last, offset uint64
	size, count, crc    uint32
	decoded             uint32
	codec               format.Codec
}

// Segment reads an immutable file using an index entry per block. Its ReaderAt
// must remain open and immutable for its lifetime. Concurrent reads are safe if
// the ReaderAt supports them. Segment does not own or close the underlying file.
type Segment struct {
	r        io.ReaderAt
	coverage Coverage
	blocks   []block
	count    uint64
}

func (s *Segment) Coverage() Coverage { return s.coverage }
func (s *Segment) Count() uint64      { return s.count }

// WriteSegment writes a complete experimental segment. It never syncs, closes,
// or publishes w. On error, discard the incomplete output. Empty segments are
// supported here; managed catalogs represent empty ranges without files.
func WriteSegment(w io.Writer, coverage Coverage, records iter.Seq2[Record, error], opts Options) error {
	if coverage.Start >= coverage.End || records == nil {
		return ErrInvalid
	}
	target := opts.BlockSize
	if target == 0 {
		target = 256 << 10
	}
	if target < format.FrameOverhead || target > format.MaxBlock || opts.Compression > ZstdBest {
		return ErrInvalid
	}
	encoder, err := format.NewEncoder(int(opts.Compression))
	if err != nil {
		return err
	}
	defer encoder.Close()
	h := make([]byte, format.HeaderSize)
	copy(h, "SEGLOG00")
	format.LE.PutUint16(h[8:], 1)  // experimental version
	format.LE.PutUint16(h[10:], 0) // no additional required features
	format.LE.PutUint64(h[12:], coverage.Start)
	format.LE.PutUint64(h[20:], coverage.End)
	format.LE.PutUint32(h[28:], format.CRC(h[:28]))
	if err := writeFull(w, h); err != nil {
		return err
	}
	offset := uint64(len(h))
	var blocks []block
	var data []byte
	var current block
	var previous, total uint64
	flush := func() error {
		if current.count == 0 {
			return nil
		}
		if len(blocks) >= format.MaxBlocks {
			return ErrInvalid
		}
		codec, stored := encoder.Encode(data)
		current.codec, current.decoded = codec, uint32(len(data))
		current.offset, current.size, current.crc = offset, uint32(len(stored)), format.CRC(stored)
		if err := writeFull(w, stored); err != nil {
			return err
		}
		offset += uint64(len(stored))
		blocks = append(blocks, current)
		data = data[:0]
		current = block{}
		return nil
	}
	for rec, err := range records {
		if err != nil {
			return err
		}
		if rec.ID < coverage.Start || rec.ID >= coverage.End || (total > 0 && rec.ID <= previous) || len(rec.Payload) > format.MaxPayload {
			return ErrInvalid
		}
		if len(data)+len(rec.Payload)+format.FrameOverhead > target {
			if err := flush(); err != nil {
				return err
			}
		}
		if current.count == 0 {
			current.first = rec.ID
		}
		current.last = rec.ID
		current.count++
		data = format.AppendFrame(data, rec.ID, rec.Payload)
		previous = rec.ID
		total++
	}
	if err := flush(); err != nil {
		return err
	}
	index := make([]byte, 0, len(blocks)*format.IndexEntrySize)
	for _, b := range blocks {
		index = format.LE.AppendUint64(index, b.first)
		index = format.LE.AppendUint64(index, b.last)
		index = format.LE.AppendUint64(index, b.offset)
		index = format.LE.AppendUint32(index, b.size)
		index = format.LE.AppendUint32(index, b.count)
		index = format.LE.AppendUint32(index, b.crc)
		index = format.LE.AppendUint32(index, b.decoded)
		index = format.LE.AppendUint16(index, uint16(b.codec))
		index = format.LE.AppendUint16(index, 0) // reserved
		index = format.LE.AppendUint32(index, 0) // reserved
	}
	if err := writeFull(w, index); err != nil {
		return err
	}
	f := make([]byte, format.FooterSize)
	format.LE.PutUint64(f, offset)
	format.LE.PutUint32(f[8:], uint32(len(blocks)))
	format.LE.PutUint64(f[12:], total)
	format.LE.PutUint32(f[20:], format.CRC(index))
	copy(f[24:], "END1")
	format.LE.PutUint32(f[28:], format.CRC(f[:28]))
	return writeFull(w, f)
}

func writeFull(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

// OpenSegment validates header, footer, and block index before allocating data
// blocks. Payload integrity is checked on read, or eagerly with Verify.
func OpenSegment(r io.ReaderAt, size int64) (*Segment, error) {
	if size < format.HeaderSize+format.FooterSize {
		return nil, ErrCorrupt
	}
	h := make([]byte, format.HeaderSize)
	f := make([]byte, format.FooterSize)
	if _, err := r.ReadAt(h, 0); err != nil {
		return nil, err
	}
	if _, err := r.ReadAt(f, size-format.FooterSize); err != nil {
		return nil, err
	}
	if format.CRC(h[:28]) != format.LE.Uint32(h[28:]) || format.CRC(f[:28]) != format.LE.Uint32(f[28:]) {
		return nil, ErrCorrupt
	}
	version := format.LE.Uint16(h[8:])
	if string(h[:8]) != "SEGLOG00" || version > 1 || format.LE.Uint16(h[10:]) != 0 {
		return nil, ErrUnsupported
	}
	entrySize, endMagic := format.IndexEntrySize, "END1"
	if version == 0 {
		entrySize, endMagic = 40, "END0"
	}
	if string(f[24:28]) != endMagic {
		return nil, ErrUnsupported
	}
	coverage := Coverage{format.LE.Uint64(h[12:]), format.LE.Uint64(h[20:])}
	if coverage.Start >= coverage.End {
		return nil, ErrCorrupt
	}
	offset, n := format.LE.Uint64(f), format.LE.Uint32(f[8:])
	indexSize := uint64(n) * uint64(entrySize)
	if n > format.MaxBlocks || offset < format.HeaderSize || offset > uint64(size-format.FooterSize) || indexSize != uint64(size-format.FooterSize)-offset {
		return nil, ErrCorrupt
	}
	index := make([]byte, int(indexSize))
	if len(index) > 0 {
		if _, err := r.ReadAt(index, int64(offset)); err != nil {
			return nil, err
		}
	}
	if format.CRC(index) != format.LE.Uint32(f[20:]) {
		return nil, ErrCorrupt
	}
	s := &Segment{r: r, coverage: coverage, count: format.LE.Uint64(f[12:])}
	next := uint64(format.HeaderSize)
	var total, previous uint64
	for i := uint32(0); i < n; i++ {
		entry := index[int(i)*entrySize:]
		b := block{
			first: format.LE.Uint64(entry), last: format.LE.Uint64(entry[8:]),
			offset: format.LE.Uint64(entry[16:]), size: format.LE.Uint32(entry[24:]),
			count: format.LE.Uint32(entry[28:]), crc: format.LE.Uint32(entry[32:]),
		}
		b.decoded = b.size
		if version == 0 {
			if format.LE.Uint32(entry[36:]) != 0 {
				return nil, ErrUnsupported
			}
		} else {
			b.decoded, b.codec = format.LE.Uint32(entry[36:]), format.Codec(format.LE.Uint16(entry[40:]))
			if b.codec > format.Zstd || format.LE.Uint16(entry[42:]) != 0 || format.LE.Uint32(entry[44:]) != 0 {
				return nil, ErrUnsupported
			}
		}
		if b.first < coverage.Start || b.last >= coverage.End || b.first > b.last || (i > 0 && b.first <= previous) {
			return nil, ErrCorrupt
		}
		if b.offset != next || b.size == 0 || b.size > format.MaxBlock || uint64(b.size) > offset-next {
			return nil, ErrCorrupt
		}
		if b.decoded < format.FrameOverhead || b.decoded > format.MaxBlock || (b.codec == format.Plain && b.size != b.decoded) {
			return nil, ErrCorrupt
		}
		if b.count == 0 || b.count > b.decoded/format.FrameOverhead || (b.count > 1 && b.first == b.last) {
			return nil, ErrCorrupt
		}
		next += uint64(b.size)
		total += uint64(b.count)
		previous = b.last
		s.blocks = append(s.blocks, b)
	}
	if next != offset || total != s.count {
		return nil, ErrCorrupt
	}
	return s, nil
}

func (s *Segment) readBlock(b block) ([]Record, error) {
	data := make([]byte, int(b.size))
	if _, err := s.r.ReadAt(data, int64(b.offset)); err != nil {
		return nil, err
	}
	return decodeBlock(b, data)
}

func decodeBlock(b block, data []byte) ([]Record, error) {
	var records []Record
	err := walkBlock(b, data, func(r Record) { records = append(records, r) })
	if err != nil {
		return nil, err
	}
	return records, nil
}

// walkBlock validates the same stored bytes, decoded frames, and index bounds
// for reads and verification. A nil visitor avoids retaining record descriptors.
// Visitors are internal: no records escape a public read until validation passes.
func walkBlock(b block, data []byte, visit func(Record)) error {
	if format.CRC(data) != b.crc {
		return ErrCorrupt
	}
	data, err := format.Decode(b.codec, data, b.decoded)
	if err != nil {
		return err
	}
	var count uint32
	var previous uint64
	for len(data) > 0 {
		id, payload, rest, err := format.Frame(data)
		if err != nil {
			return err
		}
		if count >= b.count || id < b.first || id > b.last || (count == 0 && id != b.first) || (count > 0 && id <= previous) {
			return ErrCorrupt
		}
		if visit != nil {
			visit(Record{id, payload})
		}
		count++
		previous, data = id, rest
	}
	if count != b.count || count == 0 || previous != b.last {
		return ErrCorrupt
	}
	return nil
}

// Seek returns the first surviving record with ID >= id, or ErrNotFound.
// The payload is owned by the caller and is never a shared cache buffer.
func (s *Segment) Seek(id uint64) (Record, error) {
	i := sort.Search(len(s.blocks), func(i int) bool { return s.blocks[i].last >= id })
	if i == len(s.blocks) {
		return Record{}, ErrNotFound
	}
	b := s.blocks[i]
	stored := make([]byte, int(b.size))
	if _, err := s.r.ReadAt(stored, int64(b.offset)); err != nil {
		return Record{}, err
	}
	var result Record
	found := false
	err := walkBlock(b, stored, func(r Record) {
		if !found && r.ID >= id {
			result, found = r, true
		}
	})
	// A matching prefix cannot hide corruption later in the selected block.
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrCorrupt // The validated block's last ID is >= id.
	}
	return result, nil
}

func (s *Segment) Read(id uint64) (Record, error) {
	r, err := s.Seek(id)
	if err == nil && r.ID != id {
		return Record{}, ErrNotFound
	}
	return r, err
}

// Records scans in ID order with at most one decoded block resident at a time
// unless the caller retains returned records. Each error ends the iteration.
func (s *Segment) Records() iter.Seq2[Record, error] {
	return s.recordsIn(s.coverage)
}

// recordsIn skips blocks outside the requested interval. Selected blocks are
// fully validated before delivery, including boundary records outside the range.
func (s *Segment) recordsIn(bounds Coverage) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		first := sort.Search(len(s.blocks), func(i int) bool { return s.blocks[i].last >= bounds.Start })
		for _, b := range s.blocks[first:] {
			if b.first >= bounds.End {
				return
			}
			records, err := s.readBlock(b)
			if err != nil {
				yield(Record{}, err)
				return
			}
			for _, rec := range records {
				if rec.ID < bounds.Start {
					continue
				}
				if rec.ID >= bounds.End {
					return
				}
				if !yield(rec, nil) {
					return
				}
			}
		}
	}
}

// Verify validates every frame, including data outside a particular seek.
func (s *Segment) Verify() error {
	for _, b := range s.blocks {
		stored := make([]byte, int(b.size))
		if _, err := s.r.ReadAt(stored, int64(b.offset)); err != nil {
			return err
		}
		if err := walkBlock(b, stored, nil); err != nil {
			return err
		}
	}
	return nil
}
