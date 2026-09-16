package format

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Codec identifies a decoder on disk. Encoder effort is not a format property.
type Codec uint16

const (
	Plain Codec = iota
	Zstd
)

// Encoder is owned by one segment writer, with no background workers.
type Encoder struct{ zstd *zstd.Encoder }

// NewEncoder accepts 0 for plain or zstd effort levels 1 (fast) through 4 (best).
// The caller validates the public encoding options before construction.
func NewEncoder(level int) (*Encoder, error) {
	e := &Encoder{}
	if level == 0 {
		return e, nil
	}
	var err error
	e.zstd, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
	return e, err
}

func (e *Encoder) Close() {
	if e.zstd != nil {
		_ = e.zstd.Close()
	}
}

// Encode falls back to plain storage if compression does not save bytes. Both
// encodings are bounded by the decoded block limit. Plain output aliases input.
func (e *Encoder) Encode(data []byte) (Codec, []byte) {
	if e.zstd == nil {
		return Plain, data
	}
	encoded := e.zstd.EncodeAll(data, nil)
	if len(encoded) >= len(data) {
		return Plain, data
	}
	return Zstd, encoded
}

// Decode validates size metadata before allocation and bounds decompression by
// the declared size, the package maximum, and a fixed maximum decoder window.
// The caller verifies the stored-byte checksum before calling Decode.
func Decode(codec Codec, stored []byte, decoded uint32) ([]byte, error) {
	if decoded < FrameOverhead || decoded > MaxBlock || len(stored) == 0 || len(stored) > MaxBlock {
		return nil, ErrCorrupt
	}
	switch codec {
	case Plain:
		if len(stored) != int(decoded) {
			return nil, ErrCorrupt
		}
		return stored, nil
	case Zstd:
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(MaxBlock)), zstd.WithDecoderMaxWindow(1<<20),
			zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return nil, err
		}
		defer d.Close()
		data, err := d.DecodeAll(stored, make([]byte, 0, int(decoded)))
		if err != nil {
			return nil, fmt.Errorf("%w: zstd: %v", ErrCorrupt, err)
		}
		if len(data) != int(decoded) {
			return nil, ErrCorrupt
		}
		return data, nil
	default:
		return nil, ErrUnsupported
	}
}
