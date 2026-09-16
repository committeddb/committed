// Package format owns the experimental segment encoding. It has no application
// dependencies and never interprets record payloads.
package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrCorrupt     = errors.New("segmentlog: corrupt segment")
	ErrUnsupported = errors.New("segmentlog: unsupported format")
)

const (
	HeaderSize     = 32
	FooterSize     = 32
	IndexEntrySize = 48
	FrameOverhead  = 16
	MaxPayload     = 16 << 20
	MaxBlock       = MaxPayload + FrameOverhead
	MaxBlocks      = 1 << 16
)

var (
	LE    = binary.LittleEndian
	table = crc32.MakeTable(crc32.Castagnoli)
)

func CRC(b []byte) uint32 { return crc32.Checksum(b, table) }

// CRCParts checksums consecutive regions without concatenating their buffers.
func CRCParts(parts ...[]byte) uint32 {
	var sum uint32
	for _, part := range parts {
		sum = crc32.Update(sum, table, part)
	}
	return sum
}

// AppendFrame encodes payload length, stable ID, payload, then CRC32C of all
// preceding frame bytes. Callers validate payload size before encoding.
func AppendFrame(dst []byte, id uint64, payload []byte) []byte {
	start := len(dst)
	dst = LE.AppendUint32(dst, uint32(len(payload))) // #nosec G115 -- Internal encoder contract: callers bound payloads to MaxPayload before encoding.
	dst = LE.AppendUint64(dst, id)
	dst = append(dst, payload...)
	return LE.AppendUint32(dst, CRC(dst[start:]))
}

// Frame returns a borrowed payload. It validates lengths before slicing.
func Frame(b []byte) (id uint64, payload, rest []byte, err error) {
	if len(b) < FrameOverhead {
		return 0, nil, nil, ErrCorrupt
	}
	n := uint64(LE.Uint32(b))
	if n > MaxPayload || n+FrameOverhead > uint64(len(b)) {
		return 0, nil, nil, ErrCorrupt
	}
	end := int(n) + 12
	if CRC(b[:end]) != LE.Uint32(b[end:]) {
		return 0, nil, nil, ErrCorrupt
	}
	return LE.Uint64(b[4:]), b[12:end:end], b[end+4:], nil
}
