package wal

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/committeddb/committed/internal/cluster"
)

// ErrCorruptEntry is the wal package's alias for cluster.ErrCorruptEntry. The
// canonical sentinel lives in cluster so the db sync worker can classify a
// corrupt read as fatal without importing wal (an import cycle). unframe returns
// it on a CRC mismatch; openLog wraps it on a corrupt Open. See its cluster
// definition (and docs/operations/rebuild.md) for the torn-tail-vs-bitflip
// recovery split.
var ErrCorruptEntry = cluster.ErrCorruptEntry

// On-disk frame for a checksummed WAL entry (format v1):
//
//	[magic 0xC0 'C' 'L'][version 0x01][crc32c BE, 4 bytes][payload...]
//
// The magic's first byte (0xC0) is the forward-compat discriminator. No
// payload we have ever written can begin with it: a marshaled raftpb.Entry
// (entry + event logs) always starts with 0x08, and a gob-encoded State
// (state log) always starts with 0x24. So unframe can tell a new v1 frame
// from a legacy un-checksummed entry by the leading byte alone, with no
// persisted format flag — which means the scheme also survives snapshot
// restore and compaction untouched. Legacy entries are read unverified
// ("trust on first read") and age out as the log compacts.
var (
	frameMagic   = [3]byte{0xC0, 'C', 'L'}
	frameVersion = byte(0x01)
)

const frameHeaderSize = len(frameMagic) + 1 + 4 // magic + version + crc32c

// crc32cTable is the Castagnoli polynomial, hardware-accelerated on every
// modern amd64/arm64 CPU (well under the fsyncs already on the write path).
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// frame wraps payload in a v1 checksum frame. The returned slice is a fresh
// allocation; payload is not retained.
func frame(payload []byte) []byte {
	out := make([]byte, frameHeaderSize+len(payload))
	out[0], out[1], out[2] = frameMagic[0], frameMagic[1], frameMagic[2]
	out[3] = frameVersion
	binary.BigEndian.PutUint32(out[4:8], crc32.Checksum(payload, crc32cTable))
	copy(out[frameHeaderSize:], payload)
	return out
}

// unframe inverts frame. When raw carries the v1 magic it verifies the
// CRC32C and returns the payload, or ErrCorruptEntry on mismatch / unknown
// version / a header too short to be valid. When the magic is absent the
// bytes are a legacy un-checksummed entry and are returned unchanged with a
// nil error (trust on first read).
//
// The returned payload aliases raw (no copy); callers that retain it past
// the next Read on a NoCopy log must copy. Today every caller unmarshals
// immediately, matching the prior `log.Read` behaviour.
func unframe(raw []byte) ([]byte, error) {
	if len(raw) < 3 || raw[0] != frameMagic[0] || raw[1] != frameMagic[1] || raw[2] != frameMagic[2] {
		// No magic: legacy entry written before checksums existed.
		return raw, nil
	}
	if len(raw) < frameHeaderSize || raw[3] != frameVersion {
		return nil, ErrCorruptEntry
	}
	payload := raw[frameHeaderSize:]
	if binary.BigEndian.Uint32(raw[4:8]) != crc32.Checksum(payload, crc32cTable) {
		return nil, ErrCorruptEntry
	}
	return payload, nil
}
