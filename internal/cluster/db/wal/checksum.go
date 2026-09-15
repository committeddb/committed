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
// Framing shipped in v0.5-beta and every write path frames, so every log a
// supported deployment can hold (data-dir floor: 0.7.3-beta) is fully
// framed. The un-checksummed "trust on first read" passthrough that once
// admitted pre-framing bytes is REMOVED: absent or torn magic is corruption
// and fails loudly (ErrCorruptEntry) — which also closes the old scheme's
// documented limitation, where corruption landing in the magic bytes
// silently downgraded an entry to "legacy" and skipped verification.
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

// unframe inverts frame: it verifies the v1 magic, version, and CRC32C and
// returns the payload, or ErrCorruptEntry for anything else — a mismatched
// checksum, an unknown version, a torn header, or bytes with no frame at
// all. Unframed bytes can only be corruption or a pre-v0.5-beta log, which
// sits far below the supported data-dir floor (0.7.3-beta; see
// docs/api-compatibility.md) — never trusted content.
//
// The returned payload aliases raw (no copy); callers that retain it past
// the next Read on a NoCopy log must copy. Today every caller unmarshals
// immediately, matching the prior `log.Read` behaviour.
func unframe(raw []byte) ([]byte, error) {
	if len(raw) < frameHeaderSize || raw[0] != frameMagic[0] || raw[1] != frameMagic[1] || raw[2] != frameMagic[2] || raw[3] != frameVersion {
		return nil, ErrCorruptEntry
	}
	payload := raw[frameHeaderSize:]
	if binary.BigEndian.Uint32(raw[4:8]) != crc32.Checksum(payload, crc32cTable) {
		return nil, ErrCorruptEntry
	}
	return payload, nil
}
