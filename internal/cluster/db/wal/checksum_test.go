package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameUnframe_RoundTrip(t *testing.T) {
	payloads := [][]byte{
		{},
		[]byte("hello world"),
		bytes.Repeat([]byte{0xAB}, 4096),
		{0x08, 0x00, 0x10, 0x01}, // raftpb.Entry-shaped
		{0x24, 0x7f, 0x03},       // gob State-shaped
	}
	for _, payload := range payloads {
		got, err := unframe(frame(payload))
		require.NoError(t, err)
		require.Equal(t, payload, got)
	}
}

func TestFrame_HeaderLayout(t *testing.T) {
	payload := []byte("checkme")
	framed := frame(payload)

	require.Equal(t, []byte{0xC0, 'C', 'L'}, framed[:3], "magic")
	require.Equal(t, byte(0x01), framed[3], "version")
	require.Equal(t, crc32.Checksum(payload, crc32cTable), binary.BigEndian.Uint32(framed[4:8]), "crc32c")
	require.Equal(t, payload, framed[frameHeaderSize:], "payload")
}

func TestUnframe_UnframedBytesRejected(t *testing.T) {
	// No valid frame: every supported log is fully framed (framing shipped
	// v0.5-beta; the data-dir floor is 0.7.3-beta), so bytes without the
	// magic are corruption — or a below-floor relic — and must fail loudly,
	// never be trusted unverified. Covers the shapes the removed
	// trust-on-first-read passthrough used to admit.
	unframed := [][]byte{
		{0x08, 0x00, 0x10, 0x01, 0x18, 0x01}, // marshaled raftpb.Entry, bare
		{0x24, 0x7f, 0x03, 0x01, 0x01, 0x05}, // gob-encoded State, bare
		{},
		{0xC0},      // too short for the magic
		{0xC0, 'C'}, // ditto
	}
	for _, raw := range unframed {
		_, err := unframe(raw)
		require.ErrorIs(t, err, ErrCorruptEntry)
	}
}

func TestUnframe_PayloadCorruption(t *testing.T) {
	framed := frame([]byte("the quick brown fox"))
	framed[frameHeaderSize+3] ^= 0xFF // flip a payload byte
	_, err := unframe(framed)
	require.ErrorIs(t, err, ErrCorruptEntry)
}

func TestUnframe_CRCCorruption(t *testing.T) {
	framed := frame([]byte("the quick brown fox"))
	framed[5] ^= 0xFF // flip a byte inside the CRC field (offset 4..7)
	_, err := unframe(framed)
	require.ErrorIs(t, err, ErrCorruptEntry)
}

func TestUnframe_UnknownVersion(t *testing.T) {
	framed := frame([]byte("payload"))
	framed[3] = frameVersion + 1 // a version this build doesn't understand
	_, err := unframe(framed)
	require.ErrorIs(t, err, ErrCorruptEntry)
}

func TestUnframe_TruncatedFrame(t *testing.T) {
	// Magic present but the frame is shorter than the fixed header — a torn
	// write. Treated as corruption, not a legacy passthrough.
	_, err := unframe([]byte{0xC0, 'C', 'L', 0x01, 0x00})
	require.ErrorIs(t, err, ErrCorruptEntry)
}

func TestUnframe_MagicByteCorruptionRejected(t *testing.T) {
	// The old passthrough's documented limitation, now closed: corruption
	// landing in the magic bytes used to downgrade the entry to "legacy"
	// and skip verification entirely. With the passthrough removed it is
	// what it always was — corruption — and fails loudly.
	framed := frame([]byte("payload"))
	framed[0] ^= 0xFF // destroy the magic discriminator
	_, err := unframe(framed)
	require.ErrorIs(t, err, ErrCorruptEntry)
}
