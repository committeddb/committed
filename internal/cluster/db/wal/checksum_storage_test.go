package wal_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// segmentName matches tidwall/wal's segment files: a 20-digit, zero-padded
// start index (e.g. 00000000000000000001).
var segmentName = regexp.MustCompile(`^\d{20}$`)

// TestStorage_EntriesChecksummedRoundTrip writes entries through the real
// Save path and reads them back via ents (EntryLog.Read + wal.Unframe +
// Unmarshal), proving the frame round-trips end-to-end, and asserts the
// on-disk bytes carry the v1 magic.
func TestStorage_EntriesChecksummedRoundTrip(t *testing.T) {
	want := index(1).terms(2, 3, 4)
	s := NewStorage(t, want)
	defer s.Cleanup()

	requireEntriesEqual(t, want, s.ents(t))

	raw, err := s.EntryLog.Read(1)
	require.NoError(t, err)
	require.Equal(t, []byte{0xC0, 'C', 'L'}, raw[:3], "on-disk entry should carry the checksum-frame magic")
}

// TestStorage_CorruptEntryDetectedOnReopen flips a byte in the entry-log
// segment on disk (the last entry's final payload byte — simulated bit rot)
// and asserts the next Open surfaces ErrCorruptEntry. Open reads the boundary
// entries during recovery, so a corrupt last entry fails the reopen — exactly
// the replay-time detection the ticket calls for (the node fatal-exits here).
func TestStorage_CorruptEntryDetectedOnReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-crc-corrupt-")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	s, err := wal.Open(dir, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, s.Save(&defaultHardState, index(1).terms(1, 1, 1), &defaultSnap))
	require.NoError(t, s.Close())

	flipLastByte(t, filepath.Join(dir, "raft", "log"))

	_, err = wal.Open(dir, nil, nil, nil)
	require.ErrorIs(t, err, wal.ErrCorruptEntry)
}

// flipLastByte XORs the final byte of the single segment file under segDir.
// For our small test logs that byte is the last entry's last payload byte,
// so the flip corrupts a checksummed payload without disturbing tidwall's
// length framing (which would error structurally before our CRC runs).
func flipLastByte(t *testing.T, segDir string) {
	t.Helper()

	dirents, err := os.ReadDir(segDir)
	require.NoError(t, err)

	var seg string
	for _, e := range dirents {
		if !e.IsDir() && segmentName.MatchString(e.Name()) {
			seg = filepath.Join(segDir, e.Name())
		}
	}
	require.NotEmpty(t, seg, "no segment file found under %s", segDir)

	data, err := os.ReadFile(seg)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	data[len(data)-1] ^= 0xFF
	require.NoError(t, os.WriteFile(seg, data, 0o600))
}
