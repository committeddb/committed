package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	twal "github.com/tidwall/wal"
)

// encodeRecord returns the exact on-disk shape of one committed WAL record:
// uvarint(len(frame(payload))) + frame(payload).
func encodeRecord(payload []byte) []byte {
	data := frame(payload)
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(data)))
	return append(append([]byte{}, hdr[:n]...), data...)
}

// writeSegment writes a tidwall segment file named for startIndex holding the
// given payloads as committed-framed records, and returns its path.
func writeSegment(t *testing.T, dir string, startIndex uint64, payloads [][]byte) string {
	t.Helper()
	capHint := 0
	for _, p := range payloads {
		capHint += binary.MaxVarintLen64 + frameHeaderSize + len(p)
	}
	buf := make([]byte, 0, capHint)
	for _, p := range payloads {
		buf = append(buf, encodeRecord(p)...)
	}
	path := filepath.Join(dir, fmt.Sprintf("%020d", startIndex))
	require.NoError(t, os.WriteFile(path, buf, 0o600))
	return path
}

// TestRepairLog_TruncatesTornTail is the torn-tail regression: a partial
// trailing record bricks tidwall's Open (all prior committed entries included),
// and the offline tool truncates just the unacknowledged record so the log opens
// again with its committed entries intact.
func TestRepairLog_TruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	segPath := writeSegment(t, dir, 1, [][]byte{[]byte("one"), []byte("two"), []byte("three")})

	// Append a torn record: a uvarint claiming 100 bytes with only 5 present.
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], 100)
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.Write(append(hdr[:n], []byte("xxxxx")...))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// The torn tail bricks tidwall's Open today.
	_, openErr := twal.Open(dir, nil)
	require.Error(t, openErr, "a torn tail must fail Open (the bug this repairs)")

	// Dry run: detected, nothing modified.
	d, err := RepairLog(dir, false)
	require.NoError(t, err)
	require.Equal(t, LogTornTail, d.Status)
	require.False(t, d.Repaired)
	require.Equal(t, 3, d.Records)

	// Commit: truncate the torn record.
	d, err = RepairLog(dir, true)
	require.NoError(t, err)
	require.Equal(t, LogTornTail, d.Status)
	require.True(t, d.Repaired)

	// Now clean, and tidwall opens it with the three committed entries.
	d2, err := DiagnoseLog(dir)
	require.NoError(t, err)
	require.Equal(t, LogClean, d2.Status)
	require.Equal(t, 3, d2.Records)

	lg, err := twal.Open(dir, nil)
	require.NoError(t, err, "log must open after repair")
	last, err := lg.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)
	require.NoError(t, lg.Close())
}

// TestRepairLog_RefusesMidLogCorruption: a structurally-complete record with a
// bad checksum (a bit-flip in committed data) is NOT a torn tail. The tool must
// refuse to truncate it even with commit, and point the operator at a rebuild.
func TestRepairLog_RefusesMidLogCorruption(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")}
	segPath := writeSegment(t, dir, 1, payloads)

	raw, err := os.ReadFile(segPath)
	require.NoError(t, err)
	// Flip the last byte of record 1 (the last payload byte of "bravo") so the
	// complete record fails its checksum while records 0 and 2 stay valid.
	flipPos := len(encodeRecord(payloads[0])) + len(encodeRecord(payloads[1])) - 1
	raw[flipPos] ^= 0xFF
	require.NoError(t, os.WriteFile(segPath, raw, 0o600))

	before, err := os.ReadFile(segPath)
	require.NoError(t, err)

	d, err := RepairLog(dir, true) // commit=true, but mid-log must still be refused
	require.NoError(t, err)
	require.Equal(t, LogCorrupt, d.Status)
	require.False(t, d.Repaired)
	require.Contains(t, d.Detail, "checksum mismatch")

	after, err := os.ReadFile(segPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "a corrupt (non-torn) log must never be modified")
}

// TestDiagnoseLog_Clean: a fully valid log reports clean with the record count.
func TestDiagnoseLog_Clean(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, [][]byte{[]byte("a"), []byte("b")})
	d, err := DiagnoseLog(dir)
	require.NoError(t, err)
	require.Equal(t, LogClean, d.Status)
	require.Equal(t, 2, d.Records)
}

// TestOpenLog_WrapsTornTailWithActionableError: a torn tail makes tidwall's Open
// fail with an opaque "log corrupt"; openLog turns that into an ErrCorruptEntry
// that points the operator at `committed wal repair`, instead of a bare error
// with no metric and no runbook pointer.
func TestOpenLog_WrapsTornTailWithActionableError(t *testing.T) {
	dir := t.TempDir()
	segPath := writeSegment(t, dir, 1, [][]byte{[]byte("one"), []byte("two")})

	// Append a torn record so tidwall's Open fails.
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], 100)
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.Write(append(hdr[:n], []byte("xx")...))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = openLog(dir, "event_log", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCorruptEntry, "a corrupt Open must surface as ErrCorruptEntry")
	require.Contains(t, err.Error(), "committed wal repair", "must point at the repair CLI")
	require.Contains(t, err.Error(), "event_log")
}
