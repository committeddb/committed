package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The event-log compression integration: segments compress in the background
// (the sealer), reads and restarts stay byte-faithful across the mixed
// format, DiagnoseLog understands both formats, and the offline decompress
// tool restores the downgrade-ready plain format.

func compressionTestEntry(i int) *pb.Entry {
	// Compressible, committed-shaped payloads.
	data := []byte(fmt.Sprintf(`{"entity_id":%d,"entity_state":"active","padding":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i))
	return &pb.Entry{
		Term:  proto.Uint64(1),
		Index: proto.Uint64(uint64(i)),
		Type:  pb.EntryNormal.Enum(),
		Data:  data,
	}
}

func seedEventLog(t *testing.T, s *Storage, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		require.NoError(t, s.appendEvents([]*pb.Entry{compressionTestEntry(i)}))
	}
}

func countZst(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".zst") {
			n++
		}
	}
	return n
}

// TestEventLogCompression_SealerAndReadBack: the background sealer
// compresses sealed event-log segments while the storage serves reads;
// entries read back identically live, and after a restart over the mixed
// log.
func TestEventLogCompression_SealerAndReadBack(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048))
	require.NoError(t, err)

	const n = 200
	seedEventLog(t, s, 1, n)

	eventsDir := filepath.Join(dir, "events")
	require.Eventually(t, func() bool {
		return countZst(t, eventsDir) >= 2
	}, 60*time.Second, 50*time.Millisecond, "the sealer never compressed sealed segments")

	verify := func(st *Storage) {
		t.Helper()
		for i := 1; i <= n; i++ {
			// readEventAt verifies and strips the checksum frame itself; a
			// second unframe here would reject the bare payload now that
			// unframed bytes fail loudly (the removed legacy passthrough
			// silently tolerated the old double-unframe).
			entry, rerr := st.readEventAt(uint64(i))
			require.NoError(t, rerr, "event %d", i)
			require.Contains(t, string(entry), fmt.Sprintf(`{"entity_id":%d,`, i))
		}
	}
	verify(s)
	require.NoError(t, s.Close())

	// Restart over the mixed log (some segments compressed, tail plain):
	// everything reads, and the sealer keeps working.
	s2, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048))
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()
	verify(s2)

	// DiagnoseLog is format-aware: the mixed log is clean with every record
	// counted through the compressed segments.
	d, err := DiagnoseLog(eventsDir)
	require.NoError(t, err)
	require.Equal(t, LogClean, d.Status, d.Detail)
	require.Equal(t, n, d.Records)
}

// TestEventLogCompression_DecompressNodeRoundTrip: the downgrade door — a
// compressed data dir rewrites to the plain format and reads identically.
func TestEventLogCompression_DecompressNodeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048))
	require.NoError(t, err)

	const n = 150
	seedEventLog(t, s, 1, n)
	eventsDir := filepath.Join(dir, "events")
	require.Eventually(t, func() bool {
		return countZst(t, eventsDir) >= 2
	}, 60*time.Second, 50*time.Millisecond, "the sealer never compressed sealed segments")
	require.NoError(t, s.Close())

	counts, err := DecompressNode(dir)
	require.NoError(t, err)
	require.Positive(t, counts[eventsDir], "the event log must have rewritten segments")
	require.Zero(t, countZst(t, eventsDir), "no .zst may remain")

	// The plain log reads identically (a pre-compression binary's view).
	s2, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048))
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()
	for i := 1; i <= n; i++ {
		entry, rerr := s2.readEventAt(uint64(i)) // unframes internally — see verify above
		require.NoError(t, rerr)
		require.Contains(t, string(entry), fmt.Sprintf(`{"entity_id":%d,`, i))
	}
}

// TestEventLogCompression_CorruptCompressedSegmentIsLoud: a bit-flip in a
// compressed segment fails the zstd frame checksum — DiagnoseLog classifies
// it as corruption (rebuild), never a torn tail.
func TestEventLogCompression_CorruptCompressedSegmentIsLoud(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048))
	require.NoError(t, err)
	seedEventLog(t, s, 1, 150)
	eventsDir := filepath.Join(dir, "events")
	require.Eventually(t, func() bool {
		return countZst(t, eventsDir) >= 1
	}, 60*time.Second, 50*time.Millisecond)
	require.NoError(t, s.Close())

	// Flip a byte in the middle of the first compressed segment.
	ents, err := os.ReadDir(eventsDir)
	require.NoError(t, err)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".zst") {
			p := filepath.Join(eventsDir, e.Name())
			raw, rerr := os.ReadFile(p)
			require.NoError(t, rerr)
			raw[len(raw)/2] ^= 0xFF
			require.NoError(t, os.WriteFile(p, raw, 0o640))
			break
		}
	}

	d, err := DiagnoseLog(eventsDir)
	require.NoError(t, err)
	require.Equal(t, LogCorrupt, d.Status)
	require.Contains(t, d.Detail, "zstd frame checksum")
}
