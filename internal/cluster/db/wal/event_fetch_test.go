package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openFetchPeer(t *testing.T, dir string, sealer time.Duration) *Storage {
	t.Helper()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithEventSegmentSize(2048), WithSealerIdleInterval(sealer))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// compressOldestSealed takes one sealer step by hand, so the peer's layout
// holds exactly one compressed sealed segment and the rest plain — a mix
// the background sealer's timing under load cannot be trusted to leave.
func compressOldestSealed(t *testing.T, s *Storage) {
	t.Helper()
	s.eventMu.RLock()
	log := s.eventLog
	s.eventMu.RUnlock()
	did, err := log.CompressNextSealed()
	require.NoError(t, err)
	require.True(t, did, "the seeded log must hold a plain sealed segment to compress")
}

// stage copies the serving node's sealed files somewhere a receiver could
// have downloaded them to, keeping the segment names.
func stage(t *testing.T, sealed []EventSegment) []string {
	t.Helper()
	dir := t.TempDir()
	out := make([]string, 0, len(sealed))
	for _, sg := range sealed {
		dst := filepath.Join(dir, filepath.Base(sg.Path))
		require.NoError(t, copyFile(sg.Path, dst))
		out = append(out, dst)
	}
	return out
}

// fetchTail encodes the peer's tail records as a fetch would carry them.
func fetchTail(t *testing.T, peer *Storage, lay EventLayout) []byte {
	t.Helper()
	data, last, err := peer.encodeRecords(lay.TailFirstSeq, lay.LastSeq, 1<<30)
	require.NoError(t, err)
	require.Equal(t, lay.LastSeq, last)
	return data
}

func verifyEvents(t *testing.T, s *Storage, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		entry, err := s.readEventAt(uint64(i))
		require.NoError(t, err, "event %d", i)
		require.Contains(t, string(entry), fmt.Sprintf(`{"entity_id":%d,`, i))
	}
	require.Equal(t, uint64(n), s.eventIndex.Load())
	require.Equal(t, uint64(1), s.firstEventIndex.Load())
}

// An empty node takes a peer's event log whole: the sealed segments as files
// (some already compressed, as on disk), the tail as records — and reads
// every event back, with its bounds and its offline diagnosis agreeing.
func TestEventFetch_EmptyReceiverAdoptsSegmentsAndTail(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	const n = 300
	seedEventLog(t, peer, 1, n)
	compressOldestSealed(t, peer)

	_, err := peer.EventLayout()
	require.ErrorIs(t, err, ErrLayoutNotFrozen, "the layout is only valid under a freeze")
	release := peer.FreezeEventLayout()
	defer release()
	lay, err := peer.EventLayout()
	require.NoError(t, err)
	require.NotEmpty(t, lay.Sealed)
	require.Equal(t, uint64(n), lay.LastSeq)
	compressed := 0
	for _, sg := range lay.Sealed {
		if sg.Compressed {
			compressed++
		}
	}
	require.Positive(t, compressed, "the layout must carry compressed segments as they are")

	recvDir := t.TempDir()
	recv := openFetchPeer(t, recvDir, time.Hour)
	require.NoError(t, recv.AdoptEventSegments(stage(t, lay.Sealed)))
	require.NoError(t, recv.AppendFetchedRecords(fetchTail(t, peer, lay)))
	verifyEvents(t, recv, n)

	seq, err := recv.EventSeqForIndex(150)
	require.NoError(t, err)
	require.Equal(t, uint64(150), seq)
	idx, err := recv.EventRaftIndexAt(n)
	require.NoError(t, err)
	require.Equal(t, uint64(n), idx)

	// Overlap is harmless: re-appending the tail changes nothing.
	require.NoError(t, recv.AppendFetchedRecords(fetchTail(t, peer, lay)))
	verifyEvents(t, recv, n)

	require.NoError(t, recv.Close())
	d, err := DiagnoseLog(filepath.Join(recvDir, "events"))
	require.NoError(t, err)
	require.Equal(t, LogClean, d.Status, d.Detail)
	require.Equal(t, n, d.Records)
}

// A stale node whose own log ends exactly at a peer's segment boundary
// adopts only the segments past it; one that ends elsewhere is refused and
// left untouched (its caller falls back to records, or replaces the log).
func TestEventFetch_StaleReceiverAlignedAndMisaligned(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	const n = 300
	seedEventLog(t, peer, 1, n)
	release := peer.FreezeEventLayout()
	defer release()
	lay, err := peer.EventLayout()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(lay.Sealed), 3)
	j := len(lay.Sealed) / 2
	boundary := int(lay.Sealed[j].FirstSeq) - 1 // the stale node holds exactly the sealed segments before j

	aligned := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, aligned, 1, boundary)
	require.NoError(t, aligned.AdoptEventSegments(stage(t, lay.Sealed[j:])))
	require.NoError(t, aligned.AppendFetchedRecords(fetchTail(t, peer, lay)))
	verifyEvents(t, aligned, n)

	stale := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, stale, 1, boundary-2)
	before := listNames(t, stale.eventLogDir)
	err = stale.AdoptEventSegments(stage(t, lay.Sealed[j:]))
	require.ErrorIs(t, err, ErrSegmentsMisaligned)
	verifyEvents(t, stale, boundary-2)
	require.Equal(t, before, listNames(t, stale.eventLogDir), "a refused adoption must leave the events dir as it was")

	// A receiver whose own segments cycle at different points has a
	// partially filled tail at the boundary; it stays, becomes a sealed
	// segment of its own size, and the adopted files follow it.
	partial, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithEventSegmentSize(4096), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = partial.Close() })
	seedEventLog(t, partial, 1, boundary)
	require.NoError(t, partial.AdoptEventSegments(stage(t, lay.Sealed[j:])))
	require.NoError(t, partial.AppendFetchedRecords(fetchTail(t, peer, lay)))
	verifyEvents(t, partial, n)
}

// A staged file that does not scan clean — a byte flipped inside a record,
// or a truncated compressed file — is refused before anything is copied.
func TestEventFetch_RefusesACorruptStagedSegment(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, peer, 1, 300)
	compressOldestSealed(t, peer)
	release := peer.FreezeEventLayout()
	defer release()
	lay, err := peer.EventLayout()
	require.NoError(t, err)

	damage := map[bool]func([]byte) []byte{
		true:  func(b []byte) []byte { return b[:len(b)-7] },           // a truncated compressed frame
		false: func(b []byte) []byte { b[len(b)/2] ^= 0xFF; return b }, // a flipped byte inside a record
	}
	seen := map[bool]bool{}
	for i, sg := range lay.Sealed {
		if seen[sg.Compressed] {
			continue
		}
		seen[sg.Compressed] = true
		staged := stage(t, lay.Sealed[i:i+1])
		data, err := os.ReadFile(staged[0])
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(staged[0], damage[sg.Compressed](data), 0o600))

		recv := openFetchPeer(t, t.TempDir(), time.Hour)
		require.NoError(t, recv.AdoptEventSegments(stage(t, lay.Sealed[:i])), "the clean prefix adopts")
		before := listNames(t, recv.eventLogDir)
		require.Error(t, recv.AdoptEventSegments(staged), "staged %s must be refused", filepath.Base(staged[0]))
		require.Equal(t, before, listNames(t, recv.eventLogDir), "a refused file leaves the events dir as it was")
	}
	require.Len(t, seen, 2, "the peer must have both a plain and a compressed sealed segment")
}

func listNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
