package wal_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// fillBulkLog registers a Revision-kind user type (retained by metadata GC,
// never counted as backlog) and appends n big entities of it, so the event log
// dwarfs any metadata trickle. Returns the next free raft index.
func fillBulkLog(t *testing.T, s *StorageWrapper, n int, idx uint64) uint64 {
	t.Helper()
	saveEntity(t, userType(t, "bulk", cluster.EntityKindRevision), s, 1, idx)
	idx++
	big := bytes.Repeat([]byte("x"), 2048)
	for i := 0; i < n; i++ {
		saveEntity(t, cluster.NewUpsertEntity(&cluster.Type{ID: "bulk"}, []byte(fmt.Sprintf("k%d", i)), big), s, 1, idx)
		idx++
	}
	return idx
}

// TestHasScrubBacklog_MetadataVolumeGate pins the reclaimable-volume gate: the
// metadata-GC scrub is an O(total-log) rewrite (2 full reads + a full rewrite +
// a transient ~2× disk spike, on EVERY node), so a steady trickle of tiny
// checkpoint bumps on a LARGE log must not trigger it — pre-gate, the fixed
// count threshold (crossed in seconds under sync load) fired a full-log
// rewrite every scheduler tick, forever. Only when the estimated reclaimable
// bytes are a meaningful fraction of the log does the metadata term fire.
func TestHasScrubBacklog_MetadataVolumeGate(t *testing.T) {
	defer wal.SetMetadataBacklogThresholdForTest(4)()
	defer wal.SetMetadataScrubMinLogBytesForTest(2048)()

	s := NewStorage(t, nil)
	defer s.Cleanup()
	require.NoError(t, s.SeedSyncableConfigForTest("A"))

	idx := fillBulkLog(t, s, 8, 1) // log ≈ 16 KiB+ of retained bulk

	// A trickle of checkpoint bumps: over the COUNT threshold (4), but the
	// reclaimable estimate (~80 B each) is far under logSize/16.
	for i := 0; i < 6; i++ {
		saveEntity(t, syncIndex(t, "A", uint64(i+1)), s, 1, idx)
		idx++
	}
	require.False(t, s.HasScrubBacklog(),
		"a tiny metadata trickle on a large log must NOT trigger the O(log) rewrite")

	// A genuinely compactable stream — big Snapshot-kind user entities — pushes
	// the reclaimable estimate over the ratio: now the rewrite pays for itself.
	saveEntity(t, userSnapshotType(t, "snapstream"), s, 1, idx)
	idx++
	big := bytes.Repeat([]byte("y"), 2048)
	for i := 0; i < 3; i++ {
		saveEntity(t, cluster.NewUpsertEntity(&cluster.Type{ID: "snapstream"}, []byte("same-key"), big), s, 1, idx)
		idx++
	}
	require.True(t, s.HasScrubBacklog(),
		"substantial superseded-snapshot volume must trigger the metadata scrub")
}

// TestHasScrubBacklog_RTBFNotVolumeGated pins that RTBF erasure is exempt from
// the volume gate: erasure is legally urgent, so a single unscrubbed delete
// tombstone triggers a scrub regardless of how little the rewrite reclaims.
func TestHasScrubBacklog_RTBFNotVolumeGated(t *testing.T) {
	defer wal.SetMetadataBacklogThresholdForTest(4)()
	defer wal.SetMetadataScrubMinLogBytesForTest(2048)()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	idx := fillBulkLog(t, s, 8, 1)
	require.False(t, s.HasScrubBacklog(), "no backlog yet")

	// One user-defined delete → one RTBF tombstone. Volume is irrelevant.
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "bulk"}, []byte("k0")), s, 1, idx)
	require.True(t, s.HasScrubBacklog(),
		"a single RTBF tombstone must trigger a scrub — erasure is never volume-gated")
}
