package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// A snapshot never moves this node's completed scrub bound away from what
// its own event log has been rewritten to: a node that has scrubbed further
// than the snapshot's source keeps its generation (and does not redo the
// rewrite), and the bound persists so a restart agrees.
func TestRestoreSnapshot_KeepsThisLogsGeneration(t *testing.T) {
	seed := func(s *StorageWrapper) {
		s.RegisterType(t, "u", 1, 1)
		saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
		saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 3)
		saveEntity(t, userDelete("u", "alice"), s, 1, 4)
		saveEntity(t, userUpsert("u", "carol", `{"ok":2}`), s, 1, 5)
	}
	unscrubbed := NewStorage(t, nil)
	defer unscrubbed.Cleanup()
	seed(unscrubbed)
	snap, err := unscrubbed.CreateSnapshot(5, &pb.ConfState{})
	require.NoError(t, err)
	completed, err := unscrubbed.SnapshotScrubCompleted(snap)
	require.NoError(t, err)
	require.Zero(t, completed, "the snapshot's source has scrubbed nothing")

	scrubbed := NewStorage(t, nil)
	defer scrubbed.Cleanup()
	seed(scrubbed)
	require.NoError(t, scrubbed.SetPendingScrubBoundForTest(4))
	require.NoError(t, scrubbed.RunScrubForTest(4))
	require.Equal(t, uint64(4), scrubbed.EventLogGeneration())
	scrubbedSnap, err := scrubbed.CreateSnapshot(5, &pb.ConfState{})
	require.NoError(t, err)
	completed, err = scrubbed.SnapshotScrubCompleted(scrubbedSnap)
	require.NoError(t, err)
	require.Equal(t, uint64(4), completed, "the payload carries the source's completed bound")

	require.NoError(t, scrubbed.RestoreSnapshot(snap))
	require.Never(t, func() bool { return scrubbed.EventLogGeneration() != 4 },
		300*time.Millisecond, 20*time.Millisecond, "the snapshot's lower bound must not be adopted over this log's generation")
	reopened := scrubbed.CloseAndReopenStorage(t)
	defer reopened.Cleanup()
	require.Equal(t, uint64(4), reopened.EventLogGeneration(), "and it persists")
	_, err = reopened.ActualAt(2)
	require.ErrorIs(t, err, wal.ErrActualNotFound, "the rewrite stays")
}
