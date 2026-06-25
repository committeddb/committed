package wal_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// These tests pin the in-place snapshot install path (saveWithSnapshot /
// resetEntryLogToSnapshot). Before it existed, a Ready carrying an
// InstallSnapshot left the entry log untouched, so the follow-up entries
// (snap index+1 and up) hit appendEntries' contiguity guard against the old
// log — the Save error killed the Ready loop and the node silently stopped
// replicating. Coalesced (entries in the same Ready as the snapshot) and
// non-coalesced orderings both failed.

// installReadyStorage builds a follower that has applied entries 1..n
// (event log and applied index at n) with the same entries in its raft
// entry log — the state a healthy node is in before the leader's log
// compaction forces a snapshot install.
func installReadyStorage(t *testing.T, n int) *StorageWrapper {
	t.Helper()
	s := NewStorage(t, nil)
	terms := make([]uint64, n)
	for i := range terms {
		terms[i] = 1
	}
	ents := index(1).terms(terms...)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
	for _, e := range ents {
		require.NoError(t, s.ApplyCommitted(e))
	}
	return s
}

func installSnap(idx, term uint64) *pb.Snapshot {
	return &pb.Snapshot{
		Data: []byte("leader-bbolt"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
			Index:     proto.Uint64(idx),
			Term:      proto.Uint64(term),
		},
	}
}

func assertInstalled(t *testing.T, s *StorageWrapper, snapIdx, term, last uint64) {
	t.Helper()

	fi, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, snapIdx+1, fi, "first readable index must follow the snapshot point")

	li, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, last, li)

	// The dummy at the snapshot point must answer raft's log-matching probe
	// with the SNAPSHOT's term, not whatever the superseded entry had.
	dummyTerm, err := s.Term(snapIdx)
	require.NoError(t, err)
	require.Equal(t, term, dummyTerm)

	if last > snapIdx {
		got, err := s.Entries(snapIdx+1, last+1, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, got, int(last-snapIdx))
		require.Equal(t, snapIdx+1, got[0].GetIndex())
	}
}

// The flake/zombie shape: snapshot and follow-up entries coalesced into ONE
// Ready (raft steps the leader's MsgApp before the application consumes the
// snapshot Ready).
func TestSave_SnapshotInstallWithCoalescedEntries(t *testing.T) {
	s := installReadyStorage(t, 5)
	defer s.Cleanup()

	snap := installSnap(5, 2)
	ents := index(6).terms(2, 2)
	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(7)}, ents, snap))

	assertInstalled(t, s, 5, 2, 7)

	// Everything must survive a restart: the dummy, the mapping, the
	// post-snapshot entries, and the persisted state.
	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()
	assertInstalled(t, s2, 5, 2, 7)

	hs, cs, err := s2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(7), hs.GetCommit())
	require.Equal(t, []uint64{1, 2, 3}, cs.Voters)

	got, err := s2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(5), got.Metadata.GetIndex())
	require.Equal(t, []byte("leader-bbolt"), got.Data)
}

// The non-coalesced shape — the production catch-up path: the snapshot
// arrives in one Ready, the entries that follow it in later Readys.
func TestSave_SnapshotInstallThenLaterEntries(t *testing.T) {
	s := installReadyStorage(t, 5)
	defer s.Cleanup()

	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(5)}, nil, installSnap(5, 2)))
	assertInstalled(t, s, 5, 2, 5)

	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(8)}, index(6).terms(2, 2, 2), &defaultSnap))
	assertInstalled(t, s, 5, 2, 8)
}

// Severe lag: the snapshot leaps past this node's permanent event log. Save
// must persist and mutate NOTHING — processSnapshot fatal-exits right after
// (RestoreSnapshot's invariant check), and a restart without the rsync
// rebuild must come back with the pre-snapshot state so the leader re-sends
// the snapshot and the fatal re-fires instead of silently gapping the
// permanent event log.
func TestSave_SevereLagSnapshotPersistsNothing(t *testing.T) {
	s := installReadyStorage(t, 5)
	defer s.Cleanup()

	stateLi, err := s.StateLog.LastIndex()
	require.NoError(t, err)

	snap := installSnap(50, 3)
	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(2), Commit: proto.Uint64(50)}, index(51).terms(3), snap))

	li, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), li, "entry log must be untouched")

	stateLi2, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	require.Equal(t, stateLi, stateLi2, "state log must be untouched")

	got, err := s.Snapshot()
	require.NoError(t, err)
	require.NotEqual(t, uint64(50), got.Metadata.GetIndex(), "snapshot must not be adopted")

	// The fatal trigger the Ready loop relies on must still fire.
	err = s.RestoreSnapshot(snap)
	require.ErrorContains(t, err, "run rebuild procedure")
}

// Open completes an install that crashed between persisting the snapshot
// record and cutting the entry log over.
func TestOpen_CompletesInterruptedSnapshotInstall(t *testing.T) {
	// Crash shape 1: snapshot persisted, entry log still entirely old.
	t.Run("stale entry log", func(t *testing.T) {
		s := installReadyStorage(t, 9)

		// Forge the persisted intent the way saveWithSnapshot's appendState
		// leaves it: a Snapshot record then a HardState record, both past
		// the entry log's tail (entry log only reaches 5 below).
		snap := installSnap(9, 3)
		snapb, err := proto.Marshal(snap)
		require.NoError(t, err)
		hsb, err := proto.Marshal(&pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(1), Commit: proto.Uint64(9)})
		require.NoError(t, err)

		// Rebuild the entry log to only reach 5: simulate by removing the
		// last entries via TruncateBack on the raw log (entries 1..9 at seqs
		// 1..9).
		require.NoError(t, s.EntryLog.TruncateBack(5))

		li, err := s.StateLog.LastIndex()
		require.NoError(t, err)
		require.NoError(t, s.StateLog.Write(li+1, gobStateRecord(t, wal.Snapshot, snapb)))
		require.NoError(t, s.StateLog.Write(li+2, gobStateRecord(t, wal.HardState, hsb)))

		s2 := s.CloseAndReopenStorage(t)
		defer s2.Cleanup()

		assertInstalled(t, s2, 9, 3, 9)
		hs, _, err := s2.InitialState()
		require.NoError(t, err)
		require.Equal(t, uint64(9), hs.GetCommit())

		// And the node must be able to take replication from here.
		require.NoError(t, s2.Save(&pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(1), Commit: proto.Uint64(10)}, index(10).terms(3), &defaultSnap))
		assertInstalled(t, s2, 9, 3, 10)
	})

	// Crash shape 2: the entry log dir was renamed away (or wiped) before
	// the fresh log was written — Open must rebuild it from the snapshot
	// alone and clean up the discarded dir.
	t.Run("missing entry log", func(t *testing.T) {
		s := installReadyStorage(t, 9)

		snap := installSnap(9, 3)
		snapb, err := proto.Marshal(snap)
		require.NoError(t, err)
		hsb, err := proto.Marshal(&pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(1), Commit: proto.Uint64(9)})
		require.NoError(t, err)
		li, err := s.StateLog.LastIndex()
		require.NoError(t, err)
		require.NoError(t, s.StateLog.Write(li+1, gobStateRecord(t, wal.Snapshot, snapb)))
		require.NoError(t, s.StateLog.Write(li+2, gobStateRecord(t, wal.HardState, hsb)))

		require.NoError(t, s.Close())
		logDir := filepath.Join(s.path, "raft", "log")
		require.NoError(t, os.Rename(logDir, logDir+".discarded"))

		s2 := OpenStorage(t, s.path, s.parser, nil, nil)
		defer s2.Cleanup()

		assertInstalled(t, s2, 9, 3, 9)
		_, err = os.Stat(logDir + ".discarded")
		require.True(t, os.IsNotExist(err), "discarded entry log must be cleaned up")
	})
}
