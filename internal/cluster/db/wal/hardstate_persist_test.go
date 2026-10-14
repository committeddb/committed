package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

func hardState(term, vote, commit uint64) *pb.HardState {
	return &pb.HardState{Term: proto.Uint64(term), Vote: proto.Uint64(vote), Commit: proto.Uint64(commit)}
}

// crashCopy copies a live storage's directory as a crash would leave it: no
// Close, so nothing the node holds only in memory reaches disk.
func crashCopy(t *testing.T, s *StorageWrapper) *StorageWrapper {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "crashed")
	require.NoError(t, os.CopyFS(dst, os.DirFS(s.path)))
	c := OpenStorage(t, dst, parser.New(), nil, nil)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func stateRecords(t *testing.T, s *StorageWrapper) uint64 {
	t.Helper()
	li, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	return li
}

// TestHardState_CommitOnlyIsNotWritten: a Ready whose HardState moved only
// its commit index writes no state record (and so no state-log fsync); a
// term or vote change does, as raft requires before its messages go out.
func TestHardState_CommitOnlyIsNotWritten(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	require.NoError(t, s.Save(hardState(1, 1, 0), nil, &defaultSnap))
	before := stateRecords(t, s)
	for c := uint64(1); c <= 20; c++ {
		require.NoError(t, s.Save(hardState(1, 1, c), nil, &defaultSnap))
	}
	require.Equal(t, before, stateRecords(t, s), "commit-only HardStates are not written")

	require.NoError(t, s.Save(hardState(2, 0, 20), nil, &defaultSnap))
	require.Equal(t, before+1, stateRecords(t, s), "a term change is written")
	require.NoError(t, s.Save(hardState(2, 3, 20), nil, &defaultSnap))
	require.Equal(t, before+2, stateRecords(t, s), "a vote change is written")
}

// TestHardState_CrashKeepsTheLastWrittenCommit: after a crash the node
// starts from the commit index written with the last term or vote change —
// raft relearns the rest from the leader — while a clean Close writes the
// current one.
func TestHardState_CrashKeepsTheLastWrittenCommit(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	require.NoError(t, s.Save(hardState(1, 1, 2), nil, &defaultSnap))
	for c := uint64(3); c <= 9; c++ {
		require.NoError(t, s.Save(hardState(1, 1, c), nil, &defaultSnap))
	}

	crashed := crashCopy(t, s)
	hs, _, err := crashed.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(1), hs.GetTerm())
	require.Equal(t, uint64(1), hs.GetVote())
	require.Equal(t, uint64(2), hs.GetCommit())

	reopened := s.CloseAndReopenStorage(t)
	defer reopened.Cleanup()
	hs, _, err = reopened.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(9), hs.GetCommit(), "a clean Close writes the current commit")
}

// TestHardState_SnapshotRecordCarriesTheCommit: raft refuses to load a
// HardState whose commit is below the snapshot's index, so every snapshot
// record is followed by the current HardState, even when the commit was
// otherwise held in memory.
func TestHardState_SnapshotRecordCarriesTheCommit(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	tp := &cluster.Type{ID: "hs-topic", Name: "hs", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)
	ents := batchEntries(t, tp, 2, 9)
	require.NoError(t, s.Save(hardState(1, 1, 1), ents, &defaultSnap))
	require.NoError(t, s.Save(hardState(1, 1, 10), nil, &defaultSnap)) // commit-only: held in memory
	require.NoError(t, s.ApplyCommittedBatch(ents))
	_, err = s.CreateSnapshot(10, nil)
	require.NoError(t, err)
	require.NoError(t, s.Save(hardState(1, 1, 10), nil, &defaultSnap)) // persists the dirty snapshot

	crashed := crashCopy(t, s)
	hs, _, err := crashed.InitialState()
	require.NoError(t, err)
	stored, err := crashed.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(10), stored.GetMetadata().GetIndex())
	require.GreaterOrEqual(t, hs.GetCommit(), stored.GetMetadata().GetIndex(),
		"the stored commit covers the stored snapshot")
}
