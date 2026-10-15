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

// TestHardState_CrashKeepsCommitAtOrAboveApplied pins the invariant raft
// needs at start: the node starts raft with Applied = its applied index, and
// raft panics ("applied is out of range") if the stored commit is below it.
// So after a crash — no Close, nothing held in memory reaches disk — the
// stored commit must cover everything applied, however the Readies were cut
// (commit-only HardState changes included, which raft's own MustSync would
// let a node skip writing).
func TestHardState_CrashKeepsCommitAtOrAboveApplied(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	tp := &cluster.Type{ID: "hs-topic", Name: "hs", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)
	ents := batchEntries(t, tp, 2, 9)
	require.NoError(t, s.Save(hardState(1, 1, 1), ents, &defaultSnap))
	for c := uint64(2); c <= 10; c++ {
		require.NoError(t, s.Save(hardState(1, 1, c), nil, &defaultSnap)) // commit-only changes
		require.NoError(t, s.ApplyCommittedBatch(ents[c-2:c-1]))
	}
	require.Equal(t, uint64(10), s.AppliedIndex())

	crashed := crashCopy(t, s)
	hs, _, err := crashed.InitialState()
	require.NoError(t, err)
	require.GreaterOrEqual(t, hs.GetCommit(), crashed.AppliedIndex(), "the stored commit must cover the applied index")
}

// TestHardState_SnapshotRecordCarriesTheCommit: raft refuses to load a
// HardState whose commit is below the snapshot's index, so every snapshot
// record is followed by the current HardState.
func TestHardState_SnapshotRecordCarriesTheCommit(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	tp := &cluster.Type{ID: "hs-topic", Name: "hs", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)
	ents := batchEntries(t, tp, 2, 9)
	require.NoError(t, s.Save(hardState(1, 1, 10), ents, &defaultSnap))
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
