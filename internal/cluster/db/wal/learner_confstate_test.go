package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// TestConfState_LearnerSurvivesRestart verifies the persistence half of
// "a restarted learner comes back as a learner": the Learners set in the
// ConfState the apply path records (Storage.ConfState) is persisted with
// the applied index (confStateBucket) and returned by InitialState after a
// close + reopen. On restart etcd/raft restores each node's role from this
// ConfState, so a learner is not silently promoted to a voter. The held
// snapshot is NOT where it lives: that snapshot keeps the membership as of
// its own index, for the peers it is served to.
func TestConfState_LearnerSurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)

	// Mirror what raft.applyConfChange does after a learner add commits:
	// record the resulting ConfState, then apply the entry that carried it
	// (the apply persists both atomically).
	cs := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	s.ConfState(cs)
	saveAndApply(t, s, []*pb.Entry{{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: pb.EntryConfChangeV2.Enum()}})

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	_, got, err := s2.InitialState()
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2}, got.Voters)
	require.ElementsMatch(t, []uint64{3}, got.Learners, "learner role must survive restart")
}
