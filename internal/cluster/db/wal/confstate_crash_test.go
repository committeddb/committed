package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestConfState_SurvivesCrashWindow pins confstate-lost-in-crash-window: a
// membership change's ConfState (what ApplyConfChange returns) is now written
// ATOMICALLY with the applied index, so a crash after the applied index is
// durable can never restart the node on a stale voter set.
//
// The scenario: raft applied a conf-change (ConfState staged via ConfState),
// then the applied index was persisted (SetAppliedIndexForTest → saveAppliedIndex,
// the atomic write). A crash then strikes BEFORE any later Save — which, under
// the old deferred (snapDirty) persist, was the only thing that would have
// flushed the ConfState. On reopen, InitialState must still return the current
// membership.
func TestConfState_SurvivesCrashWindow(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())

	cs := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	s.ConfState(cs)                                 // conf-change applied; ConfState staged
	require.NoError(t, s.SetAppliedIndexForTest(5)) // applied index persisted (atomic with ConfState)

	// Crash: reopen WITHOUT a subsequent Save.
	s2 := s.CloseAndReopenStorage(t)
	_, gotCS, err := s2.InitialState()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4}, gotCS.GetVoters(),
		"ConfState must survive a crash after the applied index persisted")
}

// TestConfState_NoConfChangeLeavesNoDurableState confirms the common path is
// untouched: a plain appliedIndex persist with no staged ConfState writes no
// ConfState, so InitialState falls back to the (empty) snapshot metadata.
func TestConfState_NoConfChangeLeavesNoDurableState(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	require.NoError(t, s.SetAppliedIndexForTest(3))
	s2 := s.CloseAndReopenStorage(t)
	_, gotCS, err := s2.InitialState()
	require.NoError(t, err)
	require.Empty(t, gotCS.GetVoters())
}
