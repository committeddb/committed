package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// The removed ids and the guard's switch-on point are first-wins (a
// re-delivered entry keeps the original index) and survive restart.
func TestMembershipGuardState_FirstWins(t *testing.T) {
	s := NewStorage(t, nil)
	_, on := s.MembershipGuardIndex()
	require.False(t, on)
	require.Empty(t, s.RemovedMembers())

	require.NoError(t, s.RecordRemovedMember(4, 10))
	require.NoError(t, s.RecordRemovedMember(4, 20))
	require.NoError(t, s.RecordRemovedMember(5, 30))
	require.NoError(t, s.SetMembershipGuardIndex(15))
	require.NoError(t, s.SetMembershipGuardIndex(25))

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()
	require.Equal(t, map[uint64]uint64{4: 10, 5: 30}, s2.RemovedMembers())
	at, on := s2.MembershipGuardIndex()
	require.True(t, on)
	require.Equal(t, uint64(15), at)
}

// A snapshot from a release that predates a bucket installs a metadata db
// without it; the install recreates it, so the conf-change pass can still
// write (a missing bucket there is fatal).
func TestRestoreSnapshot_RecreatesBucketsAnOlderReleaseLacks(t *testing.T) {
	source := NewStorage(t, nil)
	defer source.Cleanup()
	require.NoError(t, source.DropBucketForTest("removedMembers"))
	require.NoError(t, source.DropBucketForTest("membershipGuard"))
	require.NoError(t, source.DropBucketForTest("clusterIdentity"))
	snap, err := source.CreateSnapshot(0, &pb.ConfState{Voters: []uint64{1}})
	require.NoError(t, err)

	target := NewStorage(t, nil)
	defer target.Cleanup()
	require.NoError(t, target.RestoreSnapshot(snap))
	require.NoError(t, target.RecordRemovedMember(2, 7))
	require.NoError(t, target.SetMembershipGuardIndex(8))
	require.Equal(t, map[uint64]uint64{2: 7}, target.RemovedMembers())
}
