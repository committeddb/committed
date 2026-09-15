package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The snapshot raft serves a lagging peer carries the membership as of ITS
// index, however many conf changes have applied since: a later one is what
// the NEXT snapshot stamps. Stamping the held snapshot with a later
// membership makes the peer restore past a conf change it then applies
// again from the log — raft's "can't leave a non-joint config" panic on a
// learner joining while the cluster's membership moves.
func TestSnapshot_ConfStateStaysAsOfItsIndex(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	for i := uint64(1); i <= 3; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("x")}
		require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{e}, nil))
		require.NoError(t, s.appendEvents([]*pb.Entry{e}))
	}
	require.NoError(t, s.saveAppliedIndex(3))
	s.appliedIndex.Store(3)

	joint := &pb.ConfState{Voters: []uint64{1, 2, 3}, VotersOutgoing: []uint64{1, 2, 3}, Learners: []uint64{4}, AutoLeave: proto.Bool(true)}
	s.ConfState(joint)
	snap, err := s.CreateSnapshot(3, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(joint, snap.Metadata.ConfState), "the snapshot stamps the membership as of its index")

	// The auto-leave applies at a later index.
	left := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	s.ConfState(left)

	served, err := s.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), served.Metadata.GetIndex())
	require.True(t, proto.Equal(joint, served.Metadata.ConfState),
		"the snapshot served to a peer keeps the membership as of index 3, not the later one")

	next, err := s.CreateSnapshot(3, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(left, next.Metadata.ConfState), "the next snapshot stamps the latest membership")
}

// An installed snapshot's membership is the node's membership from then on
// — raft restores it without a conf change of its own — so the node's next
// snapshot stamps it, not whatever it last applied before the install.
func TestSnapshot_InstalledMembershipIsWhatTheNextSnapshotStamps(t *testing.T) {
	leader, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = leader.Close() })
	grown := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	for i := uint64(1); i <= 5; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("x")}
		require.NoError(t, leader.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{e}, nil))
		require.NoError(t, leader.appendEvents([]*pb.Entry{e}))
	}
	require.NoError(t, leader.saveAppliedIndex(5))
	leader.appliedIndex.Store(5)
	leader.ConfState(grown)
	snap, err := leader.CreateSnapshot(5, nil)
	require.NoError(t, err)

	// A follower that had applied an older membership installs it.
	follower, err := Open(t.TempDir(), nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = follower.Close() })
	follower.ConfState(&pb.ConfState{Voters: []uint64{1, 2, 3}})
	for i := uint64(1); i <= 5; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("x")}
		require.NoError(t, follower.appendEvents([]*pb.Entry{e}))
	}
	require.NoError(t, follower.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(5)}, nil, snap))
	require.NoError(t, follower.RestoreSnapshot(snap))

	next, err := follower.CreateSnapshot(5, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(grown, next.Metadata.ConfState), "the installed membership, not the one applied before the install")
}
