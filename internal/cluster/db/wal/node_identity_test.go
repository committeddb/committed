package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// The node-local cluster id is written once and survives restart; installing
// the same id again is a no-op and a different one is refused.
func TestLocalClusterID_InstallOnce(t *testing.T) {
	s := NewStorage(t, nil)
	_, ok, err := s.LocalClusterID()
	require.NoError(t, err)
	require.False(t, ok, "a fresh node has no cluster id")

	id, err := cluster.NewClusterID()
	require.NoError(t, err)
	require.NoError(t, s.InstallLocalClusterID(id))
	require.NoError(t, s.InstallLocalClusterID(id), "installing the same id again is a no-op")

	other, err := cluster.NewClusterID()
	require.NoError(t, err)
	require.ErrorContains(t, s.InstallLocalClusterID(other), id.String(), "a node's cluster id never changes in place")

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()
	got, ok, err := s2.LocalClusterID()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, id, got)
}

// A snapshot install replaces the metadata db, never the node-local id: the
// node keeps its own, and the snapshot's replicated copy comes with it.
func TestLocalClusterID_NotReplacedBySnapshot(t *testing.T) {
	source := NewStorage(t, nil)
	defer source.Cleanup()
	sourceID, err := cluster.NewClusterID()
	require.NoError(t, err)
	require.NoError(t, source.PutReplicatedClusterID(sourceID))
	snap, err := source.CreateSnapshot(0, &pb.ConfState{Voters: []uint64{1, 2, 3}})
	require.NoError(t, err)

	target := NewStorage(t, nil)
	defer target.Cleanup()
	targetID, err := cluster.NewClusterID()
	require.NoError(t, err)
	require.NoError(t, target.InstallLocalClusterID(targetID))
	require.NoError(t, target.RestoreSnapshot(snap))

	local, ok, err := target.LocalClusterID()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, targetID, local, "the node-local id survives the install")
	replicated, ok := target.ReplicatedClusterID()
	require.True(t, ok)
	require.Equal(t, sourceID, replicated, "the replicated copy is the snapshot's — the caller compares the two")
}

func TestPeersAtLastBoot_RoundTrip(t *testing.T) {
	s := NewStorage(t, nil)
	_, ok, err := s.PeersAtLastBoot()
	require.NoError(t, err)
	require.False(t, ok)

	peers := map[uint64]string{1: "http://n1:9022", 2: "http://n2:9022"}
	require.NoError(t, s.SetPeersAtLastBoot(peers))
	moved := map[uint64]string{1: "http://n1:9022", 2: "http://n9:9022"}
	require.NoError(t, s.SetPeersAtLastBoot(moved), "each boot replaces the record")

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()
	got, ok, err := s2.PeersAtLastBoot()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, moved, got)
}

func TestReplicatedClusterID_FirstValueWins(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	_, ok := s.ReplicatedClusterID()
	require.False(t, ok)

	a, err := cluster.NewClusterID()
	require.NoError(t, err)
	b, err := cluster.NewClusterID()
	require.NoError(t, err)
	require.NoError(t, s.PutReplicatedClusterID(a))
	require.NoError(t, s.PutReplicatedClusterID(a))
	require.Error(t, s.PutReplicatedClusterID(b))
	got, ok := s.ReplicatedClusterID()
	require.True(t, ok)
	require.Equal(t, a, got)
}

// Applying the replicated id gives a node with no local id its id (a joiner);
// a later, different proposal is ignored; a node whose local id differs from
// its cluster's stops apply loudly.
func TestHandleClusterIdentity(t *testing.T) {
	t.Run("a node without an id takes the cluster's", func(t *testing.T) {
		s := NewStorage(t, nil)
		defer s.Cleanup()
		id, err := cluster.NewClusterID()
		require.NoError(t, err)
		e, err := cluster.NewClusterIdentityEntity(id)
		require.NoError(t, err)
		saveEntity(t, e, s, 1, 1)

		local, ok, err := s.LocalClusterID()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, local)

		later, err := cluster.NewClusterID()
		require.NoError(t, err)
		e2, err := cluster.NewClusterIdentityEntity(later)
		require.NoError(t, err)
		saveEntity(t, e2, s, 1, 2) // a stale second proposal: ignored, not fatal
		replicated, _ := s.ReplicatedClusterID()
		require.Equal(t, id, replicated, "first value wins")
	})

	t.Run("a node from another cluster stops", func(t *testing.T) {
		s := NewStorage(t, nil)
		defer s.Cleanup()
		mine, err := cluster.NewClusterID()
		require.NoError(t, err)
		require.NoError(t, s.InstallLocalClusterID(mine))

		theirs, err := cluster.NewClusterID()
		require.NoError(t, err)
		e, err := cluster.NewClusterIdentityEntity(theirs)
		require.NoError(t, err)
		bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
		require.NoError(t, err)
		term, index := uint64(1), uint64(1)
		ent := &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
		require.NoError(t, s.Save(&pb.HardState{Term: &term, Commit: &index}, []*pb.Entry{ent}, &defaultSnap))
		err = s.ApplyCommitted(ent)
		require.Error(t, err)
		require.ErrorContains(t, err, "belongs to another cluster")
	})
}
