package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

func incarnationEntry(t *testing.T, index, id uint64, inc cluster.Incarnation) *pb.Entry {
	t.Helper()
	e, err := cluster.NewNodeIncarnationEntity(id, inc)
	require.NoError(t, err)
	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
	require.NoError(t, err)
	term := uint64(1)
	return &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
}

func mintIncarnation(t *testing.T) cluster.Incarnation {
	t.Helper()
	inc, err := cluster.NewIncarnation()
	require.NoError(t, err)
	return inc
}

// A data directory mints its incarnation at its first boot and keeps it.
func TestLocalIncarnation_MintedOnceAndKept(t *testing.T) {
	s := NewStorage(t, nil)
	inc, err := s.LocalIncarnation()
	require.NoError(t, err)
	require.False(t, inc.IsZero())
	again, err := s.LocalIncarnation()
	require.NoError(t, err)
	require.Equal(t, inc, again)

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()
	reopened, err := s2.LocalIncarnation()
	require.NoError(t, err)
	require.Equal(t, inc, reopened, "a restart keeps the incarnation")
}

// The first incarnation recorded for a node id stays recorded; a later one is
// refused, and applying that refusal on the node that proposed it stops the
// node.
func TestHandleNodeIncarnation_FirstValueWins(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	mine, err := s.LocalIncarnation()
	require.NoError(t, err)
	first, other := mintIncarnation(t), mintIncarnation(t)

	saveAndApply(t, s, []*pb.Entry{incarnationEntry(t, 1, 2, first)})
	got, ok := s.MemberIncarnation(2)
	require.True(t, ok)
	require.Equal(t, first, got)

	saveAndApply(t, s, []*pb.Entry{incarnationEntry(t, 2, 2, other)})
	got, _ = s.MemberIncarnation(2)
	require.Equal(t, first, got, "a second incarnation for node 2 is never recorded")

	saveAndApply(t, s, []*pb.Entry{incarnationEntry(t, 3, 1, first)})
	ent := incarnationEntry(t, 4, 1, mine)
	term, index := uint64(1), uint64(4)
	require.NoError(t, s.Save(&pb.HardState{Term: &term, Commit: &index}, []*pb.Entry{ent}, &defaultSnap))
	err = s.ApplyCommitted(ent)
	require.ErrorContains(t, err, "another process is running as node 1",
		"the node whose own incarnation lost stops")
}

// Restore makes the directory a new cluster's: the recorded incarnations go,
// the archive's own incarnation goes (the restored directory mints its own),
// and an announcement from the old cluster in the tail records nothing.
func TestRestoreAs_DropsIncarnations(t *testing.T) {
	old, id := mintID(t), mintID(t)
	applied := incarnationEntry(t, 5, 3, mintIncarnation(t))
	tail := incarnationEntry(t, 6, 2, mintIncarnation(t))
	dir := oldCluster(t, old, applied, tail)
	s := OpenStorage(t, dir, nil, nil, nil)
	archived, err := s.LocalIncarnation()
	require.NoError(t, err)
	require.NoError(t, s.ApplyCommitted(applied))
	_, ok := s.MemberIncarnation(3)
	require.True(t, ok)
	require.NoError(t, s.Close())

	_, err = wal.RestoreAs(dir, id, 2)
	require.NoError(t, err)

	r := OpenStorage(t, dir, nil, nil, nil)
	defer r.Cleanup()
	restored, err := r.LocalIncarnation()
	require.NoError(t, err)
	require.NotEqual(t, archived, restored, "a restored directory mints its own incarnation")
	_, ok = r.MemberIncarnation(3)
	require.False(t, ok, "the old cluster's recorded incarnations are dropped")
	require.NoError(t, r.ApplyCommitted(tail))
	_, ok = r.MemberIncarnation(2)
	require.False(t, ok, "an old cluster's announcement in the tail records nothing")
}
