package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func confChangeEntry(t *testing.T, term, index uint64, cc *pb.ConfChangeV2) *pb.Entry {
	t.Helper()
	data, err := proto.Marshal(cc)
	require.NoError(t, err)
	return &pb.Entry{Term: &term, Index: &index, Type: pb.EntryConfChangeV2.Enum(), Data: data}
}

func change(typ pb.ConfChangeType, id uint64) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{Type: &typ, NodeId: &id}
}

func addNode(t *testing.T, index, id uint64, url string) *pb.Entry {
	return confChangeEntry(t, 1, index, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{change(pb.ConfChangeAddNode, id)}, Context: []byte(url)})
}

func apiURLEntry(t *testing.T, index, id uint64, url string) *pb.Entry {
	t.Helper()
	e, err := cluster.NewNodeAPIURLEntity(id, url)
	require.NoError(t, err)
	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
	require.NoError(t, err)
	term := uint64(1)
	return &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
}

// oldCluster builds a stopped node of a three-member cluster with recorded
// addresses and an id: entries 1-3 add the members, 4 announces an API
// address, all applied behind a snapshot at 3; tail (unapplied, committed)
// entries follow. Returns the directory.
func oldCluster(t *testing.T, old cluster.ClusterID, tail ...*pb.Entry) string {
	t.Helper()
	s := NewStorage(t, nil)
	s.ConfState(&pb.ConfState{Voters: []uint64{1, 2, 3}})
	applied := []*pb.Entry{
		addNode(t, 1, 1, "http://old-1:7000"),
		addNode(t, 2, 2, "http://old-2:7000"),
		addNode(t, 3, 3, "http://old-3:7000"),
		apiURLEntry(t, 4, 2, "http://old-2:8080"),
	}
	saveAndApply(t, s, applied)
	if len(tail) > 0 {
		commit := tail[len(tail)-1].GetIndex()
		require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: &commit}, tail, &pb.Snapshot{}))
	}
	for id := uint64(1); id <= 3; id++ {
		require.NoError(t, s.PutMemberPeerURL(id, []byte("http://old-"+string(rune('0'+id))+":7000")))
	}
	require.NoError(t, s.InstallLocalClusterID(old))
	require.NoError(t, s.PutReplicatedClusterID(old))
	require.NoError(t, s.SetPeersAtLastBoot(map[uint64]string{1: "http://old-1:7000"}))
	_, err := s.CreateSnapshot(3, nil)
	require.NoError(t, err)
	require.NoError(t, s.Compact(3))
	require.NotEmpty(t, s.MemberAPIURLs())
	require.NoError(t, s.Close())
	return s.path
}

func mintID(t *testing.T) cluster.ClusterID {
	t.Helper()
	id, err := cluster.NewClusterID()
	require.NoError(t, err)
	return id
}

// snapshotClusterID reads the cluster id out of the metadata db a snapshot
// carries.
func snapshotClusterID(t *testing.T, snap *pb.Snapshot) cluster.ClusterID {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snap.db")
	require.NoError(t, os.WriteFile(path, snap.GetData(), 0o600))
	bdb, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	defer func() { _ = bdb.Close() }()
	var id cluster.ClusterID
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		copy(id[:], tx.Bucket([]byte("clusterIdentity")).Get([]byte("id")))
		return nil
	}))
	return id
}

// A restored directory is a member of the new cluster and nothing in it
// leads back to the old one: both copies of the id are replaced, recorded
// addresses are gone, the saved snapshot carries the new metadata, the
// entries that held the old addresses are compacted away, and a tail entry
// announcing an old address records nothing when it applies.
func TestRestoreAs_RewritesTheDirectoryAsANewCluster(t *testing.T) {
	old, id := mintID(t), mintID(t)
	tail := apiURLEntry(t, 5, 3, "http://old-3:8080")
	dir := oldCluster(t, old, tail)

	m, err := wal.RestoreAs(dir, id, 2)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3}, m.ConfState.GetVoters())

	s := OpenStorage(t, dir, nil, nil, nil)
	defer s.Cleanup()
	local, ok, err := s.LocalClusterID()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, id, local)
	rep, ok := s.ReplicatedClusterID()
	require.True(t, ok)
	require.Equal(t, id, rep)
	require.Empty(t, s.MemberPeerURLs())
	require.Empty(t, s.MemberAPIURLs())
	_, ok, err = s.PeersAtLastBoot()
	require.NoError(t, err)
	require.False(t, ok, "the old machines' COMMITTED_PEERS are forgotten")
	node, ok, err := s.RecordedNodeID()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), node)
	require.Equal(t, uint64(5), s.AddressesIgnoredThrough())

	snap, err := s.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snap.GetMetadata().GetIndex(), "a fresh snapshot at the applied index")
	require.Equal(t, id, snapshotClusterID(t, snap), "the snapshot a joiner installs carries the new id")
	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), first, "the entries holding the old addresses are compacted away")
	_, cs, err := s.InitialState()
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3}, cs.GetVoters())

	require.NoError(t, s.ApplyCommitted(tail))
	require.Empty(t, s.MemberAPIURLs(), "an old cluster's announcement in the tail records nothing")
}

// The node id must be a member once the log's tail is counted: a member the
// tail adds is accepted, an id that never was is refused.
func TestRestoreAs_NodeIDMustBeAMemberThroughTheTail(t *testing.T) {
	old := mintID(t)
	dir := oldCluster(t, old, addNode(t, 5, 4, "http://old-4:7000"))
	backup := t.TempDir()
	require.NoError(t, os.CopyFS(backup, os.DirFS(dir)))

	_, err := wal.RestoreAs(dir, mintID(t), 7)
	require.ErrorContains(t, err, "node 7 is not a member")
	_, err = wal.RestoreAs(backup, mintID(t), 4)
	require.NoError(t, err, "node 4 joined in the tail")
}

// Recover from a joint configuration: the survivor leaves it, removes every
// other voter and learner one change at a time, and commits the whole log;
// the configuration the log then holds is the survivor alone.
func TestRecover_LeavesAJointConfigurationAndRemovesEveryoneElse(t *testing.T) {
	old := mintID(t)
	joint := confChangeEntry(t, 1, 5, &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointExplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{
			change(pb.ConfChangeRemoveNode, 3),
			change(pb.ConfChangeAddNode, 4),
			change(pb.ConfChangeAddLearnerNode, 5),
		},
	})
	dir := oldCluster(t, old, joint)

	r, err := wal.Recover(dir, 1)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4, 5}, r.Removed)
	require.Equal(t, uint64(9), r.LastIndex, "a leave and three removals after index 5")
	require.NotEqual(t, old, r.ClusterID)

	s := OpenStorage(t, dir, nil, nil, nil)
	defer s.Cleanup()
	m, err := db.ReadLogMembership(s)
	require.NoError(t, err)
	require.False(t, m.Joint())
	require.Equal(t, []uint64{1}, m.ConfState.GetVoters())
	require.Empty(t, m.ConfState.GetLearners())
	require.Contains(t, m.Removed, uint64(2))
	require.Contains(t, m.Removed, uint64(3))
	hs, _, err := s.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(9), hs.GetCommit(), "the whole log is committed")
	local, _, err := s.LocalClusterID()
	require.NoError(t, err)
	require.Equal(t, r.ClusterID, local)
	rep, _ := s.ReplicatedClusterID()
	require.Equal(t, r.ClusterID, rep)
}

// Recover runs on a voter: a learner's log can lack committed entries, and an
// id outside the configuration is a typo.
func TestRecover_RefusesALearnerOrANonMember(t *testing.T) {
	learner := confChangeEntry(t, 1, 5, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{change(pb.ConfChangeAddLearnerNode, 4)}})
	dir := oldCluster(t, mintID(t), learner)
	_, err := wal.Recover(dir, 4)
	require.ErrorContains(t, err, "learner")
	_, err = wal.Recover(dir, 9)
	require.ErrorContains(t, err, "not a member")
}

// The tail replays the way the node will apply it, guard included: after the
// guard's marker, an add-learner naming a voter is refused, so the voter
// stays a voter (raft alone would demote it).
func TestRestoreAs_ReplaysTheTailWithTheGuard(t *testing.T) {
	marker := confChangeEntry(t, 1, 5, &pb.ConfChangeV2{
		Changes: []*pb.ConfChangeSingle{change(pb.ConfChangeUpdateNode, 1)},
		Context: []byte("committed:membership-guard"),
	})
	demote := confChangeEntry(t, 1, 6, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{change(pb.ConfChangeAddLearnerNode, 2)}})
	dir := oldCluster(t, mintID(t), marker, demote)

	m, err := wal.RestoreAs(dir, mintID(t), 1)
	require.NoError(t, err)
	require.True(t, m.IsVoter(2), "the guard refused the demotion: %v", m.ConfState)
	require.Empty(t, m.ConfState.GetLearners())
}

// A directory whose event log is segmented restores too: the offline open
// recognizes the engine that wrote it.
func TestRestoreAs_SegmentedEventLog(t *testing.T) {
	dir := t.TempDir()
	segmented := wal.WithSegmentedEventLog(segmentlog.LogOptions{})
	s, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync(), segmented)
	require.NoError(t, err)
	s.ConfState(&pb.ConfState{Voters: []uint64{1}})
	entries := []*pb.Entry{addNode(t, 1, 1, "http://old-1:7000"), apiURLEntry(t, 2, 1, "http://old-1:8080")}
	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(2)}, entries, &pb.Snapshot{}))
	require.NoError(t, s.ApplyCommittedBatch(entries))
	require.NoError(t, s.Close())

	id := mintID(t)
	_, err = wal.RestoreAs(dir, id, 1)
	require.NoError(t, err)

	s, err = wal.Open(dir, nil, nil, nil, wal.WithoutFsync(), segmented)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	rep, ok := s.ReplicatedClusterID()
	require.True(t, ok)
	require.Equal(t, id, rep)
}
