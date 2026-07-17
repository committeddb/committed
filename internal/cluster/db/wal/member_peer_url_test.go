package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// The peer-URL bucket is written directly from the apply path (raft.applyConfChange),
// not via a log entity, so these tests call the storage methods directly rather
// than committing an entity.

func TestMemberPeerURL_PutReadDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	require.Empty(t, s.MemberPeerURLs(), "no members known before any add")

	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))
	require.NoError(t, s.PutMemberPeerURL(3, []byte("http://n3:2380")))
	require.Equal(t, map[uint64]string{
		2: "http://n2:2380",
		3: "http://n3:2380",
	}, s.MemberPeerURLs())

	// Last-writer-wins per id (a peer re-added at a new address).
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2-new:2380")))
	require.Equal(t, "http://n2-new:2380", s.MemberPeerURLs()[2])

	require.NoError(t, s.DeleteMemberPeerURL(2))
	require.Equal(t, map[uint64]string{3: "http://n3:2380"}, s.MemberPeerURLs())

	// Idempotent: deleting an absent key is a no-op.
	require.NoError(t, s.DeleteMemberPeerURL(2))
}

// SurvivesRestart is the durability guarantee the whole fix rests on: the peer
// URLs persist across a process restart (close + reopen on the same data dir),
// so a restarted node can reconcile its transport from them instead of the stale
// static COMMITTED_PEERS set.
func TestMemberPeerURL_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	require.NoError(t, s.PutMemberPeerURL(2, []byte("http://n2:2380")))

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	require.Equal(t, map[uint64]string{2: "http://n2:2380"}, s2.MemberPeerURLs())
}

// SurvivesSnapshot proves the R3-MEMBER-2 half: the bucket rides along in a
// snapshot (bbolt is serialized whole into CreateSnapshot), so a node brought up
// via InstallSnapshot inherits the peer URLs and can connect its transport to
// every current member — including ones added while it was lagging.
func TestMemberPeerURL_SurvivesSnapshot(t *testing.T) {
	source := NewStorage(t, nil)
	defer source.Cleanup()
	require.NoError(t, source.PutMemberPeerURL(2, []byte("http://n2:2380")))
	require.NoError(t, source.PutMemberPeerURL(3, []byte("http://n3:2380")))

	snap, err := source.CreateSnapshot(0, &pb.ConfState{Voters: []uint64{1, 2, 3}})
	require.NoError(t, err)

	target := NewStorage(t, nil)
	defer target.Cleanup()
	require.NoError(t, target.RestoreSnapshot(snap))

	require.Equal(t, map[uint64]string{
		2: "http://n2:2380",
		3: "http://n3:2380",
	}, target.MemberPeerURLs())
}
