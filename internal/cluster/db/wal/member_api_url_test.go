package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// applyNodeAPIURL commits a node API-URL announcement through the real
// Save + ApplyCommitted path — the same path production runs — so the test
// exercises the handleNodeAPIURL dispatch and bucket write, not a shortcut.
func applyNodeAPIURL(t *testing.T, s *StorageWrapper, index, nodeID uint64, url string) {
	t.Helper()
	e, err := cluster.NewNodeAPIURLEntity(nodeID, url)
	require.NoError(t, err)
	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
	require.NoError(t, err)

	ent := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(index), Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{ent}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(ent))
}

func TestMemberAPIURL_ApplyAndRead(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	_, ok := s.MemberAPIURL(1)
	require.False(t, ok, "unknown node before any announce")

	applyNodeAPIURL(t, s, 1, 1, "http://n1:8080")
	applyNodeAPIURL(t, s, 2, 2, "http://n2:8080")

	url, ok := s.MemberAPIURL(1)
	require.True(t, ok)
	require.Equal(t, "http://n1:8080", url)

	require.Equal(t, map[uint64]string{
		1: "http://n1:8080",
		2: "http://n2:8080",
	}, s.MemberAPIURLs())
}

// A node whose advertised URL changed re-announces; last-writer-wins.
func TestMemberAPIURL_LastWriterWins(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	applyNodeAPIURL(t, s, 1, 1, "http://old:8080")
	applyNodeAPIURL(t, s, 2, 1, "http://new:8080")

	url, ok := s.MemberAPIURL(1)
	require.True(t, ok)
	require.Equal(t, "http://new:8080", url)
}

func TestMemberAPIURL_Delete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	applyNodeAPIURL(t, s, 1, 1, "http://n1:8080")
	require.NoError(t, s.DeleteMemberAPIURL(1))

	_, ok := s.MemberAPIURL(1)
	require.False(t, ok)

	// Idempotent: deleting an absent key is a no-op.
	require.NoError(t, s.DeleteMemberAPIURL(1))
}

// The bucket rides along in the bbolt-backed storage, so an announced URL
// survives a process restart (close + reopen on the same data dir) without
// re-applying the announce entry.
func TestMemberAPIURL_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	applyNodeAPIURL(t, s, 1, 1, "http://n1:8080")

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	url, ok := s2.MemberAPIURL(1)
	require.True(t, ok)
	require.Equal(t, "http://n1:8080", url)
}
