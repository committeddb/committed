package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// applyNodeVersion commits a node feature-level announcement through the real
// Save + ApplyCommitted path — the same path production runs — so the test
// exercises the handleNodeVersion dispatch and bucket write, not a shortcut.
func applyNodeVersion(t *testing.T, s *StorageWrapper, index, nodeID, level uint64) {
	t.Helper()
	e, err := cluster.NewNodeVersionEntity(nodeID, level)
	require.NoError(t, err)
	bs, err := (&cluster.Proposal{Entities: []*cluster.Entity{e}}).Marshal()
	require.NoError(t, err)

	ent := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(index), Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{ent}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(ent))
}

func TestMemberVersion_ApplyAndRead(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	_, ok := s.MemberVersion(1)
	require.False(t, ok, "unknown node before any announce")
	require.Empty(t, s.MemberVersions())

	applyNodeVersion(t, s, 1, 1, 1)
	applyNodeVersion(t, s, 2, 2, 3)

	lvl, ok := s.MemberVersion(1)
	require.True(t, ok)
	require.Equal(t, uint64(1), lvl)

	require.Equal(t, map[uint64]uint64{1: 1, 2: 3}, s.MemberVersions())
}

// An upgraded binary re-announces a higher level; last-writer-wins.
func TestMemberVersion_LastWriterWins(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	applyNodeVersion(t, s, 1, 1, 1)
	applyNodeVersion(t, s, 2, 1, 2)

	lvl, ok := s.MemberVersion(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), lvl, "the higher re-announced level wins")
}

func TestMemberVersion_Delete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	applyNodeVersion(t, s, 1, 1, 1)
	require.NoError(t, s.DeleteMemberVersion(1))

	_, ok := s.MemberVersion(1)
	require.False(t, ok)

	// Idempotent: deleting an absent key is a no-op.
	require.NoError(t, s.DeleteMemberVersion(1))
}

// The bucket rides along in the bbolt-backed storage, so an announced level
// survives a process restart (close + reopen on the same data dir) without
// re-applying the announce entry — the durability the gate reads on restart.
func TestMemberVersion_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	applyNodeVersion(t, s, 1, 1, 1)

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	lvl, ok := s2.MemberVersion(1)
	require.True(t, ok)
	require.Equal(t, uint64(1), lvl)
}

// The bucket rides along in a snapshot (bbolt is serialized whole into
// CreateSnapshot), so a node brought up via InstallSnapshot inherits every
// member's announced level and can gate emission correctly. RestoreSnapshot
// requires the target's permanent event log to already reach the snapshot index
// (the event log is rebuilt out of band; the snapshot carries only bbolt), so
// the target here applies its OWN two entries to advance to index 2 — and the
// restore must REPLACE that state with the source's bucket, proving the mapping
// travels in the snapshot rather than being reconstructed locally.
func TestMemberVersion_SurvivesSnapshot(t *testing.T) {
	source := NewStorage(t, nil)
	defer source.Cleanup()
	applyNodeVersion(t, source, 1, 1, 1)
	applyNodeVersion(t, source, 2, 2, 1)

	snap, err := source.CreateSnapshot(2, &pb.ConfState{Voters: []uint64{1, 2, 3}})
	require.NoError(t, err)

	target := NewStorage(t, nil)
	defer target.Cleanup()
	// Advance the target's event log to the snapshot index with unrelated
	// announcements (nodes 8, 9) that the restore must discard.
	applyNodeVersion(t, target, 1, 9, 7)
	applyNodeVersion(t, target, 2, 8, 7)
	require.NoError(t, target.RestoreSnapshot(snap))

	require.Equal(t, map[uint64]uint64{1: 1, 2: 1}, target.MemberVersions(),
		"the snapshot's bucket replaced the target's own announcements")
}
