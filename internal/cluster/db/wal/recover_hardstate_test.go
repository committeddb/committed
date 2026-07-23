package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestOpen_RecoversOneFsyncBehindCrash is the raft-hardstate-fsync-atomicity
// regression: Save persists Entries then HardState to two separately-fsync'd
// logs, so a crash between the two fsyncs leaves the entry log one Ready ahead of
// the durable HardState — HardState.Term below the last entry's term. That fatals
// raft's start-time invariant on every restart (a rebuild-only brick). Open must
// instead truncate the over-term tail and restore the invariant so the node
// restarts cleanly.
func TestOpen_RecoversOneFsyncBehindCrash(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	s, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)

	// A normal Ready: entries 1,2 at term 1, committed, with a durable HardState.
	require.Nil(t, s.Save(
		&pb.HardState{Term: proto.Uint64(1), Vote: proto.Uint64(1), Commit: proto.Uint64(2)},
		index(1).terms(1, 1),
		&defaultSnap))

	// The crash Ready: entries 3,4 at term 2 land in the entry log, but the
	// HardState fsync is lost. An empty HardState makes appendState skip persisting
	// a new one (it stays at term 1) while the entries advance — exactly the
	// on-disk state after a crash between the two fsyncs.
	require.Nil(t, s.Save(&defaultHardState, index(3).terms(2, 2), &defaultSnap))

	// On disk now: the log is a full Ready ahead of the durable HardState.
	li, _ := s.LastIndex()
	require.Equal(t, uint64(4), li)
	lt, _ := s.Term(li)
	require.Equal(t, uint64(2), lt)
	hs, _, _ := s.InitialState()
	require.Equal(t, uint64(1), hs.GetTerm(), "durable HardState is one term behind the log")
	require.Less(t, hs.GetTerm(), lt, "the storage term-invariant is violated on disk")

	require.Nil(t, s.Close())

	// Reopen: recovery truncates the term-2 tail (indices 3,4) rather than leaving
	// a state that fatals raft's start-time invariant.
	s2, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.NoError(t, err, "reopen must recover cleanly, not error out")
	defer s2.Close()

	li2, _ := s2.LastIndex()
	require.Equal(t, uint64(2), li2, "the over-term tail (indices 3,4) must be truncated")
	lt2, _ := s2.Term(li2)
	require.Equal(t, uint64(1), lt2)
	hs2, _, _ := s2.InitialState()
	require.GreaterOrEqual(t, hs2.GetTerm(), lt2,
		"invariant holds after recovery — raft restarts instead of panicking")

	// The kept prefix (entries 1,2) is intact and re-appendable from the boundary.
	require.Nil(t, s2.Save(
		&pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(1), Commit: proto.Uint64(2)},
		index(3).terms(3),
		&defaultSnap), "the truncated log accepts fresh entries at the boundary")
	li3, _ := s2.LastIndex()
	require.Equal(t, uint64(3), li3)
}

// TestOpen_RecoversFirstSaveCrash is the empty-HardState sibling of the above: a
// BRAND-NEW node crashes during its very first Save — entries reach the entry log
// but no HardState is ever persisted. On restart the term>=1 entries outrank the
// empty (term 0) HardState, which would trip raft's start-time term invariant on
// every boot (assertStorageTermInvariant → panic loop, a rebuild-only brick needing
// a manual data-dir wipe). Open must discard the un-acked entries and come back as a
// clean empty log so the node re-bootstraps (StartNode) or re-catches-up (join).
func TestOpen_RecoversFirstSaveCrash(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()

	s, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)

	// The node's FIRST-EVER Save: entries 1,2 at term 1 land in the entry log, but
	// the HardState fsync is lost — an empty HardState makes appendState skip
	// persisting one, and none was ever written before. This is the on-disk state
	// after a crash between the entry fsync and the (first) HardState fsync.
	require.Nil(t, s.Save(&defaultHardState, index(1).terms(1, 1), &defaultSnap))

	// On disk now: entries at term 1, but no durable HardState at all.
	li, _ := s.LastIndex()
	require.Equal(t, uint64(2), li)
	lt, _ := s.Term(li)
	require.Equal(t, uint64(1), lt)
	hs, _, _ := s.InitialState()
	require.Zero(t, hs.GetTerm(), "no HardState was ever persisted")
	require.Less(t, hs.GetTerm(), lt, "the storage term-invariant is violated on disk")

	require.Nil(t, s.Close())

	// Reopen: recovery discards the un-acked entries rather than leaving a state
	// that fatals raft's start-time invariant (last==0 makes the assert a no-op).
	s2, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.NoError(t, err, "reopen must recover cleanly, not error out")
	defer s2.Close()

	li2, _ := s2.LastIndex()
	require.Equal(t, uint64(0), li2, "the un-acked entries must be discarded — empty log")
	hs2, _, _ := s2.InitialState()
	require.Zero(t, hs2.GetTerm())

	// The reset log accepts a fresh first append at index 1 exactly like a new node.
	require.Nil(t, s2.Save(
		&pb.HardState{Term: proto.Uint64(1), Vote: proto.Uint64(1), Commit: proto.Uint64(2)},
		index(1).terms(1, 1),
		&defaultSnap), "the reset log accepts fresh entries from index 1")
	li3, _ := s2.LastIndex()
	require.Equal(t, uint64(2), li3)
	lt3, _ := s2.Term(li3)
	require.Equal(t, uint64(1), lt3)
}
