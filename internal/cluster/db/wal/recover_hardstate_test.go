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
