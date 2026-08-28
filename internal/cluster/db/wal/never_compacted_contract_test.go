package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// TestNeverCompactedLog_ServesFromIndexOneWithVirtualDummy pins the raft Storage
// contract for a NEVER-compacted log — the state every cluster is in until
// its first compaction (age/size-triggered, default 1h/10GB). The contract
// (etcd's MemoryStorage keeps a dummy entry below the first real one for
// exactly this): FirstIndex() names the first real entry, Term(FirstIndex-1)
// answers so a leader can build the prevLog header for its very first
// sendable entry, and Entries serves from index 1. Before the fix the
// storage reported FirstIndex()=2 and Term(0)=ErrCompacted on a never-compacted
// log, so a leader replicating to a from-scratch joining member fell into
// maybeSendSnapshot with no snapshot to send — "panic: need non-empty
// snapshot" — and real-binary cluster grow was broken on any cluster
// younger than its first compaction (the day-one bootstrap-then-grow flow).
// The process-level proof is e2e/multinode's restart-after-grow test; this
// pins the storage contract directly.
func TestNeverCompactedLog_ServesFromIndexOneWithVirtualDummy(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	for i := uint64(1); i <= 4; i++ {
		saveEntityWithHardState(t, tp, s, 1, i)
	}

	assertNeverCompactedContract(t, s)

	// The contract must survive a reopen: Open recovers the compaction
	// boundary from the wal's first entry, and a never-compacted log's first entry
	// is a REAL entry at index 1, not a compaction dummy.
	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()
	assertNeverCompactedContract(t, s)
}

func assertNeverCompactedContract(t *testing.T, s *StorageWrapper) {
	t.Helper()

	fi, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), fi, "a never-compacted log serves from its genuine first entry")

	term, err := s.Term(0)
	require.NoError(t, err, "Term(FirstIndex-1) must answer — the virtual index-0 dummy")
	require.Zero(t, term, "the term below index 1 is 0 by axiom")

	ents, err := s.Entries(1, 3, 1<<20)
	require.NoError(t, err, "entry 1 must be servable to a from-scratch joiner")
	require.Len(t, ents, 2)
	require.Equal(t, uint64(1), ents[0].GetIndex())

	// A compacted index still refuses with the bare sentinel raft compares.
	_, err = s.Entries(0, 2, 1<<20)
	require.ErrorIs(t, err, raft.ErrCompacted, "index 0 is the dummy, never servable")

	// The snapshot stays empty — the whole point is that a never-compacted
	// log's leader does not need one to replicate from index 1.
	snap, err := s.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snap.GetMetadata().GetIndex())
}

// TestNeverCompactedLog_CompactionRestoresRetainedDummySemantics: after the first
// real Compact the boundary moves to the retained entry and the never-compacted
// virtual dummy retires — Term(boundary) reads the retained entry, entries
// below it refuse, and a reopen preserves it (the first wal entry now IS
// the dummy, sitting above index 1).
func TestNeverCompactedLog_CompactionRestoresRetainedDummySemantics(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	for i := uint64(1); i <= 6; i++ {
		saveEntity(t, tp, s, 1, i)
	}
	_, err = s.CreateSnapshot(5, &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, s.Compact(3))

	check := func(s *StorageWrapper) {
		t.Helper()
		fi, err := s.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(4), fi)
		_, err = s.Term(3)
		require.NoError(t, err, "the retained boundary entry answers Term")
		_, err = s.Term(2)
		require.ErrorIs(t, err, raft.ErrCompacted)
		_, err = s.Entries(3, 5, 1<<20)
		require.ErrorIs(t, err, raft.ErrCompacted)
	}
	check(s)

	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()
	check(s)
}

// saveEntityWithHardState is saveEntity with a REAL (non-empty) HardState,
// the shape every production Save carries. The shared test fixture saves an
// EMPTY HardState, which Open's first-Save-crash reconcile rightly treats as
// a torn genesis persist and discards the entry log on reopen — fine for
// tests that never reopen, fatal for this one.
func saveEntityWithHardState(t *testing.T, e *cluster.Entity, s *StorageWrapper, term, index uint64) {
	t.Helper()
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	bs, err := p.Marshal()
	require.NoError(t, err)
	ent := &pb.Entry{Term: &term, Index: &index, Type: pb.EntryNormal.Enum(), Data: bs}
	vote := uint64(1)
	hs := pb.HardState{Term: &term, Vote: &vote, Commit: &index}
	require.NoError(t, s.Save(&hs, []*pb.Entry{ent}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(ent))
}
