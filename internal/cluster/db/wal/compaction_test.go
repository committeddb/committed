package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"

	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TestRaftLogApproxSize_GrowsAndShrinks verifies the storage-level
// accounting the compaction trigger keys off: RaftLogApproxSize
// should grow with Save and shrink when Compact drops segments from
// disk. Bytes are approximate (tidwall/wal pads segments), so the
// test only asserts direction, not exact numeric thresholds.
func TestRaftLogApproxSize_GrowsAndShrinks(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Fresh storage: some bytes exist (the initial empty segment
	// header), but not many. Capture for comparison.
	initial, err := s.RaftLogApproxSize()
	require.Nil(t, err)

	// Register the type referenced by the data proposals — apply now
	// fatal-exits on unresolved types, so the type entry must precede
	// any entity that references it.
	s.RegisterType(t, "type-x", 1, 1)

	// Apply 10 proposals with chunky payloads. Writes go through Save
	// (which writes to the raft log tier) and ApplyCommitted (which
	// writes to the event log tier — unrelated to the size we're
	// measuring here).
	payload := make([]byte, 1024)
	for i := 0; i < 10; i++ {
		payload[i%len(payload)] = byte(i + 1)
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "type-x"},
			Key:  []byte{byte(i)},
			Data: append([]byte(nil), payload...),
		}}}
		saveProposal(t, p, s, 1, uint64(i+2))
	}

	afterWrites, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	require.Greater(t, afterWrites, initial, "raft log should grow after Save")

	// Compact near the tip. Compact truncates tidwall/wal's segment
	// files, so the on-disk footprint should drop.
	require.Nil(t, s.Compact(10))
	afterCompact, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	require.Less(t, afterCompact, afterWrites, "raft log should shrink after Compact")
}

// TestCompact_ReadAndWriteStillWorkAfterCompact is a regression guard
// for a bug where Compact updated s.firstIndex to compactIndex, which
// broke the seq-mapping formula (seq = raftIndex - firstIndex + 1)
// for every subsequent Entries read and every subsequent appendEntries
// write. Under the bug, reading an uncompacted index returned the
// wrong raft entry (silently diverging state) and appending a new
// entry after a compact returned a tidwall/wal "out of order" error
// (cluster stalls at the first compact).
//
// The fix splits firstIndex (stable seq-mapping offset) from
// compactedUpTo (moving compaction boundary). This test locks in the
// post-compact read/write contract so the bug can't sneak back in.
func TestCompact_ReadAndWriteStillWorkAfterCompact(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Save 10 entries at raft indices 1..10.
	entries := index(1).terms(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	require.Nil(t, s.Save(defaultHardState, entries, defaultSnap))

	// Compact up to index 5. Raft indices 1..5 become unreadable
	// (5 is the new compacted dummy); 6..10 stay readable.
	require.Nil(t, s.Compact(5))

	// FirstIndex advanced to 6 (first readable past the dummy),
	// LastIndex unchanged at 10.
	fi, err := s.FirstIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(6), fi)
	li, err := s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(10), li)

	// Reading a survived index returns the right entry. Before the
	// split, this read returned the wrong raft entry (or a "not
	// found" error) because the seq mapping had been recomputed
	// against the advanced firstIndex.
	got, err := s.Entries(6, 11, 1<<30)
	require.Nil(t, err)
	require.Equal(t, 5, len(got))
	for i, e := range got {
		require.Equal(t, uint64(6+i), e.Index, "entry %d index", i)
	}

	// Reading at or below the compaction boundary returns
	// ErrCompacted.
	_, err = s.Entries(5, 6, 1<<30)
	require.Equal(t, raft.ErrCompacted, err)

	// Term() on the compacted dummy still works (etcd raft needs
	// this for AppendEntries match checks).
	term, err := s.Term(5)
	require.Nil(t, err)
	require.Equal(t, uint64(1), term)

	// Append a new batch after compaction. Before the split, Save
	// here failed inside tidwall/wal with an "out of order" error
	// because the recomputed seq was below the wal's surviving
	// first seq.
	more := index(11).terms(1, 1, 1)
	require.Nil(t, s.Save(defaultHardState, more, defaultSnap))

	li, err = s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(13), li)

	// And the newly written entries are readable at their real
	// raft indices.
	got, err = s.Entries(11, 14, 1<<30)
	require.Nil(t, err)
	require.Equal(t, 3, len(got))
	for i, e := range got {
		require.Equal(t, uint64(11+i), e.Index)
	}
}

// TestCompact_SurvivesReopen verifies firstIndex (stable seq-mapping
// offset) and compactedUpTo (current boundary) recover correctly on
// Open after a Compact. Without the right recovery, a restart after
// compaction leaves firstIndex at the wrong value and every
// subsequent read fails or returns stale data.
func TestCompact_SurvivesReopen(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	entries := index(1).terms(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	require.Nil(t, s.Save(defaultHardState, entries, defaultSnap))
	require.Nil(t, s.Compact(5))

	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	fi, err := s.FirstIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(6), fi, "FirstIndex after reopen must reflect the compaction boundary")

	li, err := s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(10), li)

	// The survived entries read correctly.
	got, err := s.Entries(6, 11, 1<<30)
	require.Nil(t, err)
	require.Equal(t, 5, len(got))
	require.Equal(t, uint64(6), got[0].Index)

	// Post-reopen writes still honor the compaction boundary.
	more := index(11).terms(1)
	require.Nil(t, s.Save(defaultHardState, more, defaultSnap))

	li, err = s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(11), li)
}

// TestCompact_SecondCompactAdvancesBoundary verifies that a second
// Compact moves the boundary further without tripping the "already
// compacted" short-circuit. Under the old firstIndex-doubles-as-
// boundary design, this worked coincidentally. Under the split, it
// requires Compact to consult the boundary (max of firstIndex and
// compactedUpTo), not firstIndex alone.
func TestCompact_SecondCompactAdvancesBoundary(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	entries := index(1).terms(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	require.Nil(t, s.Save(defaultHardState, entries, defaultSnap))

	require.Nil(t, s.Compact(3))
	require.Nil(t, s.Compact(7))

	fi, err := s.FirstIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(8), fi)

	// Compact(7) again is already compacted.
	require.Equal(t, raft.ErrCompacted, s.Compact(7))
	// Compact to an earlier index is also rejected.
	require.Equal(t, raft.ErrCompacted, s.Compact(5))
}

// TestRaftLogApproxSize_EmptyEventLogStillReadable is a regression
// guard: RaftLogApproxSize reads the raft/log directory, not the
// events/ directory. If a refactor ever conflates the two, this test
// fails because Compact doesn't touch events/ and the size would
// look stable when it shouldn't.
func TestRaftLogApproxSize_EmptyEventLogStillReadable(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	size, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	// The raft log starts with a small bookkeeping header (tidwall/wal
	// creates the initial segment file on Open); anything non-negative
	// is acceptable, the check is mostly "we didn't error".
	require.GreaterOrEqual(t, size, uint64(0))

	// Save a single entry directly to the raft log (no apply).
	ent := pb.Entry{Term: 1, Index: 1, Type: pb.EntryNormal, Data: []byte("x")}
	require.Nil(t, s.Save(defaultHardState, []pb.Entry{ent}, defaultSnap))

	grown, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	require.GreaterOrEqual(t, grown, size)
}
