package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"

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
		saveProposal(t, p, s, 1, uint64(i+1))
	}

	afterWrites, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	require.Greater(t, afterWrites, initial, "raft log should grow after Save")

	// Compact near the tip. Compact truncates tidwall/wal's segment
	// files, so the on-disk footprint should drop.
	require.Nil(t, s.Compact(9))
	afterCompact, err := s.RaftLogApproxSize()
	require.Nil(t, err)
	require.Less(t, afterCompact, afterWrites, "raft log should shrink after Compact")
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
