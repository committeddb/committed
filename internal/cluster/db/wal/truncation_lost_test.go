package wal_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// proposalEntry builds a raft EntryNormal carrying a marshaled
// cluster.Proposal with the given RequestID and no entities — enough for
// truncation detection, which only decodes the RequestID and never resolves
// types.
func proposalEntry(t *testing.T, term, index, requestID uint64) pb.Entry {
	t.Helper()
	bs, err := (&cluster.Proposal{RequestID: requestID}).Marshal()
	require.NoError(t, err)
	return pb.Entry{Term: term, Index: index, Type: pb.EntryNormal, Data: bs}
}

// TestTruncationLostCallback drives the wal_storage truncation path
// directly: write entries [1,2,3], overwrite [2,3] with higher-term
// conflicting entries, and assert the lost-callback received exactly the
// non-zero RequestIDs of the overwritten entries (the unit-test success
// criterion in propose-truncation-detection.md).
func TestTruncationLostCallback(t *testing.T) {
	var got [][]uint64
	cb := func(ids []uint64) { got = append(got, ids) }

	dir, err := os.MkdirTemp("", "wal-lost-cb-")
	require.NoError(t, err)
	ws, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync(), wal.WithLostCallback(cb))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ws.Close()
		_ = os.RemoveAll(dir)
	})

	// Entry 1 carries RequestID 0 (a system/fire-and-forget proposal with no
	// waiter); entries 2 and 3 carry real waiter RequestIDs. The clean append
	// must not invoke the callback at all.
	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 1, 1, 0),
		proposalEntry(t, 1, 2, 100),
		proposalEntry(t, 1, 3, 200),
	}, defaultSnap))
	require.Empty(t, got, "a non-conflicting append must not signal lost")

	// A higher-term leader's AppendEntries conflicts at index 2, truncating
	// the old tail [2,3] before appending its own.
	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 2, 2, 999),
		proposalEntry(t, 2, 3, 998),
	}, defaultSnap))

	require.Len(t, got, 1, "truncation must invoke the callback exactly once")
	require.ElementsMatch(t, []uint64{100, 200}, got[0],
		"callback gets the truncated entries' non-zero RequestIDs, not the survivors' or the new entries'")
}

// TestTruncationLostCallbackZeroOnlyTail proves a truncated tail that
// carries only RequestID-0 (fire-and-forget) proposals produces no callback
// invocation: there are no waiters to signal, so the empty set is dropped
// rather than delivered.
func TestTruncationLostCallbackZeroOnlyTail(t *testing.T) {
	var calls int
	cb := func([]uint64) { calls++ }

	dir, err := os.MkdirTemp("", "wal-lost-cb-zero-")
	require.NoError(t, err)
	ws, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync(), wal.WithLostCallback(cb))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ws.Close()
		_ = os.RemoveAll(dir)
	})

	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 1, 1, 0),
		proposalEntry(t, 1, 2, 0),
	}, defaultSnap))
	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 2, 2, 0),
	}, defaultSnap))

	require.Zero(t, calls, "a tail of only RequestID-0 entries has no waiters to signal")
}

// TestTruncationNoCallbackWhenUnset is a guard that the truncation path is
// inert when no callback is registered — the production default before
// db.New wires db.notifyLost, and the happy-path-cost guarantee in the
// ticket. It would panic on the nil callback if the gate were wrong.
func TestTruncationNoCallbackWhenUnset(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-lost-cb-unset-")
	require.NoError(t, err)
	ws, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ws.Close()
		_ = os.RemoveAll(dir)
	})

	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 1, 1, 100),
		proposalEntry(t, 1, 2, 200),
	}, defaultSnap))
	require.NoError(t, ws.Save(defaultHardState, []pb.Entry{
		proposalEntry(t, 2, 2, 999),
	}, defaultSnap))
}
