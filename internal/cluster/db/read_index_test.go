package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// ReadIndex exposes the underlying db.Raft.ReadIndex through the test
// wrapper so raft-level read-index tests (single-node here, partitioned in
// the adversarial suite) can drive the protocol directly. Snapshots the
// raft pointer under the RLock like the other wrapper methods so a
// concurrent Restart can't tear the field read.
func (rs *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	rs.mu.RLock()
	r := rs.raft
	rs.mu.RUnlock()
	return r.ReadIndex(ctx)
}

// TestRaftReadIndex_SingleNode exercises the raft-layer ReadIndex plumbing on
// a single-node cluster: the node is its own quorum, so ReadIndex confirms on
// the next Ready and returns the current commit index. After committing user
// entries the confirmed index advances past them — proof the read sees writes
// that completed before it.
//
// Uses the raft-level harness (not the DB) because the in-memory test storage
// stubs AppliedIndex to 0; the DB-level apply catch-up is covered separately
// with real wal storage in TestLinearizableRead_SingleNode.
func TestRaftReadIndex_SingleNode(t *testing.T) {
	rafts := createRafts(1)
	defer rafts.Close()

	rafts.WaitForLeader(t)
	r := rafts[0]

	first, err := r.ReadIndex(testCtx(t))
	require.NoError(t, err, "ReadIndex on a single-node leader should confirm")
	require.Greater(t, first, uint64(0), "ReadIndex should return a non-zero confirmed index")

	// Commit a few user entries; each advances the commit index.
	for _, in := range []string{"a", "b", "c"} {
		proposeAndCheck(t, r, in)
	}

	after, err := r.ReadIndex(testCtx(t))
	require.NoError(t, err)
	require.Greater(t, after, first, "confirmed read index should advance past committed writes")
}

// TestRaftReadIndex_Concurrent confirms the per-call request-context token
// keeps concurrent ReadIndex callers from stealing each other's ReadState:
// every caller gets a valid (non-zero) index back, none hang.
func TestRaftReadIndex_Concurrent(t *testing.T) {
	rafts := createRafts(1)
	defer rafts.Close()

	rafts.WaitForLeader(t)
	r := rafts[0]

	const n = 16
	type result struct {
		idx uint64
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() {
			idx, err := r.ReadIndex(testCtx(t))
			results <- result{idx, err}
		}()
	}
	for i := 0; i < n; i++ {
		got := <-results
		require.NoError(t, got.err)
		require.Greater(t, got.idx, uint64(0))
	}
}

// TestLinearizableRead_SingleNode drives the full DB-level path on a real
// single-node DB backed by wal storage: LinearizableRead runs the ReadIndex
// round-trip AND the applied-index catch-up wait, returning nil. It still
// succeeds after committing a config, and that config is visible afterward —
// the read-after-write the linearizable read guarantees.
func TestLinearizableRead_SingleNode(t *testing.T) {
	d, _ := newWalDB(t)

	require.NoError(t, d.LinearizableRead(testCtx(t)),
		"LinearizableRead on a fresh single-node leader should succeed")

	require.NoError(t, d.ProposeType(testCtx(t), createType("foo").config))

	require.NoError(t, d.LinearizableRead(testCtx(t)),
		"LinearizableRead should still succeed after committing a write")

	tipe, err := d.ResolveType(cluster.LatestTypeRef("foo"))
	require.NoError(t, err, "a linearizable read must see the type that was just committed")
	require.Equal(t, "foo", tipe.ID)
}

// TestLinearizableRead_ContextCanceled confirms a dead context short-circuits
// the read rather than hanging. With no leader yet and a canceled context, no
// ReadState can ever come back, so the call must return an error — never block
// to the deadline.
func TestLinearizableRead_ContextCanceled(t *testing.T) {
	d, _ := newWalDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, d.LinearizableRead(ctx),
		"a canceled context must make LinearizableRead return an error, not hang")
}
