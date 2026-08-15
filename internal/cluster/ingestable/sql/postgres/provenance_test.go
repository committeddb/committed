package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlushPendingStampsProvenance proves a transaction's capture provenance
// (the Begin message's commit time + xid) lands on every per-topic proposal
// of the flush — the co-transaction evidence a mixed-table transaction's
// groups share — alongside the LSN+sub SourceSeq.
func TestFlushPendingStampsProvenance(t *testing.T) {
	ch := make(chan *cluster.Proposal, 4)
	ta := &cluster.Type{ID: "topic-a"}
	tb := &cluster.Type{ID: "topic-b"}
	pending := []*cluster.Entity{
		{Type: ta, Key: []byte("a1")},
		{Type: tb, Key: []byte("b1")},
	}
	pendingBytes := 42

	const commitTS = int64(1_755_000_000_123_456_789)
	require.NoError(t, flushPending(
		context.Background(), &pending, &pendingBytes, ch,
		pglogrepl.LSN(1000), 1, commitTS, "770123"))

	pa, pb := <-ch, <-ch
	for _, p := range []*cluster.Proposal{pa, pb} {
		require.Equal(t, commitTS, p.SourceCommitUnixNano)
		require.Equal(t, "770123", p.SourceTxnID, "per-topic groups of one transaction share its xid")
	}
	require.Equal(t, uint64(1000), pa.SourceSeq)
	require.Equal(t, uint64(1001), pb.SourceSeq)

	require.Empty(t, pending, "flush resets the buffer")
	require.Zero(t, pendingBytes)
}
