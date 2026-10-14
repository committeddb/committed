package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlushPendingStampsProvenance proves a transaction's capture provenance
// (the Begin message's commit time + xid) lands on its proposal — one for the
// whole transaction, whatever topics it touched — alongside the LSN+sub
// SourceSeq and the bundled post-transaction position.
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
	watermark := []byte("encoded-post-txn-position")
	durable := 0
	emitted, err := flushPending(
		context.Background(), &pending, &pendingBytes, ch,
		pglogrepl.LSN(1000), 1, commitTS, "770123", watermark, func() { durable++ })
	require.NoError(t, err)
	require.True(t, emitted)

	p := <-ch
	require.Len(t, ch, 0, "a two-topic transaction is ONE proposal")
	require.Len(t, p.Entities, 2)
	require.Equal(t, commitTS, p.SourceCommitUnixNano)
	require.Equal(t, "770123", p.SourceTxnID)
	require.True(t, p.TxnScopedDedup, "the bundling dialect opts every transaction proposal into txn-scoped dedup")
	require.Equal(t, uint64(1001), p.SourceSeq,
		"the SourceSeq of the last per-topic part a split flush used, so a resume across the upgrade is never dropped as a duplicate")
	require.Equal(t, cluster.Position(watermark), p.Position,
		"the transaction bundles the post-transaction position — the watermark commits atomically with the entities")
	p.Durable()
	require.Equal(t, 1, durable, "the proposal reports the transaction durable")

	require.Empty(t, pending, "flush resets the buffer")
	require.Zero(t, pendingBytes)
}
