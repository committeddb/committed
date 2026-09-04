package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlush_ShapeSharedByEveryDialect pins the flush shape the three dialect
// wrappers delegate to: one proposal per topic in first-appearance order,
// every row stamped with the epoch, provenance on every group, seq called
// once per proposal in emission order, the bundled checkpoint on the LAST
// group only, and an empty buffer emitting nothing.
func TestFlush_ShapeSharedByEveryDialect(t *testing.T) {
	a, b := &cluster.Type{ID: "a"}, &cluster.Type{ID: "b"}
	pending := []*cluster.Entity{
		{Type: a, Key: []byte("1")},
		{Type: b, Key: []byte("2")},
		{Type: a, Key: []byte("3")},
	}
	pr := make(chan *cluster.Proposal, 4)
	var seqCalls []uint64
	seq := func(p *cluster.Proposal) {
		p.SourceSeq = 100 + uint64(len(seqCalls))
		seqCalls = append(seqCalls, p.SourceSeq)
	}
	prov := Provenance{CommitUnixNano: 42, TxnID: "txn-9", TxnScopedDedup: true}

	n, err := Flush(context.Background(), pending, 7, prov, seq, []byte("ckpt"), pr)
	require.NoError(t, err)
	require.Equal(t, 2, n, "two topics → two proposals")
	close(pr)

	var got []*cluster.Proposal
	for p := range pr {
		got = append(got, p)
	}
	require.Len(t, got, 2)
	require.Equal(t, "a", got[0].Entities[0].Type.ID, "first-appearance order")
	require.Len(t, got[0].Entities, 2)
	require.Equal(t, "b", got[1].Entities[0].Type.ID)
	for _, p := range got {
		for _, e := range p.Entities {
			require.Equal(t, uint64(7), e.Generation, "every row stamped with the epoch")
		}
		require.Equal(t, int64(42), p.SourceCommitUnixNano)
		require.Equal(t, "txn-9", p.SourceTxnID)
		require.True(t, p.TxnScopedDedup)
	}
	require.Equal(t, []uint64{100, 101}, seqCalls, "seq assigned in emission order")
	require.Equal(t, uint64(100), got[0].SourceSeq)
	require.Empty(t, got[0].Position, "the bundle rides the last group only")
	require.Equal(t, cluster.Position("ckpt"), got[1].Position)

	n, err = Flush(context.Background(), nil, 7, prov, seq, []byte("ckpt"), pr)
	require.NoError(t, err)
	require.Zero(t, n, "an empty buffer emits nothing")
}
