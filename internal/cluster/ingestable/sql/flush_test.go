package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFlush_ShapeSharedByEveryDialect pins the flush shape the three dialect
// wrappers delegate to: ONE proposal for the whole flush, rows in source
// order across topics, every row stamped with the epoch, the transaction's
// provenance and bundled checkpoint on it, and an empty buffer emitting
// nothing.
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
	prov := Provenance{CommitUnixNano: 42, TxnID: "txn-9", TxnScopedDedup: true, OnDurable: func() {}}

	n, err := Flush(context.Background(), pending, 7, prov, seq, []byte("ckpt"), pr)
	require.NoError(t, err)
	require.Equal(t, 1, n, "a transaction is one proposal, whatever topics it touched")
	close(pr)

	var got []*cluster.Proposal
	for p := range pr {
		got = append(got, p)
	}
	require.Len(t, got, 1)
	p := got[0]
	keys := make([]string, len(p.Entities))
	for i, e := range p.Entities {
		keys[i] = string(e.Key)
		require.Equal(t, uint64(7), e.Generation, "every row stamped with the epoch")
	}
	require.Equal(t, []string{"1", "2", "3"}, keys, "rows in source order")
	require.Equal(t, int64(42), p.SourceCommitUnixNano)
	require.Equal(t, "txn-9", p.SourceTxnID)
	require.True(t, p.TxnScopedDedup)
	require.Equal(t, cluster.Position("ckpt"), p.Position)
	require.NotNil(t, p.OnDurable)

	n, err = Flush(context.Background(), nil, 7, prov, seq, []byte("ckpt"), pr)
	require.NoError(t, err)
	require.Zero(t, n, "an empty buffer emits nothing")
}

// TestFlush_SourceSeqOfTheLastTopicPart pins the SourceSeq rule that keeps
// the upgrade from 0.8.1 (which split a flush into one proposal per topic)
// safe: seq runs once per topic, so the merged proposal carries the SourceSeq
// of the old LAST part. If an old binary committed only some parts before a
// crash, the dedup then never drops the merged transaction (its SourceSeq is
// above every committed part's); a single-topic flush is unchanged.
func TestFlush_SourceSeqOfTheLastTopicPart(t *testing.T) {
	a, b, c := &cluster.Type{ID: "a"}, &cluster.Type{ID: "b"}, &cluster.Type{ID: "c"}
	run := func(pending []*cluster.Entity) (uint64, int) {
		calls := 0
		seq := func(p *cluster.Proposal) {
			p.SourceSeq = 500 + uint64(calls)
			calls++
		}
		pr := make(chan *cluster.Proposal, 1)
		_, err := Flush(context.Background(), pending, 1, Provenance{}, seq, nil, pr)
		require.NoError(t, err)
		return (<-pr).SourceSeq, calls
	}

	got, calls := run([]*cluster.Entity{{Type: a}, {Type: b}, {Type: a}, {Type: c}})
	require.Equal(t, 3, calls, "seq advances once per topic, as the per-topic parts did")
	require.Equal(t, uint64(502), got, "the merged proposal carries the last part's SourceSeq")

	got, calls = run([]*cluster.Entity{{Type: a}, {Type: a}})
	require.Equal(t, 1, calls)
	require.Equal(t, uint64(500), got, "a single-topic flush is unchanged")
}
