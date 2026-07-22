package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestChunkTag_SensitiveToBudgetAndConfig pins the re-chunk determinism
// fingerprint (F1): the chunk_tag stamped on a checkpoint must change when any
// input that moves oversized-transaction part boundaries changes — the flush
// byte budget (the test-hook path the ticket names), or the config's rendering
// (a mapping rename) — and must be stable otherwise. A mismatch on resume is
// exactly the signal that a re-chunked replay could mis-drop a row.
func TestChunkTag_SensitiveToBudgetAndConfig(t *testing.T) {
	cfg := &sql.Config{
		Type:       &cluster.Type{ID: "t"},
		Mappings:   []sql.Mapping{{JsonName: "a", SQLColumn: "col_a"}},
		PrimaryKey: []string{"id"},
	}

	base := chunkTag(cfg)
	require.NotZero(t, base, "chunkTag must never be 0 for a real config (0 = no baseline)")
	require.Equal(t, base, chunkTag(cfg), "chunkTag must be deterministic for the same inputs")

	// A flush-budget change (the ticket's "test hook on budget") must change it.
	restore := SetTxnSoftFlushBytesForTest(sql.TxnSoftFlushBytes / 2)
	changedBudget := chunkTag(cfg)
	restore()
	require.NotEqual(t, base, changedBudget, "a flush-budget change must change the chunk tag")

	// A config rendering change (a column rename) must change it.
	cfg2 := &sql.Config{
		Type:       &cluster.Type{ID: "t"},
		Mappings:   []sql.Mapping{{JsonName: "a", SQLColumn: "col_RENAMED"}},
		PrimaryKey: []string{"id"},
	}
	require.NotEqual(t, base, chunkTag(cfg2), "a mapping change must change the chunk tag")
}

// TestFlushPending_FlagsRechunkSoftFlush pins the re-chunk half of the freeze
// (F1): when the chunking inputs changed since the checkpoint (chunkingChanged),
// a SOFT-flush — a same-coordinate multi-part piece being replayed — is flagged
// DedupUnsafe so the worker freezes rather than dropping a re-grouped row. The
// commit flush is deliberately NOT flagged (an earlier soft-flush at/below the
// highwater trips first), and an unchanged tag must never flag.
func TestFlushPending_FlagsRechunkSoftFlush(t *testing.T) {
	// forward file + no resume baseline, so the F2 lineage guard never fires;
	// only the F1 re-chunk guard is under test.
	newHandler := func(chunkingChanged bool) (*MySQLEventHandler, chan *cluster.Proposal) {
		ch := make(chan *cluster.Proposal, 1)
		h := &MySQLEventHandler{
			proposalChan:    ch,
			curFile:         "binlog.000001",
			curPos:          100,
			chunkingChanged: chunkingChanged,
			pending:         []*cluster.Entity{{Key: []byte("k"), Data: []byte("v")}},
		}
		return h, ch
	}

	t.Run("changed + soft-flush is flagged", func(t *testing.T) {
		h, ch := newHandler(true)
		require.NoError(t, h.flushPending(context.Background(), true))
		require.True(t, (<-ch).DedupUnsafe,
			"a soft-flush replayed under changed chunking must be flagged DedupUnsafe")
	})

	t.Run("changed + commit flush is not flagged", func(t *testing.T) {
		h, ch := newHandler(true)
		require.NoError(t, h.flushPending(context.Background(), false))
		require.False(t, (<-ch).DedupUnsafe,
			"the commit flush needs no flag — a replay trips on an earlier soft-flush first")
	})

	t.Run("unchanged + soft-flush is not flagged", func(t *testing.T) {
		h, ch := newHandler(false)
		require.NoError(t, h.flushPending(context.Background(), true))
		require.False(t, (<-ch).DedupUnsafe,
			"no chunking change must never flag — ordinary oversized flushes must not freeze")
	})
}
