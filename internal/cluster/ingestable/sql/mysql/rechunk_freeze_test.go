package mysql

import (
	"context"
	"encoding/binary"
	"hash/fnv"
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

	base := chunkTag(cfg, sql.TxnSoftFlushBytes)
	require.NotZero(t, base, "chunkTag must never be 0 for a real config (0 = no baseline)")
	require.Equal(t, base, chunkTag(cfg, sql.TxnSoftFlushBytes), "chunkTag must be deterministic for the same inputs")

	// A flush-budget change (the ticket's "test hook on budget") must change it.
	require.NotEqual(t, base, chunkTag(cfg, sql.TxnSoftFlushBytes/2),
		"a flush-budget change must change the chunk tag")

	// A config rendering change (a column rename) must change it.
	cfg2 := &sql.Config{
		Type:       &cluster.Type{ID: "t"},
		Mappings:   []sql.Mapping{{JsonName: "a", SQLColumn: "col_RENAMED"}},
		PrimaryKey: []string{"id"},
	}
	require.NotEqual(t, base, chunkTag(cfg2, sql.TxnSoftFlushBytes), "a mapping change must change the chunk tag")
}

// TestChunkTag_SingleTopicByteCompat locks the single-topic wire format: a
// one-topic config must hash to the EXACT pre-multi-topic byte sequence (budget,
// rendering version, then mappings, 0xff, primaryKey, type id — with NO
// between-specs separator), so a chunk_tag a pre-routing binary persisted on a
// checkpoint still matches on resume and does not falsely freeze the worker.
func TestChunkTag_SingleTopicByteCompat(t *testing.T) {
	cfg := &sql.Config{
		Type:       &cluster.Type{ID: "topic-1"},
		Mappings:   []sql.Mapping{{JsonName: "a", SQLColumn: "col_a"}, {JsonName: "b", SQLColumn: "col_b"}},
		PrimaryKey: []string{"id", "ordering"},
	}
	budget := sql.TxnSoftFlushBytes

	// Replicate the exact legacy byte sequence a pre-routing binary hashed.
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(budget)) //nolint:gosec // G115: test budget is a positive constant
	_, _ = h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], chunkRenderingVersion)
	_, _ = h.Write(buf[:])
	for _, m := range cfg.Mappings {
		_, _ = h.Write([]byte(m.JsonName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(m.SQLColumn))
		_, _ = h.Write([]byte{0})
	}
	_, _ = h.Write([]byte{0xff})
	for _, pk := range cfg.PrimaryKey {
		_, _ = h.Write([]byte(pk))
		_, _ = h.Write([]byte{0})
	}
	_, _ = h.Write([]byte(cfg.Type.ID))
	want := h.Sum64()

	require.Equal(t, want, chunkTag(cfg, budget),
		"single-topic chunkTag must match the legacy byte format so checkpoints stay valid across the multi-topic upgrade")
}

// TestChunkTag_HashesAllTopics proves every spec feeds the fingerprint: a change to
// the SECOND topic's mapping changes the tag (a hash of only Topics[0] would miss
// it), and a two-topic config differs from either topic alone.
func TestChunkTag_HashesAllTopics(t *testing.T) {
	specA := sql.TopicSpec{Type: &cluster.Type{ID: "a"}, Mappings: []sql.Mapping{{JsonName: "x", SQLColumn: "x"}}, PrimaryKey: []string{"id"}}
	specB := sql.TopicSpec{Type: &cluster.Type{ID: "b"}, Mappings: []sql.Mapping{{JsonName: "y", SQLColumn: "y"}}, PrimaryKey: []string{"id"}}
	specBRenamed := specB
	specBRenamed.Mappings = []sql.Mapping{{JsonName: "y", SQLColumn: "y_RENAMED"}}

	two := &sql.Config{Topics: []sql.TopicSpec{specA, specB}}
	twoRenamed := &sql.Config{Topics: []sql.TopicSpec{specA, specBRenamed}}
	onlyA := &sql.Config{Topics: []sql.TopicSpec{specA}}

	base := chunkTag(two, sql.TxnSoftFlushBytes)
	require.NotEqual(t, base, chunkTag(twoRenamed, sql.TxnSoftFlushBytes),
		"a change to the SECOND topic's mapping must change the tag — every spec is hashed")
	require.NotEqual(t, base, chunkTag(onlyA, sql.TxnSoftFlushBytes),
		"adding a topic must change the tag")
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
