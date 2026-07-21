package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

func TestScrubRoundTrip(t *testing.T) {
	bs, err := (&Scrub{UpperBound: 4242}).Marshal()
	require.Nil(t, err)
	got := &Scrub{}
	require.Nil(t, got.Unmarshal(bs))
	require.Equal(t, uint64(4242), got.UpperBound)
}

func TestNewScrubEntity(t *testing.T) {
	e, err := NewScrubEntity(99)
	require.Nil(t, err)
	require.True(t, IsScrub(e.Type.ID))
	require.True(t, IsInternal(e.Type.ID), "scrub is a built-in/internal type, hidden from syncables")
	require.False(t, e.IsDelete())

	s := &Scrub{}
	require.Nil(t, s.Unmarshal(e.Data))
	require.Equal(t, uint64(99), s.UpperBound)
}

// decodeEntities unmarshals a marshaled proposal at the clusterpb level and
// returns its (typeID, key) pairs in order — mirrors what the scrubber's
// rewrite sees.
func decodeEntities(t *testing.T, raw []byte) [][2]string {
	t.Helper()
	lp := &clusterpb.LogProposal{}
	require.Nil(t, proto.Unmarshal(raw, lp))
	out := make([][2]string, 0, len(lp.LogEntities))
	for _, le := range lp.LogEntities {
		out = append(out, [2]string{le.Type.GetID(), string(logEntityView(le).key)})
	}
	return out
}

func TestFilterProposalEntities(t *testing.T) {
	ty := func(id string) *Type { return &Type{ID: id, Version: 1} }
	mustMarshal := func(es ...*Entity) []byte {
		bs, err := (&Proposal{Entities: es}).Marshal()
		require.Nil(t, err)
		return bs
	}

	// remove forgets exactly (a, k1) — the RTBF shape: never removes a delete
	// (the tombstone must survive), so the predicate guards on !isDelete.
	remove := func(typeID string, key []byte, isDelete bool) bool {
		return !isDelete && typeID == "a" && string(key) == "k1"
	}

	t.Run("none removed keeps verbatim", func(t *testing.T) {
		raw := mustMarshal(
			NewUpsertEntity(ty("a"), []byte("k2"), []byte(`{}`)),
			NewUpsertEntity(ty("b"), []byte("k1"), []byte(`{}`)),
		)
		newBytes, allRemoved, changed, err := FilterProposalEntities(raw, remove)
		require.Nil(t, err)
		require.False(t, allRemoved)
		require.False(t, changed)
		require.Nil(t, newBytes)
	})

	t.Run("all removed drops record", func(t *testing.T) {
		raw := mustMarshal(NewUpsertEntity(ty("a"), []byte("k1"), []byte(`{"pii":true}`)))
		newBytes, allRemoved, changed, err := FilterProposalEntities(raw, remove)
		require.Nil(t, err)
		require.True(t, allRemoved)
		require.True(t, changed)
		require.Nil(t, newBytes)
	})

	t.Run("partial re-marshals survivors", func(t *testing.T) {
		raw := mustMarshal(
			NewUpsertEntity(ty("a"), []byte("k1"), []byte(`{"pii":true}`)), // removed
			NewUpsertEntity(ty("b"), []byte("k9"), []byte(`{"keep":1}`)),   // kept
			NewUpsertEntity(ty("c"), []byte("k1"), []byte(`{"keep":2}`)),   // kept (type c)
		)
		newBytes, allRemoved, changed, err := FilterProposalEntities(raw, remove)
		require.Nil(t, err)
		require.False(t, allRemoved)
		require.True(t, changed)
		require.NotNil(t, newBytes)
		require.Equal(t, [][2]string{{"b", "k9"}, {"c", "k1"}}, decodeEntities(t, newBytes))
	})

	t.Run("delete retention is the caller's choice", func(t *testing.T) {
		// The RTBF-shaped predicate spares deletes: (a, k1) matches the
		// type/key but is a delete, so the tombstone stays.
		raw := mustMarshal(NewDeleteEntity(ty("a"), []byte("k1")))
		newBytes, allRemoved, changed, err := FilterProposalEntities(raw, remove)
		require.Nil(t, err)
		require.False(t, allRemoved)
		require.False(t, changed)
		require.Nil(t, newBytes)

		// A metadata-GC-shaped predicate that does NOT spare deletes drops the
		// same delete — the system-tombstone path can compact a superseded
		// internal delete.
		dropDeletes := func(typeID string, key []byte, isDelete bool) bool {
			return typeID == "a" && string(key) == "k1"
		}
		newBytes, allRemoved, changed, err = FilterProposalEntities(raw, dropDeletes)
		require.Nil(t, err)
		require.True(t, allRemoved)
		require.True(t, changed)
		require.Nil(t, newBytes)
	})
}

func TestForEachProposalEntity(t *testing.T) {
	ty := func(id string) *Type { return &Type{ID: id, Version: 1} }
	raw, err := (&Proposal{Entities: []*Entity{
		NewUpsertEntity(ty("a"), []byte("k1"), []byte(`{}`)),
		NewDeleteEntity(ty("b"), []byte("k2")),
	}}).Marshal()
	require.Nil(t, err)

	type ref struct {
		typeID   string
		key      string
		data     string
		isDelete bool
	}
	var got []ref
	require.Nil(t, ForEachProposalEntity(raw, func(typeID string, key, data []byte, isDelete bool) error {
		got = append(got, ref{typeID, string(key), string(data), isDelete})
		return nil
	}))
	require.Equal(t, []ref{
		{"a", "k1", `{}`, false},
		{"b", "k2", string(delete), true},
	}, got)
}
