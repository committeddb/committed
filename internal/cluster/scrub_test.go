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
		out = append(out, [2]string{le.Type.GetID(), string(le.Key)})
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

	// remove forgets exactly (a, k1).
	remove := func(typeID string, key []byte) bool {
		return typeID == "a" && string(key) == "k1"
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

	t.Run("delete entity is always retained", func(t *testing.T) {
		// (a, k1) matches remove, but it's a delete — the tombstone must stay.
		raw := mustMarshal(NewDeleteEntity(ty("a"), []byte("k1")))
		newBytes, allRemoved, changed, err := FilterProposalEntities(raw, remove)
		require.Nil(t, err)
		require.False(t, allRemoved)
		require.False(t, changed)
		require.Nil(t, newBytes)
	})
}
