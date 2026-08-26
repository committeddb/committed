package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The endgame field defect: a stage join whose dimension is a MERGE
// stage must inherit the merge's RESOLVED key space — the merged sides'
// unanimous normalize — not the merge's own (empty) declarations. The
// field A/B: an anti-join against the fan-direct producer suppressed,
// while the identical topology fronted by a merge did not, because the
// join reference stayed UPPERCASE against the merge's lowered adopted
// keys.
func TestMergeFrontedJoinInheritsNormalize(t *testing.T) {
	sts := []Stage{
		{
			Name: "pa", From: "pay-a", KeyPath: []string{"$.pair"}, Normalize: "lower",
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "pb", From: "pay-b", KeyPath: []string{"$.pair"}, Normalize: "lower",
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "billed", Merge: []MergeEntry{{Stage: "pa"}, {Stage: "pb"}},
			Emit: []Emit{{Field: "n", From: "$.pa.n"}},
		},
		{
			Name: "unbilled", From: "visits", KeyPath: []string{"$.vid"},
			Joins: []Join{{From: "billed", On: []string{"$.pair"}, Absent: true}},
			Emit:  []Emit{{Field: "vid", From: "$.vid"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("unbilled", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// No billed row anywhere: the visit participates (vacuous absence).
	fold("visits", "v1", `{"vid":"V1","pair":"PAIR-9"}`)
	require.Equal(t, `{"vid":"V1"}`, get("V1"))

	// A payment arrives with mixed-case pair: pa keys it LOWERED, the
	// merge adopts the lowered key. The visit's UPPERCASE reference must
	// render in that inherited space and match — presence retracts.
	fold("pay-a", "m1", `{"pair":"Pair-9","n":1}`)
	require.Empty(t, get("V1"),
		"arrival through the merge must retract the anti-joined input — the join inherits the merge's RESOLVED normalize")

	// The payment retracts entirely: the merge key dies, the visit heals
	// back in (the un-retraction direction through the merge front).
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "pay-a", []byte("m1"))
	}))
	require.Equal(t, `{"vid":"V1"}`, get("V1"), "departure through the merge must heal the anti-joined input back in")
}

// Same inheritance, keyType half: a join reference against a merge of
// number-keyed stages must coerce through the merged sides' unanimous
// keyType ("5.0000" references key 5).
func TestMergeFrontedJoinInheritsOnType(t *testing.T) {
	sts := []Stage{
		{
			Name: "qa", From: "quote-a", KeyPath: []string{"$.id"}, KeyType: []string{"number"},
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "qb", From: "quote-b", KeyPath: []string{"$.id"}, KeyType: []string{"number"},
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "quoted", Merge: []MergeEntry{{Stage: "qa"}, {Stage: "qb"}},
			Emit: []Emit{{Field: "n", From: "$.qa.n"}},
		},
		{
			Name: "unquoted", From: "facts", KeyPath: []string{"$.fid"},
			Joins: []Join{{From: "quoted", On: []string{"$.ref"}, Absent: true}},
			Emit:  []Emit{{Field: "fid", From: "$.fid"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("unquoted", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// The quote keys by the NUMBER 5 (canonical digits "5"); the fact
	// references it as the string "5.0000" — only the inherited keyType
	// coerces them onto the same key.
	fold("quote-a", "m1", `{"id":5,"n":1}`)
	fold("facts", "f1", `{"fid":"f1","ref":"5.0000"}`)
	require.Empty(t, get("f1"),
		"the reference must coerce through the merge's RESOLVED keyType and match — presence suppresses the anti-join")
}

// ProbeKey renders a caller's key parts in a stage's RESOLVED key space
// — for a merge stage that means the merged sides' unanimous arity,
// keyType, and normalize (a merge declares none of its own).
func TestProbeKeyMergeStage(t *testing.T) {
	sts := []Stage{
		{
			Name: "pa", From: "pay-a", KeyPath: []string{"$.pair"}, Normalize: "lower",
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "pb", From: "pay-b", KeyPath: []string{"$.pair"}, Normalize: "lower",
			Emit: []Emit{{Field: "n", From: "$.n"}},
		},
		{
			Name: "billed", Merge: []MergeEntry{{Stage: "pa"}, {Stage: "pb"}},
			Emit: []Emit{{Field: "n", From: "$.pa.n"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))

	// The merge's resolved arity is 1 (inherited), so a one-part probe
	// renders — normalized into the inherited space.
	key, ok, err := ProbeKey(sts, 2, []string{"PAIR-9"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, OutKey([]string{"pair-9"}), key)

	// Arity is judged against the RESOLVED space, not the merge's empty
	// keyPath — a two-part probe is a loud arity error, not "keys by 0".
	_, _, err = ProbeKey(sts, 2, []string{"a", "b"})
	require.ErrorContains(t, err, "1 part(s)")
}
