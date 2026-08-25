package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Multi-arm fan — SQL's UNION ALL of lateral fans, the doubled-fan
// killer: one stage fans different paths for different event shapes
// (per-arm when), where the old grammar needed parallel fan stages,
// parallel aggregates, and a union merge.
func TestFanMultiArm(t *testing.T) {
	sts := []Stage{
		{
			Name: "els", From: "txn-events", KeyPath: []string{"$.wa"},
			Fan: []FanArm{
				{ForEach: "$.elements[*]", When: []WhenClause{{Path: "$.type", Equals: "created"}}},
				{ForEach: "$.added[*]", When: []WhenClause{{Path: "$.type", Equals: "elements-added"}}},
			},
			ElementKey: "$.id",
			Reduce:     "aggregate",
			Emit:       []Emit{{Field: "total", Sum: "$.amount"}, {Field: "n", Count: true}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txn-events", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("els", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// A created event fans its elements arm only.
	fold("e1", `{"type":"created","elements":[{"id":"a","wa":"w1","amount":10},{"id":"b","wa":"w1","amount":5}]}`)
	require.Equal(t, `{"n":2,"total":15}`, get("w1"))

	// An elements-added event fans the OTHER arm into the same fold.
	fold("e2", `{"type":"elements-added","added":[{"id":"c","wa":"w1","amount":7}]}`)
	require.Equal(t, `{"n":3,"total":22}`, get("w1"))

	// A foreign-shaped event matches no arm: fans nothing, changes nothing.
	fold("e3", `{"type":"annotated","elements":[{"id":"x","wa":"w1","amount":99}]}`)
	require.Equal(t, `{"n":3,"total":22}`, get("w1"))

	// Re-emitting e1 with an element gone reconciles that arm's fan.
	fold("e1", `{"type":"created","elements":[{"id":"a","wa":"w1","amount":10}]}`)
	require.Equal(t, `{"n":2,"total":17}`, get("w1"))

	// The input's tombstone retracts all its fanned elements, both arms'.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "txn-events", []byte("e1"))
	}))
	require.Equal(t, `{"n":1,"total":7}`, get("w1"))
}

// Two arms fanning EQUAL element keys from one parent stay distinct —
// the arm ordinal namespaces element identity.
func TestFanArmIdentityNamespaced(t *testing.T) {
	sts := []Stage{
		{
			Name: "both", From: "t", KeyPath: []string{"$.k"},
			Fan: []FanArm{
				{ForEach: "$.a[*]"},
				{ForEach: "$.b[*]"},
			},
			ElementKey: "$.id",
			Reduce:     "aggregate",
			Emit:       []Emit{{Field: "n", Count: true}, {Field: "src", Max: "$parent.tag"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicUpsertNow(tx, "t", []byte("e1"),
			decodePayload(t, `{"tag":"T","a":[{"id":"x","k":"k1"}],"b":[{"id":"x","k":"k1"}]}`))
	}))
	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut("both", []byte("k1"))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"n":2,"src":"T"}`, got, "same element id via different arms = two inputs (UNION ALL); $parent reaches the enclosing event at refold")
}

func TestFanValidates(t *testing.T) {
	base := Stage{
		Name: "s", From: "t", KeyPath: []string{"$.k"},
		Emit: []Emit{{Field: "v", From: "$.v"}},
	}
	both := base
	both.ForEach = "$.a[*]"
	both.Fan = []FanArm{{ForEach: "$.b[*]"}}
	require.ErrorContains(t, ValidateShapes([]Stage{both}), "exactly one of forEach or fan")

	one := base
	one.Fan = []FanArm{{ForEach: "$.a[*]"}}
	require.ErrorContains(t, ValidateShapes([]Stage{one}), "two", "a single arm is just forEach — say so")

	bad := base
	bad.Fan = []FanArm{{ForEach: "$.a"}, {ForEach: "$.b[*]"}}
	require.ErrorContains(t, ValidateShapes([]Stage{bad}), "single-valued")

	live := base
	live.Fan = []FanArm{{ForEach: "$.a[*]"}, {ForEach: "$.b[*]"}}
	live.Reduce = "liveSet"
	live.When = []WhenClause{{Path: "$.t", Equals: "c"}}
	live.DeleteWhen = []WhenClause{{Path: "$.t", Equals: "d"}}
	require.ErrorContains(t, ValidateShapes([]Stage{live}), "fan in a prior stage",
		"the fanned-liveSet rejection covers fan arms too")
}
