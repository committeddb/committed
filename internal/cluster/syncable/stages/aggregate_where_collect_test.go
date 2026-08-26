package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func foldGet(t *testing.T, sts []Stage, topic, stage string) (func(key, payload string), func(key string) string, func(key string)) {
	t.Helper()
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)
	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	del := func(key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	return fold, get, del
}

// Per-emit where — SQL's count(*) FILTER (WHERE ...), the visits CTE
// verbatim: one aggregate stage emits both the total and a filtered
// count, where the old grammar needed two stages.
func TestAggregateEmitWhere(t *testing.T) {
	fold, get, del := foldGet(t, []Stage{
		{
			Name: "visits", From: "timecards", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "n", Count: true},
				{Field: "reviewed", Count: true, Where: []WhenClause{{Path: "$.billed", Equals: "true"}}},
				{Field: "total", Sum: "$.hours", Where: []WhenClause{{Expr: "$.billed = 'true'"}}},
			},
		},
	}, "timecards", "visits")

	fold("t1", `{"job":"j1","billed":"true","hours":2}`)
	fold("t2", `{"job":"j1","billed":"false","hours":3}`)
	require.Equal(t, `{"n":2,"reviewed":1,"total":2}`, get("j1"))

	// The filtered field tracks the where, membership tracks the key: a
	// t2 flip to billed re-counts it; a retraction re-shrinks.
	fold("t2", `{"job":"j1","billed":"true","hours":3}`)
	require.Equal(t, `{"n":2,"reviewed":2,"total":5}`, get("j1"))
	del("t2")
	require.Equal(t, `{"n":1,"reviewed":1,"total":2}`, get("j1"))
}

// where belongs to fold arms — on a reshape emit the condition is the
// stage's when; rejected loudly, not silently ignored.
func TestEmitWhereRejectedOnReshape(t *testing.T) {
	err := ValidateShapes([]Stage{
		{
			Name: "r", From: "t", KeyPath: []string{"$.id"},
			Emit: []Emit{{Field: "v", From: "$.v", Where: []WhenClause{{Path: "$.x", Equals: "y"}}}},
		},
	})
	require.ErrorContains(t, err, "where")
}

// collect — SQL's array_agg(DISTINCT expr), deterministically: values
// sort (numbers numerically, then strings, then bools) so equal folds
// are byte-equal; distinct dedupes by rendered value; nulls are
// skipped like every other fold arm; a qualifying-but-empty set emits
// an empty array (membership is the key's, not the field's).
func TestAggregateCollect(t *testing.T) {
	fold, get, _ := foldGet(t, []Stage{
		{
			Name: "groups", From: "workareas", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "divisions", Collect: "coalesce(nullif($.group, ''), $.name)", Distinct: true},
				{Field: "statuses", Collect: "$.status", Distinct: true},
				{Field: "n", Count: true},
			},
		},
	}, "workareas", "groups")

	fold("w1", `{"job":"j1","group":"Landscaping","name":"x","status":2}`)
	fold("w2", `{"job":"j1","group":"","name":"Mowing","status":0}`)
	fold("w3", `{"job":"j1","group":"Landscaping","name":"y","status":10}`)
	require.Equal(t,
		`{"divisions":["Landscaping","Mowing"],"n":3,"statuses":[0,2,10]}`,
		get("j1"), "distinct dedupes; numbers sort numerically (10 after 2), strings lexically")

	// A null-valued input contributes nothing to the array but still
	// counts toward membership.
	fold("w4", `{"job":"j2"}`)
	require.Equal(t, `{"divisions":[],"n":1,"statuses":[]}`, get("j2"))
}

// collect with a per-emit where — the two compose (FILTER + array_agg).
func TestAggregateCollectWithWhere(t *testing.T) {
	fold, get, _ := foldGet(t, []Stage{
		{
			Name: "g", From: "workareas", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "live", Collect: "$.name", Distinct: true, Where: []WhenClause{{Path: "$.deleted", Null: true}}},
			},
		},
	}, "workareas", "g")

	fold("w1", `{"job":"j1","name":"a","deleted":null}`)
	fold("w2", `{"job":"j1","name":"b","deleted":"2026-01-01"}`)
	require.Equal(t, `{"live":["a"]}`, get("j1"))
}

// collect is a fold arm: exactly one per emit, aggregate stages only,
// and distinct without collect is meaningless.
func TestCollectValidates(t *testing.T) {
	base := func(e Emit) []Stage {
		return []Stage{{
			Name: "g", From: "t", KeyPath: []string{"$.k"},
			Reduce: "aggregate", Emit: []Emit{e},
		}}
	}
	require.NoError(t, ValidateShapes(base(Emit{Field: "v", Collect: "$.x"})))
	require.Error(t, ValidateShapes(base(Emit{Field: "v", Collect: "$.x", Sum: "$.y"})), "one fold arm")
	require.Error(t, ValidateShapes(base(Emit{Field: "v", Count: true, Distinct: true})), "distinct needs collect")
	require.Error(t, ValidateShapes([]Stage{{
		Name: "r", From: "t", KeyPath: []string{"$.k"},
		Emit: []Emit{{Field: "v", Collect: "$.x"}},
	}}), "collect is an aggregate arm")
}
