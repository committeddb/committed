package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func harness(t *testing.T, sts []Stage, stage string) (func(topic, key, payload string), func(key string) string, func(topic, key string)) {
	t.Helper()
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
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	del := func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	return fold, get, del
}

// Reference lookup — a join that names its row can read it. The
// alive_pw shape: one stage, two fk joins that FILTER (their where)
// and PULL (their alias) — where the old grammar needed lift-and-rekey
// scaffolding stages per lookup.
func TestJoinAsPullsReferencedRow(t *testing.T) {
	fold, get, del := harness(t, []Stage{
		{
			Name: "alive", From: "workareas", KeyPath: []string{"$.id"},
			Joins: []Join{
				{Topic: "projects", On: []string{"$.projectId"}, As: "project", Where: []WhenClause{{Path: "$.sold", Equals: "true"}}},
				{Topic: "groups", On: []string{"$.groupId"}, As: "grp"},
			},
			Emit: []Emit{
				{Field: "tenant", From: "$.project.tenantId"},
				{Field: "pricing", From: "$.grp.pricingType"},
				{Field: "quoted", Expr: "coalesce($.quotedPrice, 0)"},
			},
		},
	}, "alive")

	// Dimensions first, then the input: both lookups resolve.
	fold("projects", "p1", `{"sold":"true","tenantId":7}`)
	fold("groups", "g1", `{"pricingType":2}`)
	fold("workareas", "w1", `{"id":"w1","projectId":"p1","groupId":"g1","quotedPrice":50}`)
	require.Equal(t, `{"pricing":2,"quoted":50,"tenant":7}`, get("w1"))

	// The looked-up row CHANGES: dependents refold and the pulled value
	// tracks it — lookups are refold-time state, not retained copies.
	fold("projects", "p1", `{"sold":"true","tenantId":8}`)
	require.Equal(t, `{"pricing":2,"quoted":50,"tenant":8}`, get("w1"))

	// The project stops matching the join's where (unsold): a REQUIRED
	// join gates membership — the row retracts entirely.
	fold("projects", "p1", `{"sold":"false","tenantId":8}`)
	require.Empty(t, get("w1"))

	// …and heals when it matches again.
	fold("projects", "p1", `{"sold":"true","tenantId":9}`)
	require.Equal(t, `{"pricing":2,"quoted":50,"tenant":9}`, get("w1"))

	// The dimension disappears: required lookup, row gone.
	del("projects", "p1")
	require.Empty(t, get("w1"))
}

// optional = true is the LEFT JOIN: absence (or a where miss) scopes the
// alias as null instead of gating membership.
func TestJoinAsOptional(t *testing.T) {
	fold, get, _ := harness(t, []Stage{
		{
			Name: "jobs", From: "projects", KeyPath: []string{"$.id"},
			Joins: []Join{
				{Topic: "customers", On: []string{"$.custId"}, As: "cust", Optional: true},
			},
			Emit: []Emit{
				{Field: "name", From: "$.name"},
				{Field: "customer", From: "$.cust.displayName"},
				{Field: "hasCustomer", Expr: "$.cust is not null"},
			},
		},
	}, "jobs")

	fold("projects", "j1", `{"id":"j1","name":"fence","custId":"c9"}`)
	require.Equal(t, `{"customer":null,"hasCustomer":false,"name":"fence"}`, get("j1"),
		"an unresolved optional lookup is the LEFT JOIN's null side — membership unaffected")

	fold("customers", "c9", `{"displayName":"Acme"}`)
	require.Equal(t, `{"customer":"Acme","hasCustomer":true,"name":"fence"}`, get("j1"))
}

// A stage join with as pulls a PRIOR STAGE's output by reference — the
// interval_cand shape (the proposal-workareas sum looked up by proposal
// id), where the old grammar needed a re-key scaffolding stage.
func TestJoinAsFromStage(t *testing.T) {
	fold, get, _ := harness(t, []Stage{
		{
			Name: "sums", From: "ppw", KeyPath: []string{"$.proposal"},
			Reduce: "aggregate",
			Emit:   []Emit{{Field: "total", Sum: "$.price"}},
		},
		{
			Name: "cand", From: "proposals", KeyPath: []string{"$.id"},
			Joins: []Join{{From: "sums", On: []string{"$.id"}, As: "quoted", Optional: true}},
			Emit: []Emit{
				{Field: "amount", Expr: "coalesce($.quoted.total, 0)"},
			},
		},
	}, "cand")

	fold("proposals", "pr1", `{"id":"pr1"}`)
	require.Equal(t, `{"amount":0}`, get("pr1"))

	fold("ppw", "a", `{"proposal":"pr1","price":100}`)
	fold("ppw", "b", `{"proposal":"pr1","price":50}`)
	require.Equal(t, `{"amount":150}`, get("pr1"), "the stage lookup tracks the upstream fold")
}

// Aggregate arms fold over the SCOPED input: sums/wheres can reference
// a looked-up row's fields (the quoted_cand shape — per-pair values
// enriched by the workarea's quoted price before the job-grain fold).
func TestJoinAsInAggregate(t *testing.T) {
	fold, get, _ := harness(t, []Stage{
		{
			Name: "byjob", From: "pairs", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Joins:  []Join{{Topic: "workareas", On: []string{"$.wa"}, As: "area"}},
			Emit: []Emit{
				{Field: "quoted", Sum: "$.area.price"},
				{Field: "big", Count: true, Where: []WhenClause{{Expr: "$.area.price > 10"}}},
			},
		},
	}, "byjob")

	fold("workareas", "w1", `{"price":5}`)
	fold("workareas", "w2", `{"price":20}`)
	fold("pairs", "x", `{"job":"j1","wa":"w1"}`)
	fold("pairs", "y", `{"job":"j1","wa":"w2"}`)
	require.Equal(t, `{"big":1,"quoted":25}`, get("j1"))

	// A dimension price change refolds the aggregate through the
	// reverse index — the sum is never a stale retained copy.
	fold("workareas", "w1", `{"price":15}`)
	require.Equal(t, `{"big":2,"quoted":35}`, get("j1"))
}

func TestJoinAsValidates(t *testing.T) {
	base := func(j Join) error {
		return ValidateShapes([]Stage{{
			Name: "s", From: "t", KeyPath: []string{"$.id"},
			Joins: []Join{j},
			Emit:  []Emit{{Field: "v", From: "$.v"}},
		}})
	}
	require.NoError(t, base(Join{Topic: "d", On: []string{"$.r"}, As: "row"}))
	require.ErrorContains(t, base(Join{Topic: "d", On: []string{"$.r"}, As: "row", Absent: true}), "absent",
		"an anti-join has no row to name")
	require.ErrorContains(t, base(Join{Topic: "d", On: []string{"$.r"}, Optional: true}), "optional",
		"optional without as is a filter that filters nothing")
	require.ErrorContains(t, base(Join{Topic: "d", On: []string{"$.r"}, As: "not an ident"}), "as",
		"aliases must be jsonpath-addressable")

	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{
			{Topic: "d", On: []string{"$.r"}, As: "row"},
			{Topic: "e", On: []string{"$.q"}, As: "row"},
		},
		Emit: []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "row", "aliases are unique per stage")
}
