package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The expr arm: the closed expression language as a when predicate —
// the clause matches only when the expression evaluates to TRUE; false,
// null, a non-boolean result, and data errors all match nothing (SQL's
// WHERE). Compute-then-filter collapses into one stage; the two-stage
// gate idiom stays legal where introspectable intermediates matter.
func TestWhenExprArmValidates(t *testing.T) {
	ok := []WhenClause{{Expr: "coalesce($.n, 0) > 0"}}
	require.NoError(t, ValidateWhen(ok, "t"))

	// Exactly one arm: expr excludes the scalar arms AND path (the
	// expression addresses its own paths).
	require.Error(t, ValidateWhen([]WhenClause{{Expr: "$.n > 0", Path: "$.n"}}, "t"))
	require.Error(t, ValidateWhen([]WhenClause{{Expr: "$.n > 0", Equals: "x", Path: "$.n"}}, "t"))

	// A malformed expression is a loud admission error.
	err := ValidateWhen([]WhenClause{{Expr: "$.n >"}}, "t")
	require.Error(t, err)
	// The materialization rule applies inside when too.
	err = ValidateWhen([]WhenClause{{Expr: "$.a / $.b"}}, "t")
	require.ErrorContains(t, err, "round")
}

func TestWhenExprArmMatches(t *testing.T) {
	cl := []WhenClause{{Expr: "coalesce($.n, 0) > 0 and $.status in ('open', 'held')"}}
	require.NoError(t, ValidateWhen(cl, "t"))

	require.True(t, MatchScoped(cl, map[string]any{"n": jsonNum("2"), "status": "open"}, nil))
	require.False(t, MatchScoped(cl, map[string]any{"n": jsonNum("0"), "status": "open"}, nil), "false matches nothing")
	require.False(t, MatchScoped(cl, map[string]any{"n": jsonNum("2")}, nil), "null (missing status) matches nothing")

	// A non-boolean expression result matches nothing — when is a
	// filter, not a truthiness coercion.
	nb := []WhenClause{{Expr: "coalesce($.n, 1)"}}
	require.NoError(t, ValidateWhen(nb, "t"))
	require.False(t, MatchScoped(nb, map[string]any{"n": jsonNum("5")}, nil))

	// $parent reaches the enclosing scope where a fan provides one.
	pc := []WhenClause{{Expr: "$parent.type = 'created' and $.amount > 0"}}
	require.NoError(t, ValidateWhen(pc, "t"))
	el := map[string]any{"amount": jsonNum("3")}
	par := map[string]any{"type": "created"}
	require.True(t, MatchScoped(pc, el, par))
	require.False(t, MatchScoped(pc, el, map[string]any{"type": "deleted"}))
}

// $parent inside an expr arm is rejected where no fan scope exists —
// same guard as the path arms.
func TestWhenExprParentRejectedOutsideFan(t *testing.T) {
	cl := []WhenClause{{Expr: "$parent.type = 'created'"}}
	require.NoError(t, ValidateWhen(cl, "t"))
	require.Error(t, RejectParentPaths(cl, "t"))
	require.NoError(t, RejectParentPaths([]WhenClause{{Expr: "$.type = 'created'"}}, "t"))
}

// The field shape this exists for: quoted_by_job's compute-then-filter
// (remaining = quoted - invoiced; keep only remaining > 0) as ONE stage
// instead of an emit-gate stage plus a filter stage.
func TestWhenExprCollapsesComputeThenFilter(t *testing.T) {
	sts := []Stage{
		{
			Name: "quoted-open", From: "quotes", KeyPath: []string{"$.wa"},
			When: []WhenClause{{Expr: "$.quoted - coalesce($.invoiced, 0) > 0"}},
			Emit: []Emit{{Field: "remaining", Expr: "$.quoted - coalesce($.invoiced, 0)"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "quotes", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("quoted-open", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	fold("w1", `{"wa":"w1","quoted":100,"invoiced":40}`)
	require.Equal(t, `{"remaining":60}`, get("w1"))

	// The invoiced total catches up: the same input stops matching and
	// RETRACTS (filtering is refold, not skip) — the when-flip
	// retraction direction through an expr arm.
	fold("w1", `{"wa":"w1","quoted":100,"invoiced":100}`)
	require.Empty(t, get("w1"))
}
