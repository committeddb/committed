package stages

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The lookup scope exists at REFOLD (emits, fold arms, per-emit where).
// A fold-time position — when, deleteWhen, keyPath, forEach, a join's
// on — that addresses a declared alias would resolve nothing and
// silently never match (the silent-empty family at the grammar's
// newest seam), so admission rejects it loudly.
func TestFoldTimeAliasReferencesRejected(t *testing.T) {
	mk := func(mut func(*Stage)) error {
		st := Stage{
			Name: "s", From: "t", KeyPath: []string{"$.id"},
			Joins: []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
			Emit:  []Emit{{Field: "tenant", From: "$.project.tenantId"}},
		}
		mut(&st)
		return ValidateShapes([]Stage{st})
	}
	require.NoError(t, mk(func(*Stage) {}), "emits legally read the alias")
	require.ErrorContains(t, mk(func(s *Stage) {
		s.When = []WhenClause{{Path: "$.project.sold", Equals: "true"}}
	}), "REFOLD")
	require.ErrorContains(t, mk(func(s *Stage) {
		s.When = []WhenClause{{Expr: "$.project.sold = 'true'"}}
	}), "REFOLD", "the guard walks expr paths too")
	require.ErrorContains(t, mk(func(s *Stage) {
		s.KeyPath = []string{"$.project.tenantId"}
	}), "REFOLD")
	require.ErrorContains(t, mk(func(s *Stage) {
		s.Joins = append(s.Joins, Join{Topic: "d2", On: []string{"$.project.other"}})
	}), "REFOLD")
	require.NoError(t, mk(func(s *Stage) {
		s.When = []WhenClause{{Path: "$.projectile.x", Equals: "y"}}
	}), "a longer identifier sharing the alias prefix is not the alias")

	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"}, Reduce: "liveSet",
		When:       []WhenClause{{Path: "$.type", Equals: "created"}},
		DeleteWhen: []WhenClause{{Path: "$.project.gone", Equals: "true"}},
		Joins:      []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
		Emit:       []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "REFOLD", "deleteWhen is fold-time too")
}

// A fanned liveSet's delete evidence is per-element: a delete-shaped
// event carrying no elements fans zero times and its retraction is
// silently lost — the shape is unrepresentable rather than documented.
func TestFannedLiveSetRejected(t *testing.T) {
	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.txn"},
		ForEach: "$.els[*]", ElementKey: "$.id",
		Reduce:     "liveSet",
		When:       []WhenClause{{Path: "$.type", Equals: "created"}},
		DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
		Emit:       []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "fan in a prior stage")
}

// Scalar when-arms compare EXACTLY — the last float64 comparison in the
// system. Two distinct 2^53-scale integers must not collide as equal,
// and ordering must hold where float64 rounds them together.
func TestScalarWhenArmsCompareExactly(t *testing.T) {
	eq := []WhenClause{{Path: "$.id", Equals: int64(9007199254740993)}}
	require.NoError(t, ValidateWhen(eq, "t"))
	require.False(t, Match(eq, map[string]any{"id": json.Number("9007199254740994")}),
		"distinct 2^53-scale IDs must not float-collide as equal")
	require.True(t, Match(eq, map[string]any{"id": json.Number("9007199254740993")}))

	ne := []WhenClause{{Path: "$.id", NotEquals: int64(9007199254740993)}}
	require.NoError(t, ValidateWhen(ne, "t"))
	require.True(t, Match(ne, map[string]any{"id": json.Number("9007199254740994")}))

	gt := []WhenClause{{Path: "$.n", GreaterThan: int64(9007199254740992)}}
	require.NoError(t, ValidateWhen(gt, "t"))
	require.True(t, Match(gt, map[string]any{"n": json.Number("9007199254740993")}),
		"2^53+1 > 2^53 exactly; equal as float64")

	// A float TOML literal means its DECIMAL intent: equals = 0.1
	// matches the payload digits "0.1".
	dq := []WhenClause{{Path: "$.d", Equals: 0.1}}
	require.NoError(t, ValidateWhen(dq, "t"))
	require.True(t, Match(dq, map[string]any{"d": json.Number("0.1")}))
	require.False(t, Match(dq, map[string]any{"d": json.Number("0.10000000000000000555")}))
}

// Boolean literals — SQL's TRUE/FALSE, completing the predicate set
// (a boolean design with no way to write its own constants couldn't
// express coalesce($.flag, false)).
func TestExprBooleanLiteralsInLanguage(t *testing.T) {
	p := map[string]any{"b": true}
	require.Equal(t, true, evalSrc(t, "$.b = true", p))
	require.Equal(t, false, evalSrc(t, "$.b = false", p))
	require.Equal(t, true, evalSrc(t, "coalesce($.missing, true)", p))
	require.Equal(t, false, evalSrc(t, "coalesce($.missing, FALSE)", p), "case-insensitive, like every keyword")
	require.Equal(t, true, evalSrc(t, "not false", p))
	require.Equal(t, true, evalSrc(t, "$.missing is null and true", p))
}
