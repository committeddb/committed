package stages

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// < <= > >= order within one family: numbers exactly, strings byte-wise
// (code-point order — PG's COLLATE "C"), booleans false < true. Null is
// null; mixed families are a data error, as in PG.
func TestExprOrderingComparisons(t *testing.T) {
	p := `{"a":"apple","b":"Banana","e":"é","d1":"2026-01-31","d2":"2026-02-01","t":true,"f":false,"n":5}`
	cases := map[string]any{
		"$.a < 'b'":            true,
		"$.b < $.a":            true, // byte order: uppercase before lowercase
		"$.a <= 'apple'":       true,
		"$.a >= 'apple'":       true,
		"$.a > 'apple'":        false,
		"$.e > 'z'":            true, // multibyte code points sort after ASCII
		"$.d1 < $.d2":          true, // ISO dates order correctly as text
		"$.f < $.t":            true,
		"$.t <= $.t":           true,
		"$.t > $.f":            true,
		"$.n < 10":             true, // numbers unchanged
		"$.missing < 'a'":      nil,
		"'a' >= $.missing":     nil,
		"upper($.a) < $.a":     true,
		"$.a || 'x' > 'apple'": true,
	}
	for src, want := range cases {
		require.Equal(t, want, evalToString(t, src, p), src)
	}

	for src, want := range map[string]string{
		"$.a < 5":      "< compares two values of the same kind, got the string \"apple\" and the number 5",
		"$.t >= 1":     "got a boolean and the number 1",
		"$.n > 'four'": "got the number 5 and the string \"four\"",
	} {
		require.ErrorContains(t, evalErr(t, src, p), want, src)
	}
}

// In a when expr arm a mixed-family comparison is a data error, which —
// like every when-arm data error — matches nothing rather than failing
// the event.
func TestExprOrderingWhenArmMixedFamilyMatchesNothing(t *testing.T) {
	cl := []WhenClause{{Expr: "$.s < 5"}}
	require.NoError(t, ValidateWhen(cl, "w"))
	require.False(t, Match(cl, map[string]any{"s": "a"}))
	require.True(t, Match([]WhenClause{{Expr: "$.s < 'b'"}}, map[string]any{"s": "a"}))
}

// Expressions and folds share one order: over a set of strings (mixed
// case, multibyte, digits, ISO dates), every value lies between the
// aggregate stage's min and max by the expression operators, and the
// min/max themselves are the expression-order extremes.
func TestExprOrderingAgreesWithMinMaxFolds(t *testing.T) {
	values := []string{"b", "B", "a", "ab", "Z", "é", "zz", "10", "9", "2026-01-31", "2026-02-01", ""}
	stages := []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
		Emit: []Emit{{Field: "lo", Min: "$.v"}, {Field: "hi", Max: "$.v"}},
	}}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	for i, v := range values {
		bs, err := json.Marshal(map[string]any{"k": "k", "v": v})
		require.NoError(t, err)
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "rows", fmt.Append(nil, i), decodePayload(t, string(bs)))
		}))
	}
	var out map[string]string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		bs, err := g.Output(tx, "g", []byte("k"))
		if err != nil {
			return err
		}
		return json.Unmarshal(bs, &out)
	}))
	require.Equal(t, "", out["lo"])
	require.Equal(t, "é", out["hi"])

	between, err := Compile("$.v >= $.lo and $.v <= $.hi")
	require.NoError(t, err)
	strictlyOutside, err := Compile("$.v < $.lo or $.v > $.hi")
	require.NoError(t, err)
	for _, v := range values {
		row := map[string]any{"v": v, "lo": out["lo"], "hi": out["hi"]}
		got, err := Eval(between, row, nil)
		require.NoError(t, err)
		require.Equal(t, true, got, "%q lies within the folded [min, max]", v)
		got, err = Eval(strictlyOutside, row, nil)
		require.NoError(t, err)
		require.Equal(t, false, got, "%q", v)
	}
}
