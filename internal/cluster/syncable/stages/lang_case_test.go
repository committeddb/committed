package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Searched CASE — SQL semantics: the first WHEN that is TRUE wins; a null
// or false WHEN falls through; no match and no ELSE is null.
func TestExprCaseSearched(t *testing.T) {
	band := "case when $.hours < 8 then 'small' when $.hours < 40 then 'medium' else 'large' end"
	require.Equal(t, "small", evalToString(t, band, `{"hours":2}`))
	require.Equal(t, "medium", evalToString(t, band, `{"hours":8}`), "first TRUE arm wins, in order")
	require.Equal(t, "large", evalToString(t, band, `{"hours":40}`))
	require.Equal(t, "large", evalToString(t, band, `{}`), "a null WHEN falls through to ELSE")

	noElse := "case when $.s = 'done' then 1 end"
	require.Equal(t, "1", evalToString(t, noElse, `{"s":"done"}`))
	require.Nil(t, evalToString(t, noElse, `{"s":"open"}`), "no match, no ELSE → null")
	require.Nil(t, evalToString(t, noElse, `{}`))

	// Keywords are case-insensitive, like the rest of the language.
	require.Equal(t, "y", evalToString(t, "CASE WHEN $.a = 1 THEN 'y' ELSE 'n' END", `{"a":1}`))

	// Results are full expressions, and CASE nests.
	require.Equal(t, "12.5", evalToString(t, "case when $.f then $.a * 2.5 else 0 end", `{"f":true,"a":5}`))
	nested := "case when $.a > 0 then case when $.b > 0 then 'pp' else 'pn' end else 'n' end"
	require.Equal(t, "pn", evalToString(t, nested, `{"a":1,"b":-1}`))
}

// Simple CASE is `x = a` per arm (exprEqual's families: a mixed-family
// comparison is simply unequal), so a null operand matches no arm.
func TestExprCaseSimple(t *testing.T) {
	label := "case $.status when 1 then 'open' when 2 then 'closed' else 'other' end"
	require.Equal(t, "open", evalToString(t, label, `{"status":1}`))
	require.Equal(t, "closed", evalToString(t, label, `{"status":2.000}`), "numbers compare exactly, not by digits")
	require.Equal(t, "other", evalToString(t, label, `{"status":"1"}`), "a string never equals a number")
	require.Equal(t, "other", evalToString(t, label, `{}`), "a null operand matches no arm")
	require.Nil(t, evalToString(t, "case $.k when null then 'x' end", `{}`), "null = null is UNKNOWN, not a match")
}

// Evaluation is lazy: WHENs after the match and every unselected result
// never run, so their data errors cannot surface — the guard idiom.
func TestExprCaseLazyBranches(t *testing.T) {
	guard := "case when $.n = 0 then null else round($.a / $.n, 2) end"
	require.Nil(t, evalToString(t, guard, `{"a":1,"n":0}`), "the division by zero sits in the unselected branch")
	require.Equal(t, "0.33", evalToString(t, guard, `{"a":1,"n":3}`))

	// An unselected arithmetic-on-a-string arm does not dead-letter…
	require.Equal(t, "ok", evalToString(t, "case when true then 'ok' else $.s + 1 end", `{"s":"x"}`))
	require.Equal(t, "ok", evalToString(t, "case when true then 'ok' when $.s + 1 > 0 then 'no' end", `{"s":"x"}`),
		"WHENs after the match are not evaluated either")
	// …but the selected one still does.
	n, err := Compile("case when false then 'ok' else $.s + 1 end")
	require.NoError(t, err)
	_, err = Eval(n, exprPayload(t, `{"s":"x"}`), nil)
	require.ErrorContains(t, err, "arithmetic needs a number")
}

// A searched WHEN must be a predicate: a non-boolean value is a data
// error, never a truthiness guess (the connectives' rule).
func TestExprCaseNonBooleanWhen(t *testing.T) {
	n, err := Compile("case when $.a then 1 end")
	require.NoError(t, err)
	_, err = Eval(n, exprPayload(t, `{"a":5}`), nil)
	require.ErrorContains(t, err, "boolean")
}

// The null literal: SQL's NULL as a value, so a branch can say "no value"
// without a nullif trick. It propagates like any null.
func TestExprNullLiteral(t *testing.T) {
	require.Nil(t, evalToString(t, "null", `{}`))
	require.Nil(t, evalToString(t, "NULL + 1", `{}`))
	require.Equal(t, "5", evalToString(t, "coalesce(null, 5)", `{}`))
	require.Equal(t, true, evalToString(t, "null is null", `{}`))
	require.Nil(t, evalToString(t, "$.a = null", `{"a":1}`), "= null is UNKNOWN, as in SQL — use is null")
}

func TestExprCaseAdmission(t *testing.T) {
	bad := map[string]string{
		"case end":                                  "at least one when",
		"case when $.a = 1 then 'x'":                "expected when, else, or end",
		"case when $.a = 1 'x' end":                 "expected then",
		"case when $.a = 1 then 'x' else 'y' 'z'":   "expected when, else, or end",
		"case when $.a then 1 else 'x' end":         "mix literal types (number and string)",
		"case when $.a then true else -1 end":       "mix literal types (boolean and number)",
		"case when $.a then $.b / $.c end":          "round(...) or trunc(...)",
		"case when $.a then 1 else $.b / $.c end":   "round(...) or trunc(...)",
		"case $.b / $.c when 1 then 'x' else 'y' x": "expected when, else, or end",
	}
	for src, want := range bad {
		_, err := Compile(src)
		require.ErrorContains(t, err, want, "src %q", src)
	}

	good := []string{
		"case when $.a then 1 else null end",          // null fits every family
		"case when $.a then 1 else $.s end",           // non-literal branches are untyped
		"round(case when $.a then $.b / $.c end, 2)",  // round dominates every branch
		"case when $.b / $.c > 1 then 'x' end",        // a WHEN is boolean context
		"case $.b / $.c when 1 then 'x' end",          // so is the operand
		"case when $.a then -1 when $.b then 2.5 end", // one family: number
		"case when $.a then 'x' end = 'x'",            // CASE is a primary
		"coalesce(case when $.a then 1 end, 0) + $.n", // …usable anywhere
	}
	for _, src := range good {
		_, err := Compile(src)
		require.NoError(t, err, "src %q", src)
	}
}

// The path walkers see inside CASE: an alias addressed from a fold-time
// position and a stray $parent are rejected even when buried in a branch.
func TestExprCaseScopeWalkers(t *testing.T) {
	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
		When:  []WhenClause{{Expr: "case when $.x = 1 then $.project.sold else false end"}},
		Emit:  []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "REFOLD")

	n, err := Compile("case $.k when 1 then 'a' else $parent.kind end")
	require.NoError(t, err)
	require.True(t, exprReferencesParent(n))
}

// CASE inside a fold arm — SQL's sum(case when … then x else 0 end), the
// conditional-sum idiom — folds through a real aggregate stage.
func TestExprCaseInFoldArm(t *testing.T) {
	stages := []Stage{{
		Name: "by-job", From: "visits", KeyPath: []string{"$.job"},
		Reduce: "aggregate",
		Emit: []Emit{
			{Field: "done_hours", Sum: "case when $.status = 'done' then $.hours else 0 end"},
			{Field: "bands", Collect: "case when $.hours < 8 then 'small' else 'large' end", Distinct: true},
		},
	}}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	upsert := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "visits", []byte(key), decodePayload(t, payload))
		}))
	}
	upsert("v1", `{"job":"j1","status":"done","hours":3}`)
	upsert("v2", `{"job":"j1","status":"open","hours":20}`)
	upsert("v3", `{"job":"j1","status":"done","hours":1.5}`)

	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := g.Output(tx, "by-job", []byte("j1"))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"bands":["large","small"],"done_hours":4.5}`, got)
}

// IS [NOT] DISTINCT FROM — PG's null-safe comparison: never null.
func TestExprIsDistinctFrom(t *testing.T) {
	for _, tc := range []struct {
		payload        string
		distinct, same bool
	}{
		{`{"a":1,"b":1}`, false, true},
		{`{"a":1,"b":2}`, true, false},
		{`{"a":1}`, true, false},
		{`{"b":1}`, true, false},
		{`{}`, false, true},
		{`{"a":"x","b":"x"}`, false, true},
		{`{"a":5,"b":5.0}`, false, true},
	} {
		require.Equal(t, tc.distinct, evalToString(t, "$.a is distinct from $.b", tc.payload), tc.payload)
		require.Equal(t, tc.same, evalToString(t, "$.a IS NOT DISTINCT FROM $.b", tc.payload), tc.payload)
	}
	// Comparison precedence: the right side is a full concat-level operand.
	require.Equal(t, false, evalToString(t, "$.a is distinct from $.b || 'x'", `{"a":"yx","b":"y"}`))
	_, err := Compile("$.a is distinct $.b")
	require.ErrorContains(t, err, "expected from after is distinct")
}
