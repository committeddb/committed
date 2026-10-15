package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Collapsed top-N and friends over an ordered array_agg, each expectation
// PostgreSQL 16's own result for the same subscript on
// array[30, 10, 20] (b=1: 30, b=2: 10, b=3: 20).
func TestArraySubscriptsMatchPostgres(t *testing.T) {
	cases := map[string]string{
		"(array_agg($.v order by $.b))[1]":                    `30`,
		"(array_agg($.v order by $.b))[1:2]":                  `[30,10]`,
		"(array_agg($.v order by $.b))[2:]":                   `[10,20]`,
		"(array_agg($.v order by $.b))[:2]":                   `[30,10]`,
		"(array_agg($.v order by $.b))[:]":                    `[30,10,20]`,
		"(array_agg($.v order by $.b))[3:2]":                  `[]`,
		"(array_agg($.v order by $.b))[-5:2]":                 `[30,10]`,
		"(array_agg($.v order by $.b))[0:10]":                 `[30,10,20]`,
		"(array_agg($.v order by $.b))[0]":                    `null`,
		"(array_agg($.v order by $.b))[4]":                    `null`,
		"(array_agg($.v order by $.b))[null]":                 `null`,
		"(array_agg($.v order by $.b))[2:null]":               `null`,
		"(array_agg($.v order by $.b))[1.5]":                  `10`, // rounds, as PG's integer cast does
		"(array_agg($.v order by $.b))[1.4999]":               `30`,
		"(array_agg($.v order by $.b))[1 + 1]":                `10`,
		"cardinality(array_agg($.v))":                         `3`,
		"cardinality((array_agg($.v))[3:2])":                  `0`,
		"(array_agg($.v) filter (where $.v > 99))[1]":         `null`,
		"cardinality(array_agg($.v) filter (where $.v > 99))": `null`,
		"(array_agg($.v order by $.b))[1] + 1":                `31`,
		"(array_agg($.v::text order by $.b))[2]::numeric * 2": `20`,
	}
	for src, want := range cases {
		h := newAggHarness(t, aggStage([]Emit{{Field: "x", Expr: src}}, ""))
		require.NoError(t, h.upsert("1", `{"k":"m","b":1,"v":30}`))
		require.NoError(t, h.upsert("2", `{"k":"m","b":2,"v":10}`))
		require.NoError(t, h.upsert("3", `{"k":"m","b":3,"v":20}`))
		require.JSONEq(t, `{"x":`+want+`}`, h.out("g", "m"), src)
	}
}

func TestArraySubscriptAdmission(t *testing.T) {
	for src, want := range map[string]string{
		"array_agg($.v)[1]":       "needs a parenthesized array",
		"(array_agg($.v))[1":      "expected ] to close",
		"(array_agg($.v))[]":      "empty subscript",
		"(array_agg($.v))[1:2:3]": "expected ] to close",
		"$.a:int":                 "a cast is ::type",
	} {
		err := ValidateShapes(aggStage([]Emit{{Field: "x", Expr: src}}, ""))
		require.ErrorContains(t, err, want, src)
	}
	// A payload array is still a jsonpath (0-based), never an array value.
	require.Equal(t, "b", evalToString(t, "$.items[1]", `{"items":["a","b"]}`))
	n, err := Compile("($.a)[1]")
	require.NoError(t, err)
	_, err = Eval(n, exprPayload(t, `{"a":5}`), nil)
	require.ErrorContains(t, err, "needs an array")
	_, err = Compile("cardinality($.a, 2)")
	require.ErrorContains(t, err, "cardinality takes 1–1 arguments")
}
