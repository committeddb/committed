package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The PostgreSQL probe rows, verbatim: (id, s, o) = (1,'b',3), (2,null,1),
// (3,'a',null), (4,'a',2). Each expectation is PG 16's own output.
func TestOrderedAggregatesMatchPostgres(t *testing.T) {
	cases := map[string]string{
		// o orders the ROWS: o=1 (s null), 2 (a), 3 (b), then o null (a) last.
		"array_agg($.s order by $.o)": `[null,"a","b","a"]`,
		// desc puts the null o FIRST, then 3 (b), 2 (a), 1 (s null).
		"array_agg($.s order by $.o desc)":          `["a","b","a",null]`,
		"string_agg($.s, ', ' order by $.o)":        `"a, b, a"`,
		"array_agg(distinct $.s)":                   `["a","b",null]`,
		"array_agg(distinct $.s order by $.s desc)": `[null,"b","a"]`,
		// s null (o=1) first; the two a rows tie on s and fall back to the
		// values themselves (2 before null); then b (o=3).
		"array_agg($.o order by $.s nulls first)": `[1,2,null,3]`,
		"array_agg($.s order by $.id)":            `["b",null,"a","a"]`,
	}

	for src, want := range cases {
		h := newAggHarness(t, aggStage([]Emit{{Field: "v", Expr: src}}, ""))
		for id, row := range map[string]string{
			"1": `{"k":"x","id":1,"s":"b","o":3}`,
			"2": `{"k":"x","id":2,"s":null,"o":1}`,
			"3": `{"k":"x","id":3,"s":"a","o":null}`,
			"4": `{"k":"x","id":4,"s":"a","o":2}`,
		} {
			require.NoError(t, h.upsert(id, row))
		}
		require.Equal(t, `{"v":`+want+`}`, h.out("g", "x"), src)
	}
}

func TestOrderedAggregateSemantics(t *testing.T) {
	h := newAggHarness(t, aggStage([]Emit{
		{Field: "none", Expr: "array_agg($.s) filter (where $.n > 100)"},
		{Field: "none_s", Expr: "string_agg($.s, ',') filter (where $.n > 100)"},
		{Field: "dflt", Expr: "coalesce(string_agg($.s, ',') filter (where $.n > 100), '-')"},
		{Field: "nums", Expr: "array_agg($.n order by $.n desc)"},
		{Field: "days", Expr: "array_agg($.d::date order by $.d::date)"},
		{Field: "csv", Expr: "string_agg($.n::text, '|' order by $.n)"},
	}, ""))
	require.NoError(t, h.upsert("1", `{"k":"x","s":"a","n":2.50,"d":"2024-03-01"}`))
	require.NoError(t, h.upsert("2", `{"k":"x","s":"b","n":10,"d":"2024-01-15"}`))
	require.Equal(t,
		`{"csv":"2.5|10","days":["2024-01-15","2024-03-01"],"dflt":"-","none":null,"none_s":null,"nums":[10,2.5]}`,
		h.out("g", "x"), "an empty set is null (not []); elements materialize like any stored value")

	bad := newAggHarness(t, aggStage([]Emit{{Field: "v", Expr: "string_agg($.n, ',')"}}, ""))
	require.ErrorContains(t, bad.upsert("1", `{"k":"x","n":5}`), "string_agg needs text")
}

func TestOrderedAggregateAdmission(t *testing.T) {
	ok := func(src string) error {
		return ValidateShapes(aggStage([]Emit{{Field: "v", Expr: src}}, ""))
	}
	for _, src := range []string{
		"array_agg($.x)",
		"array_agg($.x order by $.y desc nulls last, $.z)",
		"string_agg(distinct lower($.x), ', ' order by lower($.x))",
		"ARRAY_AGG($.x ORDER BY $.y ASC NULLS FIRST)",
	} {
		require.NoError(t, ok(src), src)
	}
	for src, want := range map[string]string{
		"array_agg(distinct $.x order by $.y)":  "the order by must be the argument itself",
		"string_agg($.x)":                       "string_agg takes a value and a delimiter",
		"string_agg($.x, $.sep)":                "delimiter must be a string literal",
		"sum($.x order by $.y)":                 `expected ")"`,
		"array_agg($.x order $.y)":              "expected by after order",
		"array_agg($.x order by $.y nulls top)": "expected first or last",
		"array_agg($.x) = array_agg($.y)":       "",
	} {
		err := ok(src)
		if want == "" {
			require.NoError(t, err, "admits; comparing arrays fails at evaluation")
			continue
		}
		require.ErrorContains(t, err, want, src)
	}
	h := newAggHarness(t, aggStage([]Emit{{Field: "v", Expr: "array_agg($.x) = array_agg($.y)"}}, ""))
	require.ErrorContains(t, h.upsert("1", `{"k":"a","x":1,"y":1}`), "does not compare")
}
