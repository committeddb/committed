package stages

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func payload(t *testing.T, s string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	require.NoError(t, dec.Decode(&v))
	return v
}

// The comparison arms follow SQL, matching the engine's other folds
// (aggregate null-skip, argmax nulls-lowest) and the SQL-oracle
// acceptance methodology: a missing or null value matches NO comparison
// — including notEquals — and cross-family values match neither equals
// nor notEquals (the typed-equality rule).
func TestMatchComparisons(t *testing.T) {
	cases := []struct {
		name   string
		clause WhenClause
		json   string
		want   bool
	}{
		// The pilot's two verbatim shapes.
		{"notEquals excludes the literal", WhenClause{Path: "$.EventType", NotEquals: "delete"}, `{"EventType":"delete"}`, false},
		{"notEquals admits others", WhenClause{Path: "$.EventType", NotEquals: "delete"}, `{"EventType":"update"}`, true},
		{"greaterThan admits above", WhenClause{Path: "$.visit_price", GreaterThan: 0}, `{"visit_price":12.5}`, true},
		{"greaterThan excludes zero (strict)", WhenClause{Path: "$.visit_price", GreaterThan: 0}, `{"visit_price":0}`, false},
		{"greaterThan excludes below", WhenClause{Path: "$.visit_price", GreaterThan: 0}, `{"visit_price":-3}`, false},

		// SQL null semantics: null/missing matches no comparison.
		{"notEquals: null is not <>", WhenClause{Path: "$.EventType", NotEquals: "delete"}, `{"EventType":null}`, false},
		{"notEquals: missing is not <>", WhenClause{Path: "$.EventType", NotEquals: "delete"}, `{}`, false},
		{"greaterThan: null never compares", WhenClause{Path: "$.visit_price", GreaterThan: 0}, `{"visit_price":null}`, false},
		{"greaterThan: missing never compares", WhenClause{Path: "$.visit_price", GreaterThan: 0}, `{}`, false},
		{"lessThan strict below", WhenClause{Path: "$.n", LessThan: 10}, `{"n":9.99}`, true},
		{"lessThan excludes equal", WhenClause{Path: "$.n", LessThan: 10}, `{"n":10}`, false},

		// Typed comparisons: cross-family is no match either way.
		{"notEquals: cross-family string vs number is no match", WhenClause{Path: "$.v", NotEquals: "5"}, `{"v":5}`, false},
		{"notEquals: cross-family bool vs string is no match", WhenClause{Path: "$.v", NotEquals: "true"}, `{"v":true}`, false},
		{"greaterThan: a string value never compares", WhenClause{Path: "$.v", GreaterThan: 0}, `{"v":"12"}`, false},
		{"notEquals numeric across representations", WhenClause{Path: "$.v", NotEquals: 5}, `{"v":5.0}`, false},
		{"notEquals numeric different", WhenClause{Path: "$.v", NotEquals: 5}, `{"v":6}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, Match([]WhenClause{tc.clause}, payload(t, tc.json)))
		})
	}
}

// Admission: exactly one arm, scalar literals, numeric-only ordering.
func TestValidateWhenComparisons(t *testing.T) {
	ok := []WhenClause{
		{Path: "$.a", NotEquals: "delete"},
		{Path: "$.b", GreaterThan: 0},
		{Path: "$.c", LessThan: 10.5},
	}
	require.NoError(t, ValidateWhen(ok, "w"))

	err := ValidateWhen([]WhenClause{{Path: "$.a", Equals: "x", NotEquals: "y"}}, "w")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one of equals, null, notNull, notEquals, greaterThan, lessThan, or expr")

	err = ValidateWhen([]WhenClause{{Path: "$.a", GreaterThan: "high"}}, "w")
	require.Error(t, err)
	require.Contains(t, err.Error(), "greaterThan takes a numeric literal")

	err = ValidateWhen([]WhenClause{{Path: "$.a", NotEquals: map[string]any{}}}, "w")
	require.Error(t, err)
	require.Contains(t, err.Error(), "notEquals must be a scalar literal")
}
