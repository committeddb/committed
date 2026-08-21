package stages

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func jsonNum(s string) json.Number { return json.Number(s) }

func evalSrc(t *testing.T, src string, payload any) any {
	t.Helper()
	n, err := Compile(src)
	require.NoError(t, err, "compile %q", src)
	v, err := Eval(n, payload, nil)
	require.NoError(t, err, "eval %q", src)
	return v
}

// SQL's three-valued logic, all nine cells per connective: null is
// UNKNOWN, so (null or true) is true and (null and false) is false —
// the one place the language's blanket null-propagation deliberately
// does not apply.
func TestExprConnectiveTruthTables(t *testing.T) {
	// $.t = true, $.f = false, $.n = null (missing field).
	p := map[string]any{"t": true, "f": false}
	tt := func(operand string) string {
		switch operand {
		case "T":
			return "$.t"
		case "F":
			return "$.f"
		default:
			return "$.n"
		}
	}
	orTable := map[[2]string]any{
		{"T", "T"}: true, {"T", "F"}: true, {"T", "N"}: true,
		{"F", "T"}: true, {"F", "F"}: false, {"F", "N"}: nil,
		{"N", "T"}: true, {"N", "F"}: nil, {"N", "N"}: nil,
	}
	andTable := map[[2]string]any{
		{"T", "T"}: true, {"T", "F"}: false, {"T", "N"}: nil,
		{"F", "T"}: false, {"F", "F"}: false, {"F", "N"}: false,
		{"N", "T"}: nil, {"N", "F"}: false, {"N", "N"}: nil,
	}
	for cell, want := range orTable {
		src := tt(cell[0]) + " or " + tt(cell[1])
		require.Equal(t, want, evalSrc(t, src, p), "or cell %v", cell)
	}
	for cell, want := range andTable {
		src := tt(cell[0]) + " and " + tt(cell[1])
		require.Equal(t, want, evalSrc(t, src, p), "and cell %v", cell)
	}
	// not: T→F, F→T, N→N.
	require.Equal(t, false, evalSrc(t, "not $.t", p))
	require.Equal(t, true, evalSrc(t, "not $.f", p))
	require.Nil(t, evalSrc(t, "not $.n", p))
}

// The connectives take BOOLEANS (comparisons/predicates); handing them a
// number is a data error, not a truthiness guess.
func TestExprConnectiveTypeErrors(t *testing.T) {
	n, err := Compile("1 or 2")
	require.NoError(t, err)
	_, err = Eval(n, map[string]any{}, nil)
	require.ErrorContains(t, err, "boolean")

	n, err = Compile("not 5")
	require.NoError(t, err)
	_, err = Eval(n, map[string]any{}, nil)
	require.ErrorContains(t, err, "boolean")
}

// in — SQL semantics: match is true; a null operand is null; a null
// member with no match makes the answer UNKNOWN (null), never false.
func TestExprIn(t *testing.T) {
	p := map[string]any{"pricing": jsonNum("2"), "s": "b"}
	require.Equal(t, true, evalSrc(t, "$.pricing in (0, 2)", p))
	require.Equal(t, false, evalSrc(t, "$.pricing in (1, 3)", p))
	require.Equal(t, true, evalSrc(t, "$.s in ('a', 'b')", p))
	// A "5.00" member equals the number 5 by exact value.
	require.Equal(t, true, evalSrc(t, "$.pricing in (2.0)", p))
	// Null operand → null.
	require.Nil(t, evalSrc(t, "$.missing in (1, 2)", p))
	// Null member + no match → null (x in (a, null) is x=a or x=null).
	require.Nil(t, evalSrc(t, "$.pricing in (9, $.missing)", p))
	// Null member + a match → still true.
	require.Equal(t, true, evalSrc(t, "$.pricing in (2, $.missing)", p))
	// Mixed types are simply not equal.
	require.Equal(t, false, evalSrc(t, "$.s in (1, 2)", p))
}

// is [not] null — the predicate that never returns null.
func TestExprIsNull(t *testing.T) {
	p := map[string]any{"x": jsonNum("1"), "s": ""}
	require.Equal(t, false, evalSrc(t, "$.x is null", p))
	require.Equal(t, true, evalSrc(t, "$.x is not null", p))
	require.Equal(t, true, evalSrc(t, "$.missing is null", p))
	require.Equal(t, false, evalSrc(t, "$.missing is not null", p))
	// An empty string is a value, not null (SQL agrees).
	require.Equal(t, false, evalSrc(t, "$.s is null", p))
	// Composes with nullif: nullif('', '') is null.
	require.Equal(t, true, evalSrc(t, "nullif($.s, '') is null", p))
}

// Precedence: or < and < not < comparison — a = 1 or b = 2 and c = 3
// groups as a=1 or (b=2 and c=3), and parentheses override.
func TestExprBooleanPrecedence(t *testing.T) {
	p := map[string]any{"a": jsonNum("0"), "b": jsonNum("2"), "c": jsonNum("3")}
	require.Equal(t, true, evalSrc(t, "$.a = 1 or $.b = 2 and $.c = 3", p))
	require.Equal(t, false, evalSrc(t, "($.a = 1 or $.b = 2) and $.c = 9", p))
	require.Equal(t, true, evalSrc(t, "not $.a = 1 and $.c = 3", p), "not binds to the comparison, not the and")
	// Keywords are case-insensitive, SQL style.
	require.Equal(t, true, evalSrc(t, "$.b = 2 OR $.a = 9", p))
	require.Equal(t, true, evalSrc(t, "$.b IN (2)", p))
	require.Equal(t, true, evalSrc(t, "$.a IS NOT NULL", p))
}

// The field formulas this exists for: the three-way candidate OR and
// the boolean flag columns.
func TestExprBooleanFieldShapes(t *testing.T) {
	p := map[string]any{"ib": true, "upv": false, "amount": jsonNum("120")}
	require.Equal(t, true, evalSrc(t, "$.ib or $.upv or $.q is not null", p))
	require.Equal(t, true, evalSrc(t, "coalesce($.amount, 0) > 0 and $.pricing is null", p))
}

// Booleans never materialize a decimal: a division inside a predicate is
// dominated (like a comparison), so it compiles without round/trunc.
func TestExprBooleanMaterialization(t *testing.T) {
	_, err := Compile("$.a / $.b in (1, 2)")
	require.NoError(t, err, "a compared quotient never reaches a column")
	_, err = Compile("$.a / $.b > 1 or $.c = 2")
	require.NoError(t, err)
	// But a bare quotient at the top is still rejected.
	_, err = Compile("$.a / $.b")
	require.ErrorContains(t, err, "round")
}
