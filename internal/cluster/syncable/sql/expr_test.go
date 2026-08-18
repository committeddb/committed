package sql

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func exprPayload(t *testing.T, src string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(src))
	dec.UseNumber()
	var v any
	require.NoError(t, dec.Decode(&v))
	return v
}

// evalToString compiles and evaluates src against the JSON payload and
// renders the result the way the apply path would: rationals through
// formatRat, everything else via fmt-free direct returns.
func evalToString(t *testing.T, src, payload string) any {
	t.Helper()
	n, err := compileExpr(src)
	require.NoError(t, err, "compile %q", src)
	v, err := evalExpr(n, exprPayload(t, payload))
	require.NoError(t, err, "eval %q", src)
	if r, ok := v.(*big.Rat); ok {
		s, err := formatRat(r)
		require.NoError(t, err)
		return s
	}
	return v
}

func TestExprExactDecimalArithmetic(t *testing.T) {
	// The float trap: 0.1+0.2 is exactly 0.3 here, not 0.30000000000000004.
	require.Equal(t, "0.3", evalToString(t, "0.1 + 0.2", `{}`))
	require.Equal(t, true, evalToString(t, "0.1 + 0.2 = 0.3", `{}`))
	require.Equal(t, "0.02", evalToString(t, "1.1 * 0.2 - 0.2", `{}`))
	require.Equal(t, "-3", evalToString(t, "-1.5 * 2", `{}`))
	// Minimal decimal form: trailing zeros trimmed, integral values bare.
	require.Equal(t, "2.5", evalToString(t, "2.50 + 0", `{}`))
	require.Equal(t, "0", evalToString(t, "5 - 5", `{}`))
}

func TestExprRoundingModes(t *testing.T) {
	// round = half AWAY from zero (PG round(numeric), T-SQL ROUND) — ties
	// go up in magnitude for both signs, never banker's rounding.
	require.Equal(t, "3", evalToString(t, "round(2.5, 0)", `{}`))
	require.Equal(t, "-3", evalToString(t, "round(0 - 2.5, 0)", `{}`))
	require.Equal(t, "2.35", evalToString(t, "round(2.345, 2)", `{}`))
	// trunc = toward zero for both signs (T-SQL ROUND(x, s, 1)).
	require.Equal(t, "2", evalToString(t, "trunc(2.9)", `{}`))
	require.Equal(t, "-2", evalToString(t, "trunc(0 - 2.9)", `{}`))
	require.Equal(t, "2.98", evalToString(t, "trunc(2.987, 2)", `{}`))
	// Division is exact until the explicit rounding point.
	require.Equal(t, "0.3333", evalToString(t, "round(1/3, 4)", `{}`))
	require.Equal(t, "4", evalToString(t, "round(7/2, 0)", `{}`))
	require.Equal(t, "2", evalToString(t, "trunc(10/4)", `{}`))
}

func TestExprNullSemantics(t *testing.T) {
	// Missing field and JSON null are both null; null propagates.
	require.Nil(t, evalToString(t, "$.missing + 1", `{}`))
	require.Nil(t, evalToString(t, "$.a * 2", `{"a": null}`))
	require.Nil(t, evalToString(t, "$.missing > 0", `{}`))
	require.Equal(t, "42", evalToString(t, "coalesce($.missing, 42)", `{}`))
	require.Equal(t, "7", evalToString(t, "coalesce($.a, $.b, 42)", `{"a": null, "b": 7}`))
	// nullif is the division-by-zero guard: equal → null → quotient null.
	require.Nil(t, evalToString(t, "round(1 / nullif($.n, 0), 2)", `{"n": 0}`))
	require.Equal(t, "0.5", evalToString(t, "round(1 / nullif($.n, 0), 2)", `{"n": 2}`))
	// nullif with a null first operand flows the null through.
	require.Nil(t, evalToString(t, "nullif($.missing, 0)", `{}`))
}

func TestExprStrings(t *testing.T) {
	// SA.2's shape: blank group name falls back to the plain name.
	f := "coalesce(nullif($.GroupName, ''), $.Name)"
	require.Equal(t, "G", evalToString(t, f, `{"GroupName": "G", "Name": "N"}`))
	require.Equal(t, "N", evalToString(t, f, `{"GroupName": "", "Name": "N"}`))
	require.Equal(t, "N", evalToString(t, f, `{"Name": "N"}`))
	require.Equal(t, true, evalToString(t, "$.a = 'x''y'", `{"a": "x'y"}`))
	// Mixed types are simply not equal.
	require.Equal(t, false, evalToString(t, "$.a = '1'", `{"a": 1}`))
}

// The pilot's item-price formula, verbatim shape: 3-deep coalesce, nullif
// div-by-zero guard, nested round — the acceptance shape for the grammar.
func TestExprItemPriceFormula(t *testing.T) {
	f := `round($.Quantity * coalesce($.CustomUnitPrice, $.OriginalCatalogCustomPrice,
	        round(($.Cost + $.Overhead + $.Tax)
	            / nullif(1 - coalesce($.CustomProfitMargin, $.CatalogCustomProfitMargin, $.OriginalProfitMargin), 0),
	        0)),
	    0)`

	// Margin path: (100+20+5)/(1-0.25) = 166.666… → 167; 3 * 167 = 501.
	require.Equal(t, "501", evalToString(t, f, `{
		"Quantity": 3, "CustomUnitPrice": null, "OriginalCatalogCustomPrice": null,
		"Cost": 100, "Overhead": 20, "Tax": 5,
		"CustomProfitMargin": null, "CatalogCustomProfitMargin": 0.25, "OriginalProfitMargin": 0.5}`))
	// Custom price short-circuits the margin math: 2.5 × 15 = 37.5, and the
	// formula's OUTER round(…, 0) takes it half-away to 38.
	require.Equal(t, "38", evalToString(t, f, `{"Quantity": 2.5, "CustomUnitPrice": 15}`))
	// 100% margin → nullif(0,0) → null → whole formula null (not a crash).
	require.Nil(t, evalToString(t, f, `{
		"Quantity": 1, "Cost": 10, "Overhead": 0, "Tax": 0, "CustomProfitMargin": 1}`))
	// The filter form: formula > 0 (comparison over the rounded value).
	require.Equal(t, true, evalToString(t, "("+f+") > 0", `{"Quantity": 2.5, "CustomUnitPrice": 15}`))
}

func TestExprPerInvoiceFormula(t *testing.T) {
	f := "trunc($.total_quoted_amount / nullif($.total_payments, 0))"
	require.Equal(t, "333", evalToString(t, f, `{"total_quoted_amount": 1000.50, "total_payments": 3}`))
	require.Nil(t, evalToString(t, f, `{"total_quoted_amount": 1000.50, "total_payments": 0}`))
}

func TestExprComparisonConsumesExactQuotient(t *testing.T) {
	// Comparisons never materialize, so an unrounded quotient is legal there
	// — and exact: 1/3 three times over sums to exactly 1.
	require.Equal(t, true, evalToString(t, "$.a / $.b > 1", `{"a": 3, "b": 2}`))
	require.Equal(t, true, evalToString(t, "1/3 + 1/3 + 1/3 = 1", `{}`))
}

func TestExprAdmissionRejections(t *testing.T) {
	cases := map[string]string{
		"$.a / $.b":                "round(...) or trunc(...)",
		"1 + $.a / 2":              "round(...) or trunc(...)",
		"round($.a, $.b)":          "scale must be a literal",
		"round($.a, 1 + 1)":        "scale must be a literal",
		"round($.a, -1)":           "between 0 and 12",
		"round($.a)":               "takes 2",
		"foo($.a)":                 "unknown function",
		"1 +":                      "unexpected",
		"1 2":                      "trailing",
		"nullif($.a)":              "takes 2",
		"coalesce($.a)":            "takes 2",
		"'never closed":            "unterminated string",
		"$.a ? 1":                  "unexpected character",
		"round(1 / nullif($.a, 0)": `expected ","`,
	}
	for src, want := range cases {
		_, err := compileExpr(src)
		require.Error(t, err, "compile %q must fail", src)
		require.Contains(t, err.Error(), want, "compile %q", src)
	}
}

func TestExprRuntimeErrors(t *testing.T) {
	n, err := compileExpr("round(1 / $.z, 2)")
	require.NoError(t, err)
	_, err = evalExpr(n, exprPayload(t, `{"z": 0}`))
	require.ErrorContains(t, err, "division by zero")

	n, err = compileExpr("$.s + 1")
	require.NoError(t, err)
	_, err = evalExpr(n, exprPayload(t, `{"s": "not a number"}`))
	require.ErrorContains(t, err, "needs a number")
}

func TestExprBitCap(t *testing.T) {
	huge := new(big.Rat).SetFrac(
		new(big.Int).Lsh(big.NewInt(1), exprMaxBits), big.NewInt(1))
	_, err := capBits(huge)
	require.ErrorContains(t, err, "exceeded")
}

func TestFormatRatNonTerminatingIsInternalError(t *testing.T) {
	_, err := formatRat(big.NewRat(1, 3))
	require.ErrorContains(t, err, "non-terminating")
}
