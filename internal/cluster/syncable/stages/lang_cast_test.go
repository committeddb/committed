package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// numeric accepts PostgreSQL 16's input syntax exactly: whitespace, sign,
// fraction, exponent, 0x/0o/0b prefixes, and single underscores between
// digits.
func TestExprCastNumeric(t *testing.T) {
	ok := map[string]string{
		"1e3":     "1000",
		" 5 ":     "5",
		"-0.50":   "-0.5",
		"+.5":     "0.5",
		"5.":      "5",
		"-.5e-1":  "-0.05",
		"1E+2":    "100",
		"0x1F":    "31",
		"0X_1f":   "31",
		"0o17":    "15",
		"-0b101":  "-5",
		"1_000.5": "1000.5",
		"1.5_5":   "1.55",
		"1e1_0":   "10000000000",
		"007":     "7",
		"5.0000":  "5",
		"123456789012345678901234567890.123456789": "123456789012345678901234567890.123456789",
	}
	for in, want := range ok {
		require.Equal(t, want, evalToString(t, "cast($.s as numeric)", `{"s":"`+in+`"}`), "%q", in)
	}
	for _, in := range []string{"", "+", ".", "1e", "x", "1__0", "_1", "1_", "1_.5", "1._5", "0x", "0x1.8", "1.2.3", "١"} {
		require.ErrorContains(t, evalErr(t, "cast($.s as numeric)", `{"s":"`+in+`"}`), "invalid input for numeric", "%q", in)
	}
	require.ErrorContains(t, evalErr(t, "cast($.s as numeric)", `{"s":"NaN"}`), "no exact decimal value")
	require.ErrorContains(t, evalErr(t, "cast($.s as numeric)", `{"s":"-Infinity"}`), "no exact decimal value")
	require.ErrorContains(t, evalErr(t, "cast($.s as numeric)", `{"s":"1e999999"}`), "out of range")
	require.ErrorContains(t, evalErr(t, "cast($.b as numeric)", `{"b":true}`), "cannot cast a boolean to numeric")

	require.Equal(t, "2.5", evalToString(t, "cast($.n as decimal)", `{"n":2.50}`), "a number passes through")
	require.Nil(t, evalToString(t, "cast($.missing as numeric)", `{}`))
}

// integer/bigint: numbers round half away from zero; strings take integer
// syntax only (no fraction, no exponent); booleans are 1/0; PG's ranges.
func TestExprCastInteger(t *testing.T) {
	cases := map[string]string{
		"cast(2.5 as integer)":                "3",
		"cast(-2.5 as integer)":               "-3",
		"cast(2.4 as int)":                    "2",
		"cast(' 7 ' as int4)":                 "7",
		"cast('-0x10' as integer)":            "-16",
		"cast('1_000' as integer)":            "1000",
		"cast(true as integer)":               "1",
		"cast(false as integer)":              "0",
		"cast('0xFFFFFFFF' as bigint)":        "4294967295",
		"cast(-2147483648.4 as integer)":      "-2147483648",
		"cast('9223372036854775807' as int8)": "9223372036854775807",
	}
	for src, want := range cases {
		require.Equal(t, want, evalToString(t, src, `{}`), src)
	}
	for src, want := range map[string]string{
		"cast('2.5' as integer)":                "invalid input for integer",
		"cast('1e2' as integer)":                "invalid input for integer",
		"cast('0xFFFFFFFF' as integer)":         "out of range for integer",
		"cast(2147483647.5 as integer)":         "out of range for integer",
		"cast('9223372036854775808' as bigint)": "out of range for bigint",
	} {
		require.ErrorContains(t, evalErr(t, src, `{}`), want, src)
	}
}

func TestExprCastTextAndBoolean(t *testing.T) {
	require.Equal(t, "1.5", evalToString(t, "cast($.n as text)", `{"n":1.50}`), "canonical digits")
	require.Equal(t, "true", evalToString(t, "cast($.b as varchar)", `{"b":true}`))
	require.Equal(t, "x", evalToString(t, "cast('x' as text)", `{}`))

	truthy := map[string]bool{
		"t": true, "TrUe": true, " true ": true, "tr": true, "y": true, "yes": true, "on": true, "1": true,
		"f": false, "FALSE": false, "n": false, "no": false, "of": false, "off": false, "0": false,
	}
	for in, want := range truthy {
		require.Equal(t, want, evalToString(t, "cast($.s as boolean)", `{"s":"`+in+`"}`), "%q", in)
	}
	for _, in := range []string{"", "o", "10", "truex", "yess", "onn", "2"} {
		require.ErrorContains(t, evalErr(t, "cast($.s as bool)", `{"s":"`+in+`"}`), "invalid input for boolean", "%q", in)
	}
	require.Equal(t, true, evalToString(t, "cast(2 as boolean)", `{}`), "an integer casts: nonzero is true")
	require.Equal(t, false, evalToString(t, "cast(0 as boolean)", `{}`))
	require.ErrorContains(t, evalErr(t, "cast(2.5 as boolean)", `{}`), "non-integer number")
}

// x::t is cast(x as t), binding tighter than unary minus (PG), and it
// chains.
func TestExprCastPostfix(t *testing.T) {
	require.Equal(t, "6", evalToString(t, "$.s::numeric + 1", `{"s":"5"}`))
	require.Equal(t, "6", evalToString(t, "$.s::int+1", `{"s":"5"}`), ":: delimits a path")
	require.Equal(t, "-5", evalToString(t, "-'5'::integer", `{}`))
	require.Equal(t, "3", evalToString(t, "'2.5'::numeric::integer", `{}`))
	require.Equal(t, true, evalToString(t, "$.s::numeric = 5", `{"s":"5.000"}`))
	require.Equal(t, "Status 2", evalToString(t, "'Status ' || $.s::int::text", `{"s":"2"}`))
}

// try_cast is cast that yields null instead of failing on a conversion —
// but an operand that is not a scalar is still a data error.
func TestExprTryCast(t *testing.T) {
	require.Equal(t, "5", evalToString(t, "try_cast($.s as numeric)", `{"s":"5"}`))
	require.Nil(t, evalToString(t, "try_cast($.s as numeric)", `{"s":"n/a"}`))
	require.Nil(t, evalToString(t, "try_cast($.s as integer)", `{"s":"2.5"}`))
	require.Nil(t, evalToString(t, "try_cast($.s as boolean)", `{"s":"maybe"}`))
	require.Equal(t, "0", evalToString(t, "coalesce(try_cast($.s as numeric), 0)", `{"s":""}`))
	require.ErrorContains(t, evalErr(t, "try_cast($.o as text)", `{"o":{"a":1}}`), "scalars")
}

func TestExprCastAdmission(t *testing.T) {
	for src, want := range map[string]string{
		"cast($.a as float)":       "unknown cast type \"float\"",
		"$.a::varchar(10)":         "unexpected trailing",
		"cast($.a)":                "expected as in cast",
		"cast($.a as text":         "expected \")\"",
		"$.a:int":                  "a cast is ::type",
		"$.a::":                    "unknown cast type",
		"cast($.a / 3 as numeric)": "round(...) or trunc(...)",
		"cast($.a / 3 as text)":    "round(...) or trunc(...)",
		"abs($.a / 3)":             "round(...) or trunc(...)",
		"mod($.a / 3, 1)":          "round(...) or trunc(...)",
		"greatest($.a / 3, 1)":     "round(...) or trunc(...)",
	} {
		_, err := Compile(src)
		require.ErrorContains(t, err, want, src)
	}
	for _, src := range []string{
		"cast($.a / 3 as integer)", // an integer cast rounds
		"ceil($.a / 3)",            // ceil/floor materialize
		"floor($.a / 3) + 1",
		"cast($.a / 3 > 1 as boolean)",
	} {
		_, err := Compile(src)
		require.NoError(t, err, src)
	}
}

func TestExprMathFunctions(t *testing.T) {
	cases := map[string]any{
		"abs(-1.5)":                   "1.5",
		"ceil(1.2)":                   "2",
		"ceil(-1.2)":                  "-1",
		"ceiling(3)":                  "3",
		"floor(-1.2)":                 "-2",
		"floor(1.9)":                  "1",
		"mod(-7, 3)":                  "-1",
		"mod(7, -3)":                  "1",
		"mod(5.5, 2)":                 "1.5",
		"mod(-5.5, 2)":                "-1.5",
		"abs($.missing)":              nil,
		"mod(5, $.missing)":           nil,
		"greatest(1, $.missing, 3)":   "3",
		"least($.missing, $.missing)": nil,
		"greatest('a', 'B')":          "a", // byte order
		"least(2.50, 3)":              "2.5",
		"greatest($.qty::numeric, 0)": "0",
		"least(true, false)":          false,
	}
	for src, want := range cases {
		require.Equal(t, want, evalToString(t, src, `{"qty":"-4"}`), src)
	}
	require.ErrorContains(t, evalErr(t, "mod(5, 0)", `{}`), "division by zero")
	require.ErrorContains(t, evalErr(t, "abs('x')", `{}`), "arithmetic needs a number")
	require.ErrorContains(t, evalErr(t, "greatest(1, 'a')", `{}`), "same kind")
}

// x between a and b is a <= x and x <= b — SQL's definition, three-valued
// logic and the ordering families included.
func TestExprBetween(t *testing.T) {
	cases := map[string]any{
		"2 between 1 and 3":                                  true,
		"1 between 1 and 3":                                  true,
		"2 not between 1 and 3":                              false,
		"2 between 3 and 1":                                  false, // not symmetric
		"$.missing between 1 and 3":                          nil,
		"5 between 1 and $.missing":                          nil, // true and unknown is unknown
		"'b' between 'a' and 'c'":                            true,
		"'2026-02-01' between '2026-01-01' and '2026-03-31'": true,
		"2 between 1 and 3 and false":                        false, // the outer and is a connective
		"$.n between 1 + 1 and 3 * 2":                        true,
	}
	for src, want := range cases {
		require.Equal(t, want, evalToString(t, src, `{"n":4}`), src)
	}
	require.ErrorContains(t, evalErr(t, "'b' between 1 and 3", `{}`), "same kind")
	_, err := Compile("$.n between 1 3")
	require.ErrorContains(t, err, "expected and in between")
}

// The path walkers see inside casts.
func TestExprCastScopeWalkers(t *testing.T) {
	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
		When:  []WhenClause{{Expr: "$.project.n::integer > 1"}},
		Emit:  []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "REFOLD")
	n, err := Compile("try_cast($parent.n as integer)")
	require.NoError(t, err)
	require.True(t, exprReferencesParent(n))
}
