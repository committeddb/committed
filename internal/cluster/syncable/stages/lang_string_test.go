package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// evalErr compiles src and returns the data error evaluating it raises.
func evalErr(t *testing.T, src, payload string) error {
	t.Helper()
	n, err := Compile(src)
	require.NoError(t, err, "compile %q", src)
	_, err = Eval(n, exprPayload(t, payload), nil)
	require.Error(t, err, "eval %q", src)
	return err
}

// || propagates null; concat skips it. Both render numbers in canonical
// digits and booleans as true/false (PG's concat says t — a listed
// divergence), and || binds looser than + and tighter than comparison.
func TestExprConcat(t *testing.T) {
	p := `{"first":"Ada","last":"Lovelace","n":1.50,"b":true}`
	require.Equal(t, "Ada Lovelace", evalToString(t, "$.first || ' ' || $.last", p))
	require.Equal(t, "Ada Lovelace", evalToString(t, "$.first||' '||$.last", p), "|| delimits a path")
	require.Nil(t, evalToString(t, "$.first || $.missing", p), "|| propagates null")
	require.Equal(t, "Ada", evalToString(t, "concat($.first, $.missing)", p), "concat skips null")
	require.Equal(t, "", evalToString(t, "concat($.missing)", p), "all-null concat is empty, as in PG")
	require.Equal(t, "n=1.5", evalToString(t, "'n=' || $.n", p), "canonical digits, not source digits")
	require.Equal(t, "truex", evalToString(t, "$.b || 'x'", p))
	require.Equal(t, "truex", evalToString(t, "concat($.b, 'x')", p))
	require.Equal(t, "x3", evalToString(t, "'x' || 1 + 2", p), "+ binds tighter than ||")
	require.Equal(t, true, evalToString(t, "$.first || 'x' = 'Adax'", p), "|| binds tighter than =")

	require.ErrorContains(t, evalErr(t, "$.o || 'x'", `{"o":{"a":1}}`), "scalars")
	_, err := Compile("$.a | $.b")
	require.ErrorContains(t, err, "concatenation is ||")
	_, err = Compile("$.a || $.b / 3")
	require.ErrorContains(t, err, "round(...) or trunc(...)", "a rendered quotient still needs rounding")
}

func TestExprCaseMapping(t *testing.T) {
	require.Equal(t, "ada", evalToString(t, "lower('AdA')", `{}`))
	require.Equal(t, "ÀÉ", evalToString(t, "upper('àé')", `{}`))
	// Simple, locale-free case mapping — the pinned divergences.
	require.Equal(t, "ß", evalToString(t, "upper('ß')", `{}`), "PG's upper gives ẞ; simple mapping keeps ß")
	require.Equal(t, "Ა", evalToString(t, "upper('ა')", `{}`), "Georgian capitals (Unicode 11)")
	require.ErrorContains(t, evalErr(t, "lower($.n)", `{"n":5}`), "lower needs a string, got the number 5")
}

func TestExprTrimLengthReplace(t *testing.T) {
	require.Equal(t, "a b", evalToString(t, "trim('  a b  ')", `{}`))
	require.Equal(t, "\t a \t", evalToString(t, "trim($.s)", `{"s":"\t a \t"}`), "PG trims spaces only")
	require.Equal(t, "a  ", evalToString(t, "ltrim('  a  ')", `{}`))
	require.Equal(t, "  a", evalToString(t, "rtrim('  a  ')", `{}`))
	require.Equal(t, "a", evalToString(t, "trim('xyaxy', 'xy')", `{}`), "a second argument is the character set")
	require.Equal(t, "axx", evalToString(t, "ltrim('xxaxx', 'x')", `{}`))

	require.Equal(t, "5", evalToString(t, "length('héllo')", `{}`), "code points, not bytes")
	require.Equal(t, "5", evalToString(t, "char_length('héllo')", `{}`))
	require.Equal(t, "0", evalToString(t, "length('')", `{}`))

	require.Equal(t, "xx", evalToString(t, "replace('abab', 'ab', 'x')", `{}`))
	require.Equal(t, "aaa", evalToString(t, "replace('aaa', '', 'z')", `{}`), "an empty search string changes nothing")
	require.Equal(t, true, evalToString(t, "starts_with($.sku, 'SVC-')", `{"sku":"SVC-9"}`))
	require.Equal(t, false, evalToString(t, "starts_with($.sku, 'SVC-')", `{"sku":"svc-9"}`))

	// Strict: any null argument is null.
	require.Nil(t, evalToString(t, "upper($.missing)", `{}`))
	require.Nil(t, evalToString(t, "replace('a', $.missing, 'b')", `{}`))
	require.Nil(t, evalToString(t, "trim('a', $.missing)", `{}`))
}

// substring follows PG over code points: 1-based, the window clipped to
// the string, so a start below 1 eats into the count.
func TestExprSubstring(t *testing.T) {
	cases := map[string]string{
		"substring('hello', 2)":     "ello",
		"substring('hello', 2, 3)":  "ell",
		"substring('hello', 0, 3)":  "he",
		"substring('hello', -2)":    "hello",
		"substring('hello', -2, 3)": "",
		"substring('hello', 9)":     "",
		"substring('hello', 2, 0)":  "",
		"substring('héllo', 2, 2)":  "él",
		"substring('hello', 2.0)":   "ello",
	}
	for src, want := range cases {
		require.Equal(t, want, evalToString(t, src, `{}`), src)
	}
	require.Nil(t, evalToString(t, "substring('hello', $.missing)", `{}`))
	require.ErrorContains(t, evalErr(t, "substring('hello', 2, -1)", `{}`), "must not be negative")
	require.ErrorContains(t, evalErr(t, "substring('hello', 2.5)", `{}`), "needs an integer")
	require.ErrorContains(t, evalErr(t, "substring('hello', 3000000000)", `{}`), "out of the integer range")
	require.ErrorContains(t, evalErr(t, "substring($.n, 1)", `{"n":5}`), "needs a string")
}

func TestExprLike(t *testing.T) {
	match := map[[2]string]bool{
		{"SVC-9", "SVC-%"}:          true,
		{"svc-9", "SVC-%"}:          false, // case-sensitive
		{"abc", "a_c"}:              true,
		{"abbc", "a_c"}:             false,
		{"aéc", "a_c"}:              true, // _ is one code point
		{"abc", "%"}:                true,
		{"", "%"}:                   true,
		{"", "_"}:                   false,
		{"abc", "ab"}:               false, // anchored
		{"a_c", `a\_c`}:             true,
		{"abc", `a\_c`}:             false, // the escaped _ is literal
		{"100%", `100\%`}:           true,
		{"a%b%c", "a%c"}:            true,
		{"mississippi", "%iss%ppi"}: true,
		{"mississippi", "%iss%ppx"}: false,
		{"aaa", "%a%a%a%"}:          true,
		{"aa", "%a%a%a%"}:           false,
	}
	for in, want := range match {
		payload := map[string]any{"s": in[0], "p": in[1]}
		n, err := Compile("$.s like $.p")
		require.NoError(t, err)
		got, err := Eval(n, payload, nil)
		require.NoError(t, err)
		require.Equal(t, want, got, "%q like %q", in[0], in[1])
	}
	require.Equal(t, true, evalToString(t, "$.s not like 'x%'", `{"s":"abc"}`))
	require.Equal(t, false, evalToString(t, "$.s NOT LIKE 'a%'", `{"s":"abc"}`))
	require.Nil(t, evalToString(t, "$.missing like 'a%'", `{}`))
	require.Nil(t, evalToString(t, "$.missing not like 'a%'", `{}`), "not of null stays null")
	require.Equal(t, true, evalToString(t, "$.s like $.pre || '%'", `{"s":"abc","pre":"ab"}`), "|| binds tighter than like")

	require.ErrorContains(t, evalErr(t, `'abc' like 'ab\'`, `{}`), "must not end with the escape character")
	require.ErrorContains(t, evalErr(t, "$.n like '1%'", `{"n":12}`), "like needs a string")
}

// not in is not(x in …), three-valued: a null member with no match keeps
// the answer UNKNOWN.
func TestExprNotIn(t *testing.T) {
	require.Equal(t, true, evalToString(t, "$.k not in (1, 2)", `{"k":3}`))
	require.Equal(t, false, evalToString(t, "$.k not in (1, 2)", `{"k":2}`))
	require.Nil(t, evalToString(t, "$.k not in (1, $.missing)", `{"k":3}`))
	require.Nil(t, evalToString(t, "$.missing not in (1, 2)", `{}`))
	require.Equal(t, false, evalToString(t, "not $.k not in (1, 2)", `{"k":3}`), "a leading not still negates the predicate")
}

// The path walkers see inside like and the string functions.
func TestExprStringScopeWalkers(t *testing.T) {
	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
		When:  []WhenClause{{Expr: "$.name like $.project.prefix || '%'"}},
		Emit:  []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "REFOLD")

	n, err := Compile("upper(substring($parent.code, 1, 3))")
	require.NoError(t, err)
	require.True(t, exprReferencesParent(n))
}

func TestExprStringFunctionArity(t *testing.T) {
	for src, want := range map[string]string{
		"lower('a', 'b')":   "lower takes 1–1 arguments, got 2",
		"substring('a')":    "substring takes 2–3 arguments",
		"replace('a', 'b')": "replace takes 3–3 arguments",
		"concat()":          "unexpected",
		"reverse('a')":      "the closed set is",
	} {
		_, err := Compile(src)
		require.ErrorContains(t, err, want, src)
	}
}
