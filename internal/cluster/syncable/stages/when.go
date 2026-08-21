// Package stages is the sink-agnostic staged-computation engine: the
// stage vocabulary and its keyed-refold evaluator over the stage store,
// plus the closed expression language and when-clause matching the
// vocabulary is built from. Per the terminal rule, stages are internal
// middleware any syncable kind can host — the sql package wires them to
// its table terminal; nothing here knows what a sink is.
package stages

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/PaesslerAG/jsonpath"
)

// WhenClause is one match condition: exactly one of Equals (the value
// at Path must equal it), Null (the value must be JSON null), NotEquals
// (present, same scalar family, and different), or GreaterThan/LessThan
// (numeric, strictly ordered against a numeric literal) must be set. A
// rule matches when every one of its clauses holds (AND); a missing
// Path is "no match", never an error — and that invariant is why a Null
// clause matches only a PRESENT null.
//
// Comparison semantics are SQL's, deliberately: the engine's folds
// already follow SQL (aggregates skip nulls, the argmax orders nulls
// lowest) and acceptance is measured against SQL oracles, so a missing
// or null value matches NO comparison — including NotEquals (SQL's
// NULL <> x is not true). Cross-family values (a string where the
// literal is a number) match neither Equals nor NotEquals, the same
// typed-equality rule that keeps `equals = "true"` from matching a JSON
// boolean. That is the whole language: single-field, compare-to-literal
// (the field-measured predicate set) — boolean expressions stay out.
type WhenClause struct {
	Path   string `mapstructure:"path"`
	Equals any    `mapstructure:"equals"`
	Null   bool   `mapstructure:"null"`
	// NotNull matches a PRESENT, non-null value (SQL's IS NOT NULL) —
	// the complement of Null, and the merge's left/inner gate: a merged
	// side's alias path resolves null when that upstream holds no output
	// for the key.
	NotNull   bool `mapstructure:"notNull"`
	NotEquals any  `mapstructure:"notEquals"`
	// GreaterThan/LessThan take numeric literals only (validated at
	// admission); values compare numerically, non-numbers never match.
	GreaterThan any `mapstructure:"greaterThan"`
	LessThan    any `mapstructure:"lessThan"`
}

// MarshalJSON renders a clause's DECLARED content for the store
// fingerprint: the path plus exactly the arm that is set. Explicit —
// not omitempty — because the comparison arms are any-typed and a legal
// zero-valued literal (equals = false, equals = 0) would vanish under
// omitempty, colliding fingerprints of semantically different configs.
func (c WhenClause) MarshalJSON() ([]byte, error) {
	m := map[string]any{"path": c.Path}
	switch {
	case c.Null:
		m["null"] = true
	case c.NotNull:
		m["notNull"] = true
	case c.NotEquals != nil:
		m["notEquals"] = c.NotEquals
	case c.GreaterThan != nil:
		m["greaterThan"] = c.GreaterThan
	case c.LessThan != nil:
		m["lessThan"] = c.LessThan
	case c.Equals != nil:
		m["equals"] = c.Equals
	}
	return json.Marshal(m)
}

// IsScalar reports whether a TOML-decoded literal is a scalar (string,
// number, bool). Tables and arrays are rejected at config time: they
// would compare structurally against decoded JSON, silently
// shape-sensitive.
func IsScalar(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return false
	}
	return true
}

// KeyString renders a correlation/element key as canonical text. A
// string passes through; nil is empty; numbers print without a decimal
// point for integral values.
func KeyString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Match reports whether every clause holds against the unmarshaled
// payload. A missing path is "no match", never an error: when is a
// filter, and events of other shapes simply don't match. That holds
// for null clauses too — jsonpath distinguishes a present null (nil,
// no error) from an absent field (error), and only the former matches.
func Match(clauses []WhenClause, jsonData any) bool {
	return MatchScoped(clauses, jsonData, nil)
}

// MatchScoped is Match with the enclosing scope in reach: inside a
// forEach fan, data is the ELEMENT and parent the enclosing event, so a
// `$parent.` path lets a per-element when test a parent field (the
// field's b6 shape — an event-type filter on a fan stage — written the
// natural way). Outside a fan, parent is nil and a `$parent.` path
// matches nothing (admission rejects it where no parent can exist).
func MatchScoped(clauses []WhenClause, jsonData, parent any) bool {
	for _, c := range clauses {
		v, err := ResolvePath(c.Path, jsonData, parent)
		if err != nil {
			return false
		}
		switch {
		case c.Null:
			if v != nil {
				return false
			}
		case c.NotNull:
			if v == nil {
				return false
			}
		case c.NotEquals != nil:
			// SQL semantics: null never satisfies <>, and a cross-family
			// value (string vs number literal) is no match either way.
			if v == nil || !sameScalarFamily(c.NotEquals, v) || literalEquals(c.NotEquals, v) {
				return false
			}
		case c.GreaterThan != nil:
			if !numericallyOrdered(v, c.GreaterThan, false) {
				return false
			}
		case c.LessThan != nil:
			if !numericallyOrdered(v, c.LessThan, true) {
				return false
			}
		default:
			if !literalEquals(c.Equals, v) {
				return false
			}
		}
	}
	return true
}

// sameScalarFamily reports whether a decoded value is comparable to a
// TOML literal: both numeric, or the same non-numeric scalar type.
func sameScalarFamily(lit, v any) bool {
	if _, ok := toFloat(lit); ok {
		_, ok2 := toFloat(v)
		return ok2
	}
	switch lit.(type) {
	case string:
		_, ok := v.(string)
		return ok
	case bool:
		_, ok := v.(bool)
		return ok
	}
	return false
}

// numericallyOrdered reports v < lit (less=true) or v > lit (less=false),
// SQL-style: a null, missing, or non-numeric value matches neither.
// float64 comparison for the same reason toFloat documents: when filters
// small post-fold values; value BINDING is what keeps exact digits.
func numericallyOrdered(v, lit any, less bool) bool {
	lf, ok := toFloat(lit)
	if !ok {
		return false // rejected at admission; unreachable via a validated config
	}
	vf, ok := toFloat(v)
	if !ok {
		return false
	}
	if less {
		return vf < lf
	}
	return vf > lf
}

// literalEquals compares a TOML literal against a decoded JSON value.
// Numbers need normalizing: TOML integers decode as int64 while JSON
// numbers decode as float64/json.Number, and == across those types is
// always false.
func literalEquals(want, got any) bool {
	if wf, ok := toFloat(want); ok {
		gf, ok2 := toFloat(got)
		return ok2 && wf == gf
	}
	return reflect.DeepEqual(want, got)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		// Payloads decode with UseNumber; float64 comparison preserves the
		// prior when-clause matching exactly. A when clause filters small
		// discriminator values, so the float64 range is not a concern here
		// (unlike value binding, which keeps exact digits).
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// CanonicalKeyPart renders one key part for the store's byte-exact key
// space. Strings pass through UNTOUCHED — an entity key "007" is text,
// not a number; only genuinely numeric JSON values canonicalize — and
// numbers render in canonical minimal-decimal form: "5", 5.0000, and
// 5e0 are ONE key, while 5.25 keeps its digits. Source digits are
// exactness for VALUES (bindings keep them); for KEYS they are just
// bytes, and the field defect was a CDC-rendered 5.0000 against a
// jsonColumns-decoded 5 silently never matching (an anti-join that
// suppressed nothing, in either direction).
func CanonicalKeyPart(v any) string {
	switch n := v.(type) {
	case json.Number:
		return canonicalNumericDigits(n.String())
	case float64:
		return canonicalNumericDigits(strconv.FormatFloat(n, 'f', -1, 64))
	case float32:
		return canonicalNumericDigits(strconv.FormatFloat(float64(n), 'f', -1, 32))
	case int, int32, int64, uint, uint32, uint64:
		return fmt.Sprintf("%d", n)
	default:
		return KeyString(v)
	}
}

// TypedKeyPart renders one key part in its DECLARED comparison space
// (see Stage.KeyType). "text" (or empty) is CanonicalKeyPart — strings
// verbatim, typed numbers canonical. "number" additionally coerces
// STRING renderings through the same canonicalization, closing the
// cross-source hazard where one producer serializes 5 as "5.0000";
// ok=false means the value cannot render into the declared space (a
// non-numeric string under "number") — non-membership, like a missing
// key part.
func TypedKeyPart(keyType string, v any) (string, bool) {
	if keyType != KeyTypeNumber {
		return CanonicalKeyPart(v), true
	}
	switch n := v.(type) {
	case string:
		out, ok := numericDigits(n)
		return out, ok
	case json.Number, float64, float32, int, int32, int64, uint, uint32, uint64:
		return CanonicalKeyPart(v), true
	default:
		return "", false
	}
}

// canonicalNumericDigits normalizes a JSON numeric literal's digit
// string exactly (no floating point): exponent applied, leading integer
// zeros and trailing fractional zeros stripped, "-0" folded to "0". A
// string that fails to parse as a JSON number passes through verbatim.
func canonicalNumericDigits(s string) string {
	out, ok := numericDigits(s)
	if !ok {
		return s
	}
	return out
}

// numericDigits is canonicalNumericDigits with an explicit validity
// verdict (ok=false: not a numeric literal).
func numericDigits(s string) (string, bool) {
	neg := false
	rest := s
	switch {
	case strings.HasPrefix(rest, "-"):
		neg, rest = true, rest[1:]
	case strings.HasPrefix(rest, "+"):
		rest = rest[1:]
	}
	mant := rest
	exp := 0
	if i := strings.IndexAny(rest, "eE"); i >= 0 {
		e, err := strconv.Atoi(rest[i+1:])
		if err != nil {
			return "", false
		}
		mant, exp = rest[:i], e
	}
	intPart, fracPart := mant, ""
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, fracPart = mant[:i], mant[i+1:]
	}
	digits := intPart + fracPart
	if digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return "", false
	}
	// point = number of digits left of the decimal point after applying
	// the exponent.
	point := len(intPart) + exp
	// Strip leading zeros (tracking the point) and trailing zeros.
	start := 0
	for start < len(digits)-1 && digits[start] == '0' {
		start++
	}
	point -= start
	digits = digits[start:]
	end := len(digits)
	for end > 1 && digits[end-1] == '0' && end > point {
		end--
	}
	digits = digits[:end]
	if digits == "0" {
		return "0", true
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	switch {
	case point <= 0:
		b.WriteString("0.")
		for i := 0; i < -point; i++ {
			b.WriteByte('0')
		}
		b.WriteString(digits)
	case point >= len(digits):
		b.WriteString(digits)
		for i := 0; i < point-len(digits); i++ {
			b.WriteByte('0')
		}
	default:
		b.WriteString(digits[:point])
		b.WriteByte('.')
		b.WriteString(digits[point:])
	}
	return b.String(), true
}

// NormalizeLower is the one supported key normalization. Cross-source
// keys can render the same logical value differently (the field case:
// SQL Server CDC renders uniqueidentifier GUIDs UPPERCASE while
// application JSON serializers write them lowercase — RFC 4122's
// canonical form), and every key comparison here is deliberately
// byte-exact, so the mismatch is silent non-participation. normalize =
// "lower" on a key-bearing declaration folds the rendering once, at the
// key seam — never on payload values.
const NormalizeLower = "lower"

// NormalizeKeyPart renders one key part into its declared canonical
// form. Only letters change (digits are caseless), so it is safe to
// apply to any rendered part. Every key-rendering seam routes through
// here so both sides of a comparison agree by construction.
func NormalizeKeyPart(mode, part string) string {
	if mode == NormalizeLower {
		return strings.ToLower(part)
	}
	return part
}

// NormalizeKeyValue is NormalizeKeyPart for a typed key value (the sql
// side binds coerced values, not rendered parts): strings lower,
// everything else passes through untouched.
func NormalizeKeyValue(mode string, v any) any {
	if mode == NormalizeLower {
		if s, ok := v.(string); ok {
			return strings.ToLower(s)
		}
	}
	return v
}

// ValidNormalize reports whether a normalize declaration is supported
// ("" = none).
func ValidNormalize(mode string) bool {
	return mode == "" || mode == NormalizeLower
}

// RejectParentPaths is the admission guard for `$parent.` in when
// clauses: only a forEach fan has an enclosing scope, so anywhere else
// the path would silently never match — reject it loudly instead.
func RejectParentPaths(clauses []WhenClause, where string) error {
	for _, c := range clauses {
		if strings.HasPrefix(c.Path, "$parent") {
			return fmt.Errorf("%s: path [%s] — $parent is only meaningful where a fan provides an enclosing scope (a forEach stage's when/deleteWhen, a forEach source's rule when); here it would silently never match", where, c.Path)
		}
	}
	return nil
}

// MultiValuedPath reports whether a jsonpath can yield more than one
// value (wildcards, recursive descent, filters, slices). In a VALUE
// position such a path produces parallel arrays that look like data and
// are silently wrong (field-verified).
func MultiValuedPath(p string) bool {
	return strings.ContainsAny(p, "*?") || strings.Contains(p, "..")
}

// RejectMultiValued returns a loud config error when a value-position
// jsonpath is multi-valued.
func RejectMultiValued(p, where string) error {
	if MultiValuedPath(p) {
		return fmt.Errorf("%s: jsonpath [%s] is multi-valued (wildcard/recursive/filter) — in a value position that produces parallel arrays, not row values; use a single-valued path (row fan-out is the forEach capability, not a projection column)", where, p)
	}
	return nil
}

// ResolvePath resolves a value-position jsonpath in the rule scope: a
// `$parent.` prefix reaches the enclosing scope (the forEach element's
// event); everything else resolves against data. Outside forEach, parent
// is nil and a $parent path is a loud misconfiguration.
func ResolvePath(path string, data, parent any) (any, error) {
	if rest, ok := strings.CutPrefix(path, "$parent"); ok {
		if parent == nil {
			return nil, fmt.Errorf("path [%s]: $parent is only meaningful inside a forEach source", path)
		}
		return jsonpath.Get("$"+rest, parent)
	}
	return jsonpath.Get(path, data)
}
