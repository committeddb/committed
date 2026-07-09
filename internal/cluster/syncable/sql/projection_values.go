package sql

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/PaesslerAG/jsonpath"
)

// isScalar reports whether a TOML-decoded literal is a scalar (string,
// number, bool). Tables and arrays are rejected at config time: as a
// when target they would compare structurally against decoded JSON
// (silently shape-sensitive), and as a set value they have no defined
// column binding.
func isScalar(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return false
	}
	return true
}

// keyString renders a correlation/element key as the text the sidecar stores. A
// string passes through; nil is empty; a JSON number (float64) prints without a
// decimal point for integral values (ordering 1 → "1", not "1.000000").
func keyString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// matchWhen reports whether every clause holds against the unmarshaled
// payload. A missing path is "no match", never an error: when is a
// filter, and events of other shapes simply don't match. That holds
// for null clauses too — jsonpath distinguishes a present null (nil,
// no error) from an absent field (error), and only the former matches.
func matchWhen(clauses []WhenClause, jsonData any) bool {
	for _, c := range clauses {
		v, err := jsonpath.Get(c.Path, jsonData)
		if err != nil {
			return false
		}
		if c.Null {
			if v != nil {
				return false
			}
			continue
		}
		if !literalEquals(c.Equals, v) {
			return false
		}
	}
	return true
}

// literalEquals compares a TOML literal against a decoded JSON value.
// Numbers need normalizing: TOML integers decode as int64 while JSON
// numbers decode as float64, and == across those types is always
// false.
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
		// Payloads now decode with UseNumber, so a JSON numeric leaf arrives as
		// json.Number; float64 comparison preserves the prior (float64-decoded)
		// when-clause matching exactly. A when clause filters small discriminator
		// values, so the float64 range is not a concern here (unlike value binding,
		// which keeps exact digits via coerceForColumn).
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// bindable converts a jsonpath result into something database/sql can
// bind. A json.Number binds as its source digits (payloads are decoded
// with UseNumber, so a numeric key or value keeps full precision — a raw
// float64 would round-trip-corrupt integers above 2^53 and any decimal).
// Objects and arrays (e.g. an allocs subtree headed for a JSONB column)
// re-marshal to JSON text — drivers cannot bind a Go map; that re-marshal
// normalizes key order but, with UseNumber, no longer mangles numbers.
// Other scalars pass through. For type-aware value binding (a number into
// an INTEGER vs a DECIMAL column) callers route through coerceForColumn;
// bindable is the untyped fallback for keys, which are text.
func bindable(v any) any {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case map[string]any, []any:
		bs, err := json.Marshal(v)
		if err != nil {
			return v // unmarshalable shapes don't exist post-Unmarshal; let the driver report
		}
		return string(bs)
	}
	return v
}
