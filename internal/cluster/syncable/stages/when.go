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
	"strings"

	"github.com/PaesslerAG/jsonpath"
)

// WhenClause is one match condition: exactly one of Equals (the value
// at Path must equal it) or Null (the value at Path must be JSON null)
// must be set. A rule matches when every one of its clauses holds
// (AND); a missing Path is "no match", never an error — and that
// invariant is why a Null clause matches only a PRESENT null. There is
// no negation; the when language is equality-only.
type WhenClause struct {
	Path   string `mapstructure:"path"`
	Equals any    `mapstructure:"equals"`
	Null   bool   `mapstructure:"null"`
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
