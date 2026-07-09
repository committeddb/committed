package sql

import (
	"encoding/json"
	"fmt"
	"strings"
)

// coerceForColumn renders a value extracted from an entity payload into the
// form its destination column expects, given the column's declared SQL type.
//
// Typed ingest payloads carry JSON-native scalars: a numeric source column
// arrives as a JSON number (decoded here as json.Number), a boolean as a JSON
// bool, an embedded json/jsonb column as a JSON object. A sink column, though,
// has whatever type the mapping declared — commonly TEXT even for a numeric
// source (denormalizing a key into a text BFF column is a normal choice). The
// driver will not implicitly bridge the two: pgx has no plan to encode a number
// into a text column ("cannot find encode plan"), so a number mapped into TEXT
// must be handed over as its text form. Symmetrically, a json.Number mapped
// into an INTEGER column must become a native Go integer the numeric codec
// accepts, not the string json.Number is under the hood.
//
// nil (SQL NULL) always passes through untouched.
func coerceForColumn(v any, sqlType string) any {
	if v == nil {
		return nil
	}
	if columnIsNumericOrBool(sqlType) {
		// Numeric/bool target: the driver wants a native scalar. A JSON bool is
		// already a Go bool; only json.Number needs unwrapping from its string
		// representation into the integer or float the codec can encode.
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				return i
			}
			// Not an integer. For an exact-numeric column (DECIMAL/NUMERIC/MONEY)
			// bind the source digits as a string — the driver parses them losslessly,
			// whereas a float64 round-trip corrupts a value it can't represent
			// (e.g. 7922816251426433.75). Only approximate columns (FLOAT/DOUBLE/REAL),
			// which are IEEE floats anyway, take the native float64.
			if columnIsExactNumeric(sqlType) {
				return n.String()
			}
			if f, err := n.Float64(); err == nil {
				return f
			}
			return n.String()
		}
		return v
	}
	// Text/temporal/uuid/json/everything-else target: bind the value's text
	// form, the representation Postgres parses for all of these.
	return asText(v)
}

// asText renders a JSON-decoded value as the text a string-ish column stores:
// the string itself, a number's exact digits (json.Number preserves the source
// text, so no float64 round-trip corrupts large integers or decimals), a bool
// as "true"/"false", and any composite (an embedded JSON object/array) as its
// compact JSON encoding.
func asText(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		if x {
			return "true"
		}
		return "false"
	case json.RawMessage:
		return string(x)
	case []byte:
		return string(x)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// columnIsNumericOrBool reports whether a declared SQL column type is a numeric
// or boolean type — the types whose values the driver wants as native Go
// scalars rather than text. Everything else (TEXT/VARCHAR/CHAR, DATE/TIMESTAMP,
// UUID, JSON/JSONB, BYTEA, …) is treated as text-binding: Postgres parses a
// text representation into all of those, so stringifying is universally safe,
// and defaulting to text matches the all-strings shape the syncable bound
// before typed payloads existed.
//
// It compares the leading type token (so "DOUBLE PRECISION" → DOUBLE,
// "NUMERIC(15,2)" → NUMERIC, "CHARACTER VARYING" → CHARACTER) against the
// Postgres and MySQL numeric/bool spellings.
func columnIsNumericOrBool(sqlType string) bool {
	switch leadingTypeToken(sqlType) {
	case "INT", "INTEGER", "INT2", "INT4", "INT8",
		"SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
		"SERIAL", "SMALLSERIAL", "BIGSERIAL",
		"DECIMAL", "NUMERIC", "DEC", "FIXED", "NUMBER",
		"REAL", "DOUBLE", "FLOAT", "FLOAT4", "FLOAT8", "MONEY",
		"BOOL", "BOOLEAN":
		return true
	}
	return false
}

// columnIsExactNumeric reports whether a declared type stores numbers exactly
// (fixed/arbitrary precision), so a non-integer value must be bound as its source
// digits rather than a lossy float64. Approximate types (FLOAT/DOUBLE/REAL) are
// excluded — they are IEEE floats where float64 is the native, correct form.
func columnIsExactNumeric(sqlType string) bool {
	switch leadingTypeToken(sqlType) {
	case "DECIMAL", "NUMERIC", "DEC", "FIXED", "NUMBER", "MONEY":
		return true
	}
	return false
}

// leadingTypeToken upper-cases a SQL type and returns its leading identifier —
// the run of characters before the first space, parenthesis, or comma. It lets
// columnIsNumericOrBool match on the base type without enumerating every
// precision/scale or multi-word variant.
func leadingTypeToken(sqlType string) string {
	t := strings.ToUpper(strings.TrimSpace(sqlType))
	if i := strings.IndexAny(t, " (,\t"); i >= 0 {
		t = t[:i]
	}
	return t
}
