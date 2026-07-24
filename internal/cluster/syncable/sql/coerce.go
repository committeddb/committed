package sql

import (
	"encoding/base64"
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
	if columnIsBinary(sqlType) {
		// A binary target column: the payload carries the bytes base64-encoded
		// (see sql.JSONValue's CatBinary). Decode back to raw bytes and bind []byte,
		// which both pgx and go-sql-driver write to a binary column. Fall back to
		// text-binding when the value isn't valid base64 — e.g. a legacy mapping
		// that sends Postgres's "\x…" hex text straight into a bytea column, which
		// Postgres itself parses; base64 decoding rejects it (the '\' isn't in the
		// alphabet), so that path keeps working untouched.
		if s, ok := v.(string); ok {
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				return b
			}
		}
		return asText(v)
	}
	if columnIsNumericOrBool(sqlType) {
		// Numeric/bool target: the driver wants a native scalar. A JSON bool is
		// already a Go bool; only json.Number needs unwrapping from its string
		// representation into the integer or float the codec can encode.
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				return i
			}
			// Int64 overflowed: a BIGINT UNSIGNED value above 2^63 is an integer
			// that simply does not fit int64, or the value is a genuine non-integer.
			// Bind the exact source digits as a string, which the driver parses
			// losslessly, for EVERY column except the approximate IEEE-float types
			// (FLOAT/DOUBLE/REAL): only those take a native float64, their on-the-wire
			// form. Rounding a large unsigned integer, or an exact decimal like
			// 7922816251426433.75, through float64 would silently corrupt the value.
			if columnIsApproximateFloat(sqlType) {
				if f, err := n.Float64(); err == nil {
					return f
				}
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

// columnIsBinary reports whether a declared type is a binary column — the types
// whose payload value is a base64 string that must be decoded back to raw bytes
// before binding, rather than text-bound. Covers Postgres bytea and the MySQL
// BLOB/BINARY family; compared on the leading type token like the others.
func columnIsBinary(sqlType string) bool {
	switch leadingTypeToken(sqlType) {
	case "BYTEA", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY":
		return true
	}
	return false
}

// columnIsApproximateFloat reports whether a declared type is an approximate
// IEEE-float column (FLOAT/DOUBLE/REAL and their aliases) — the only columns for
// which a non-integer, or an integer too large for int64, is correctly bound as
// a native float64. Every other numeric column binds its source digits as a
// string instead: the integer types, and the exact fixed-precision
// DECIMAL/NUMERIC/MONEY family. So a value float64 cannot represent — a BIGINT
// UNSIGNED above 2^63, or a high-precision decimal — reaches the driver
// uncorrupted rather than silently rounded.
func columnIsApproximateFloat(sqlType string) bool {
	switch leadingTypeToken(sqlType) {
	case "REAL", "DOUBLE", "FLOAT", "FLOAT4", "FLOAT8":
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
