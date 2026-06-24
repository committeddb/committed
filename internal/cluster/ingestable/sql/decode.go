package sql

import (
	"encoding/json"
	"regexp"
)

// JSONCategory is how a source column's value should be rendered in the
// ingested JSON payload. A dialect maps its own type metadata — PostgreSQL type
// OIDs (CDC) or database type names (snapshot), MySQL column types — onto these,
// and JSONValue does the rendering, so both ingest paths and both dialects agree
// on the shape.
type JSONCategory int

const (
	CatText   JSONCategory = iota // string — the default: text, dates, uuid, bytea, …
	CatNumber                     // JSON number — int, float, numeric/decimal
	CatBool                       // JSON bool
	CatJSON                       // embedded JSON — json/jsonb columns
)

// jsonNumberRe matches a JSON number literal (the RFC 8259 grammar). It rejects
// the non-JSON spellings a SQL source can still produce for a numeric column —
// NaN, Infinity, the empty string — so those fall back to a string rather than
// emitting an invalid payload.
var jsonNumberRe = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

// JSONValue renders one source column value as the natural JSON value for its
// category. raw is the value as the ingest read it: nil for SQL NULL, a string
// or []byte for a text representation (the CDC path, and most snapshot columns),
// or an already-typed scalar the driver decoded (int64/float64/bool/time.Time —
// left as-is, since those marshal to the right JSON type already).
//
// A value that doesn't fit its category — a numeric column somehow holding
// "NaN", a json column holding non-JSON — falls back to a string, so a
// malformed source never produces an invalid payload. Numbers are carried as
// json.Number, so the exact source text (precision, trailing zeros) survives
// into the log byte-for-byte; the consumer decodes it however it likes. Keeping
// numbers as their source text is also what makes the snapshot and CDC paths
// produce identical payload bytes for the same row.
func JSONValue(raw any, cat JSONCategory) any {
	var text string
	switch x := raw.(type) {
	case nil:
		return nil
	case string:
		text = x
	case []byte:
		text = string(x)
	default:
		return x // already a JSON-native scalar the driver typed for us
	}

	switch cat {
	case CatNumber:
		if jsonNumberRe.MatchString(text) {
			return json.Number(text)
		}
	case CatBool:
		switch text {
		case "t", "true", "TRUE", "True":
			return true
		case "f", "false", "FALSE", "False":
			return false
		}
	case CatJSON:
		if json.Valid([]byte(text)) {
			return json.RawMessage(text)
		}
	}
	return text
}
