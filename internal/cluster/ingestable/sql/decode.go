package sql

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
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
		if canon, ok := canonicalJSON(text); ok {
			return canon
		}
	}
	return text
}

// canonicalJSON re-serializes a JSON document into a stable canonical form so the
// snapshot and CDC paths emit byte-identical bytes for the same value. Both paths
// hand JSONValue a valid JSON document, but in different spellings — e.g. MySQL's
// snapshot returns keys in length-then-bytes order while go-mysql's binlog decode
// re-marshals them alphabetically, so `{"id":2,"apple":3}` (snapshot) diverged
// from `{"apple":3,"id":2}` (CDC) on the first CDC update after snapshot,
// defeating the byte-compare that replay/dedup relies on.
//
// Decoding with UseNumber and re-marshalling normalizes it: encoding/json sorts
// object keys (recursively) and json.Number carries each number's exact source
// text through unchanged, so precision and formatting survive. Returns ok=false
// for anything that is not a single well-formed JSON value, so a malformed source
// falls back to a plain string rather than an invalid payload (as before).
func canonicalJSON(text string) (json.RawMessage, bool) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if dec.More() {
		return nil, false // trailing content — not a single JSON value
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}
	return json.RawMessage(b), true
}

// CanonicalizeMySQLJSONNumbers rewrites float-syntax JSON numbers in raw to Go's
// canonical float form (float64), matching what the MySQL binlog (CDC) path
// already produces — go-mysql decodes a JSON double to float64 and json.Marshals
// it. The MySQL SNAPSHOT path calls this so a whole double's "1.0"/"-0.0" text
// (MySQL's normalized JSON) agrees with CDC's "1"/"-0", closing the byte
// divergence dedup relies on. Pure-integer tokens (no '.'/'e'/'E') stay exact —
// the UseNumber contract that keeps >2^53 IDs/keys byte-for-byte.
//
// DELIBERATELY MySQL-snapshot-only, not in the shared canonicalJSON: Postgres
// renders jsonb identically on both its paths (no divergence to fix), and jsonb
// preserves arbitrary-precision numerics — float64-normalizing there would
// silently round a high-precision decimal, a corruption with no upside. Returns
// raw unchanged on a parse error.
func CanonicalizeMySQLJSONNumbers(raw []byte) []byte {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return raw
	}
	b, err := json.Marshal(normalizeJSONNumbers(v))
	if err != nil {
		return raw
	}
	return b
}

func normalizeJSONNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = normalizeJSONNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = normalizeJSONNumbers(e)
		}
		return x
	case json.Number:
		s := x.String()
		if !strings.ContainsAny(s, ".eE") {
			return x // integer syntax — keep exact (big-int safe)
		}
		f, err := x.Float64()
		if err != nil {
			return x // not representable as float64 — leave the token as-is
		}
		return f
	default:
		return v
	}
}

// BuildEntityJSON maps a decoded source row into the topic payload, keyed by each
// mapping's JsonName. Both the value and category maps are keyed by LOWERCASED
// column name — every decode path (snapshot and CDC, both dialects) lowercases
// column names when it builds them. So the mapping's configured SQL column is
// lowercased here for the lookup; without it a mixed-case config (column =
// "CreatedAt") misses the "createdat" map entry and silently emits a null field.
// Shared by all four decode sites so the case handling can't drift between them.
func BuildEntityJSON(mappings []Mapping, values map[string]any, cats map[string]JSONCategory) map[string]any {
	out := make(map[string]any, len(mappings))
	for _, m := range mappings {
		col := strings.ToLower(m.SQLColumn)
		out[m.JsonName] = JSONValue(values[col], cats[col])
	}
	return out
}
