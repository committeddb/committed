package sql

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// JSONCategory is how a source column's value should be rendered in the
// ingested JSON payload. A dialect maps its own type metadata — PostgreSQL type
// OIDs (CDC) or database type names (snapshot), MySQL column types — onto these,
// and JSONValue does the rendering, so both ingest paths and both dialects agree
// on the shape.
type JSONCategory int

const (
	CatText   JSONCategory = iota // string — the default: text, dates, uuid, …
	CatNumber                     // JSON number — int, float, numeric/decimal
	CatBool                       // JSON bool
	CatJSON                       // embedded JSON — json/jsonb columns
	CatBinary                     // base64 string — binary columns (bytea, BLOB, VARBINARY)
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
	if raw == nil {
		return nil
	}
	// Binary is handled before the []byte→string coercion below, because it needs
	// the value's original form: MySQL hands a binary column back as raw []byte,
	// while Postgres (which casts every column ::text) hands it back as its
	// "\xDEADBEEF" hex text. Both become the same base64 string.
	if cat == CatBinary {
		return binaryToBase64(raw)
	}

	var text string
	switch x := raw.(type) {
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

// binaryToBase64 renders a binary column value as a base64 JSON string — the one
// encoding for bytes shared across dialects, matching how the entity key already
// encodes binary (see CompositeKey). The value must already be the raw bytes:
// MySQL returns BLOB/BINARY/VARBINARY as []byte, and Postgres decodes its
// "\xDEADBEEF" bytea text to bytes with DecodeByteaText before it reaches here —
// so this never has to sniff hex-text vs raw bytes (which are indistinguishable
// once both are []byte). A nil is handled by the caller.
func binaryToBase64(raw any) any {
	switch v := raw.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v))
	default:
		return raw
	}
}

// DecodeByteaText decodes Postgres's hex bytea text ("\xDEADBEEF") to raw bytes,
// so the shared base64 binary rendering (binaryToBase64 / CatBinary) sees bytes,
// not the hex text. Postgres casts every column ::text and pgoutput emits text,
// so a bytea arrives in this form on BOTH the snapshot and CDC paths; the dialect
// runs its CatBinary payload values through here first. A value not in "\x…" form
// is returned unchanged.
func DecodeByteaText(v any) any {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		return v
	}
	rest, ok := strings.CutPrefix(s, `\x`)
	if !ok {
		return v
	}
	b, err := hex.DecodeString(rest)
	if err != nil {
		return v
	}
	return b
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

// mysqlJSONPathKey renders a JSON object key as a MySQL JSON path step, always
// double-quoting so an arbitrary user key (spaces, dots, unicode, digits) is a
// valid path member. MySQL accepts a quoted key everywhere an unquoted one is
// allowed, so quoting unconditionally keeps the path builder total; the escapes
// match MySQL's path-string grammar (backslash and double-quote).
func mysqlJSONPathKey(k string) string {
	return `."` + jsonPathKeyEscaper.Replace(k) + `"`
}

var jsonPathKeyEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

// JSONNumberPaths returns the MySQL JSON paths (e.g. `$."a"[0]`) of every
// float-syntax number leaf in a MySQL-rendered JSON document — the leaves whose
// DECIMAL-vs-DOUBLE type the snapshot must resolve before it can render them the
// same way CDC does (CDC reads the type off the binlog; the snapshot only has
// this text, where a DECIMAL 1.5 and a DOUBLE 1.5 are identical). Integer leaves
// are unambiguous and omitted, so a doc with no fractional numbers needs no type
// query at all. Returns nil on a parse error (caller falls back).
func JSONNumberPaths(raw []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil
	}
	var out []string
	var walk func(any, string)
	walk = func(v any, path string) {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				walk(e, path+mysqlJSONPathKey(k))
			}
		case []any:
			for i, e := range x {
				walk(e, path+"["+strconv.Itoa(i)+"]")
			}
		case json.Number:
			if strings.ContainsAny(x.String(), ".eE") {
				out = append(out, path)
			}
		}
	}
	walk(v, "$")
	return out
}

// RenderMySQLJSONByType canonicalizes a MySQL-rendered JSON document using a
// per-path type map (MySQL JSON path -> the column's JSON_TYPE, "DECIMAL" /
// "DOUBLE" / …). A DOUBLE leaf is normalized through float64 — matching what the
// CDC path emits for the same value, and preserving the existing "100.0"->"100"
// double canonicalization. A DECIMAL leaf (and any leaf absent from types, and
// every integer) keeps its exact token, so a high-precision or >2^53 decimal is
// no longer silently rounded. Returns raw unchanged on a parse error, mirroring
// CanonicalizeMySQLJSONNumbers; the path builder matches JSONNumberPaths exactly
// so the map lookups line up.
func RenderMySQLJSONByType(raw []byte, types map[string]string) []byte {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return raw
	}
	var conv func(any, string) any
	conv = func(v any, path string) any {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				x[k] = conv(e, path+mysqlJSONPathKey(k))
			}
			return x
		case []any:
			for i, e := range x {
				x[i] = conv(e, path+"["+strconv.Itoa(i)+"]")
			}
			return x
		case json.Number:
			if types[path] == "DOUBLE" && strings.ContainsAny(x.String(), ".eE") {
				if f, err := x.Float64(); err == nil {
					return f
				}
			}
			return x // DECIMAL / unresolved / integer — keep exact
		default:
			return v
		}
	}
	b, err := json.Marshal(conv(v, "$"))
	if err != nil {
		return raw
	}
	return b
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
