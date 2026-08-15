package cluster

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// This file owns the composite entity-key ENCODING — the producer↔consumer
// contract for how a multi-column source identity collapses into the one
// opaque Entity.Key and how a consumer (a syncable honoring a payload-less
// delete tombstone, an ingest resume cursor) unpacks it again. It moved here
// from ingestable/sql when the syncable side became its second consumer: the
// encoding was never an ingest implementation detail, and a syncable
// importing an ingest package would invert the architecture. The encoding is
// deliberately deterministic and decodable; changing it re-keys every
// existing composite entity, so treat it as frozen.
//
// ORDER IS PART OF THE CONTRACT: values marshal in the producer's configured
// primaryKey column order, and every decoder assigns them back positionally
// from ITS configured order. The two configs are decoupled on purpose
// (topics), so nothing can verify they agree — a consumer whose column list
// differs in order from the producer's mis-addresses rows. Both config docs
// state this.

// compositeBinaryTag prefixes the byte-preserving encoding CompositeKey falls
// back to when a value contains non-UTF-8 bytes. A JSON array (the
// text/number form) always starts with '[', so this tag can never collide
// with it — which is how DecodeCompositeKey tells the two encodings apart.
const compositeBinaryTag = "b64:"

// CompositeKey builds an entity key from a row's primary-key column values.
//
// A single-column key is the bare value — unchanged from the original
// single-PK behavior, so existing entity keys stay byte-stable. A multi-column
// key is a JSON array of the values: deterministic and collision-free across
// any column contents (JSON quoting disambiguates, so "a"+"b" can never alias
// "ab" the way a delimiter join would). m holds stringified column values
// (every producer stringifies before keying), so the []string marshal never
// errors.
//
// The one exception is a value with non-UTF-8 bytes (a BINARY/VARBINARY/BLOB
// column): json.Marshal replaces every invalid byte with U+FFFD, so two
// distinct binary tuples would collapse onto one entity key (silent row loss)
// and the value wouldn't round-trip for the resume cursor. When any value
// isn't valid UTF-8, CompositeKey encodes each value byte-preservingly
// (base64, whose alphabet excludes '.') joined by '.', behind
// compositeBinaryTag. All-UTF-8 keys keep the exact JSON-array bytes, so
// existing text/number entity keys don't change; every producer runs
// identical values through here, so snapshot and CDC agree either way.
func CompositeKey(m map[string]any, cols []string) string {
	// m is keyed by LOWERCASED column names (every decode path lowercases), so a
	// PK column configured in any other case (primaryKey = "ID" vs column id) must
	// be lowercased for the lookup — otherwise it misses and every row keys to
	// "<nil>", collapsing all rows onto one entity key.
	if len(cols) == 1 {
		return fmt.Sprintf("%v", m[strings.ToLower(cols[0])])
	}
	vals := make([]string, len(cols))
	allValidUTF8 := true
	for i, c := range cols {
		vals[i] = fmt.Sprintf("%v", m[strings.ToLower(c)])
		if !utf8.ValidString(vals[i]) {
			allValidUTF8 = false
		}
	}
	if allValidUTF8 {
		b, _ := json.Marshal(vals)
		return string(b)
	}
	enc := make([]string, len(vals))
	for i, v := range vals {
		enc[i] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	return compositeBinaryTag + strings.Join(enc, ".")
}

// IsCompositeEncoded reports whether key is shaped like a MULTI-column
// CompositeKey encoding (a JSON string-array of two or more values, or the
// binary-tagged form) — the loud-guard predicate for a single-key consumer
// receiving a composite producer's tombstone, where binding the encoding as
// a bare key would silently DELETE nothing. A legitimate bare value that
// happens to be a JSON string-array is indistinguishable by construction
// (rare false positive); the guard trades that visible, replayable
// dead-letter for never silently no-oping an erasure.
func IsCompositeEncoded(key string) bool {
	if strings.HasPrefix(key, compositeBinaryTag) {
		return true
	}
	if !strings.HasPrefix(key, "[") {
		return false
	}
	var vals []string
	return json.Unmarshal([]byte(key), &vals) == nil && len(vals) > 1
}

// DecodeCompositeKey reverses CompositeKey, returning the per-column values in
// the producer's column order. A single-column key (n == 1) is the bare value;
// a multi-column key is either the JSON array or, when a value held non-UTF-8
// bytes, the compositeBinaryTag form. n is the number of primary-key columns
// the CONSUMER is configured with — an arity mismatch against the encoded key
// is reported (the producer keyed by a different column count).
//
// PII note: the key IS the row's primary key. Errors report only the parse
// reason and arity numbers, never the key value — they bubble into logs and
// (on the syncable path) replicated dead-letter records.
func DecodeCompositeKey(key string, n int) ([]string, error) {
	if n == 1 {
		return []string{key}, nil
	}
	if rest, ok := strings.CutPrefix(key, compositeBinaryTag); ok {
		parts := strings.Split(rest, ".")
		if len(parts) != n {
			return nil, fmt.Errorf("binary composite key has %d values, want %d", len(parts), n)
		}
		vals := make([]string, n)
		for i, p := range parts {
			b, err := base64.StdEncoding.DecodeString(p)
			if err != nil {
				return nil, fmt.Errorf("decode binary composite key: %w", err)
			}
			vals[i] = string(b)
		}
		return vals, nil
	}
	var vals []string
	if err := json.Unmarshal([]byte(key), &vals); err != nil {
		return nil, fmt.Errorf("decode composite key: %w", err)
	}
	if len(vals) != n {
		return nil, fmt.Errorf("composite key has %d values, want %d", len(vals), n)
	}
	return vals, nil
}
