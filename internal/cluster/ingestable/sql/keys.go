package sql

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// compositeBinaryTag prefixes the byte-preserving encoding CompositeKey falls
// back to when a value contains non-UTF-8 bytes. A JSON array (the text/number
// form) always starts with '[', so this tag can never collide with it — which is
// how DecodeCompositeCursor tells the two encodings apart.
const compositeBinaryTag = "b64:"

// CompositeKey builds an entity key from a row's primary-key column values.
//
// A single-column key is the bare value — unchanged from the original
// single-PK behavior, so existing entity keys stay byte-stable. A multi-column
// key is a JSON array of the values: deterministic and collision-free across
// any column contents (JSON quoting disambiguates, so "a"+"b" can never alias
// "ab" the way a delimiter join would). m holds stringified column values (both
// dialects stringify before keying), so the []string marshal never errors.
//
// The one exception is a value with non-UTF-8 bytes (a BINARY/VARBINARY/BLOB
// column): json.Marshal replaces every invalid byte with U+FFFD, so two distinct
// binary tuples would collapse onto one entity key (silent row loss) and the
// value wouldn't round-trip for the resume cursor. When any value isn't valid
// UTF-8, CompositeKey encodes each value byte-preservingly (base64, whose
// alphabet excludes '.') joined by '.', behind compositeBinaryTag. All-UTF-8
// keys keep the exact JSON-array bytes, so existing text/number entity keys
// don't change; both dialects run identical values through here, so snapshot
// and CDC agree either way.
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

// DecodeCompositeCursor reverses CompositeKey for keyset-pagination resume,
// returning the per-column boundary values to bind into a row-value comparison
// ((c1, c2) > ($1, $2)). A single-column cursor is the bare value; a multi-column
// cursor is either the JSON array or, when a value held non-UTF-8 bytes, the
// compositeBinaryTag form. n is the number of PK columns.
func DecodeCompositeCursor(cursor string, n int) ([]string, error) {
	if n == 1 {
		return []string{cursor}, nil
	}
	if rest, ok := strings.CutPrefix(cursor, compositeBinaryTag); ok {
		parts := strings.Split(rest, ".")
		if len(parts) != n {
			return nil, fmt.Errorf("binary composite cursor has %d values, want %d", len(parts), n)
		}
		vals := make([]string, n)
		for i, p := range parts {
			b, err := base64.StdEncoding.DecodeString(p)
			if err != nil {
				return nil, fmt.Errorf("decode binary composite cursor: %w", err)
			}
			vals[i] = string(b)
		}
		return vals, nil
	}
	var vals []string
	if err := json.Unmarshal([]byte(cursor), &vals); err != nil {
		return nil, fmt.Errorf("decode composite cursor %q: %w", cursor, err)
	}
	if len(vals) != n {
		return nil, fmt.Errorf("composite cursor %q has %d values, want %d", cursor, len(vals), n)
	}
	return vals, nil
}
