package sql

import (
	"encoding/json"
	"fmt"
)

// CompositeKey builds an entity key from a row's primary-key column values.
//
// A single-column key is the bare value — unchanged from the original
// single-PK behavior, so existing entity keys stay byte-stable. A multi-column
// key is a JSON array of the values: deterministic and collision-free across
// any column contents (JSON quoting disambiguates, so "a"+"b" can never alias
// "ab" the way a delimiter join would). m holds stringified column values (both
// dialects stringify before keying), so the []string marshal never errors.
func CompositeKey(m map[string]any, cols []string) string {
	if len(cols) == 1 {
		return fmt.Sprintf("%v", m[cols[0]])
	}
	vals := make([]string, len(cols))
	for i, c := range cols {
		vals[i] = fmt.Sprintf("%v", m[c])
	}
	b, _ := json.Marshal(vals)
	return string(b)
}

// DecodeCompositeCursor reverses CompositeKey for keyset-pagination resume,
// returning the per-column boundary values to bind into a row-value comparison
// ((c1, c2) > ($1, $2)). A single-column cursor is the bare value; a
// multi-column cursor is the JSON array. n is the number of PK columns.
func DecodeCompositeCursor(cursor string, n int) ([]string, error) {
	if n == 1 {
		return []string{cursor}, nil
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
