package sql

import (
	"encoding/json"
)

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
