package sql

import (
	"github.com/committeddb/committed/internal/cluster"
)

// The composite entity-key encoding moved to the cluster package
// (composite_key.go) when the syncable side became its second consumer — it
// is the producer↔consumer contract of the entity model, not an ingest
// implementation detail. These delegations keep the ingest dialects' call
// sites stable; new code should call the cluster functions directly.

// CompositeKey builds an entity key from a row's primary-key column values.
// See cluster.CompositeKey for the encoding contract (bare single value,
// JSON-array composite, b64 fallback for non-UTF-8 bytes; order is part of
// the contract).
func CompositeKey(m map[string]any, cols []string) string {
	return cluster.CompositeKey(m, cols)
}

// DecodeCompositeCursor reverses CompositeKey for keyset-pagination resume,
// returning the per-column boundary values to bind into a row-value
// comparison ((c1, c2) > ($1, $2)). See cluster.DecodeCompositeKey.
func DecodeCompositeCursor(cursor string, n int) ([]string, error) {
	return cluster.DecodeCompositeKey(cursor, n)
}
