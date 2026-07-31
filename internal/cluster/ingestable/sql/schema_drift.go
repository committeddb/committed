package sql

import (
	"errors"
	"fmt"
	"strings"
)

// Runtime schema-drift reconciliation.
//
// committed validates a config's column contract against the source schema once,
// at admission (validateMappingColumns). The source schema can then evolve under a
// live ingest — a column renamed or dropped mid-stream — and nothing re-checks it.
// ReconcileSchema is the runtime counterpart: at each dialect's schema-observation
// point (MySQL's per-row TableMapEvent, Postgres's RelationMessage) it compares the
// contract against the freshly-observed columns and classifies the drift by its
// effect on the keyed-view promise. There are two tiers (a third, benign, needs no
// handling):
//
//   - CORRUPTION — a primaryKey column is gone. CompositeKey would then key every
//     row on "<nil>" and collapse distinct rows onto one entity; a keyed sink keeps
//     one row and mis-tombstones deletes. The promise is broken, so the worker must
//     PARK (ParkError wraps ErrPrimaryKeyColumnMissing; each dialect's reconnect
//     loop returns it to freeze the worker rather than busy-retry).
//
//   - DIVERGENCE — a mapped non-key column is gone. That field renders null from
//     here on, but every row stays correctly keyed and distinct, so the promise is
//     degraded, not broken. The worker keeps going and WARNs that the sink now
//     diverges and must be re-snapshotted to reconcile — the same policy handleDDL
//     applies to a TRUNCATE on a watched table.
//
//   - BENIGN (no handling) — an added or unmapped column (ignored), or a column
//     type change (each row decodes against its own write-time schema, so no
//     divergence). Not reported.

// ErrPrimaryKeyColumnMissing marks the CORRUPTION tier: a configured primaryKey
// column is absent from the source table's current schema (renamed or dropped
// after the ingestable was created). Each dialect's stream loop matches it with
// errors.Is and returns it to PARK the worker (freeze → supervisor → parked,
// observable via committed.worker.parked) instead of collapsing every row onto one
// key. The POST-time counterpart is validateMappingColumns.
var ErrPrimaryKeyColumnMissing = errors.New("ingest: a configured primaryKey column is missing from the source table's current schema")

// SchemaDrift is the classified result of reconciling a config's column contract
// against a freshly-observed source schema. A column that is both keyed and mapped
// is reported only in MissingKey — corruption dominates divergence.
type SchemaDrift struct {
	// MissingKey are configured primaryKey columns absent from the source schema
	// (CORRUPTION → park). Original config casing, for the operator-facing message.
	MissingKey []string
	// MissingMapped are mapped non-key columns absent from the source schema
	// (DIVERGENCE → warn). Original config casing.
	MissingMapped []string
}

// ReconcileSchema classifies the drift between a topic-spec's column contract
// (primaryKey ∪ mapped columns) and observed — the set of lowercased column names
// the live decode schema carries. It is per-spec: each watched table's rows are
// reconciled against the spec that routes it (the flat config is one spec). It is
// pure; the caller decides the response from the tiers (ParkError for corruption,
// a deduped warn for divergence).
func ReconcileSchema(spec *TopicSpec, observed map[string]bool) SchemaDrift {
	var d SchemaDrift
	key := make(map[string]bool, len(spec.PrimaryKey))
	for _, pk := range spec.PrimaryKey {
		if pk == "" {
			continue
		}
		key[strings.ToLower(pk)] = true
		if !observed[strings.ToLower(pk)] {
			d.MissingKey = append(d.MissingKey, pk)
		}
	}
	for _, m := range spec.Mappings {
		if m.SQLColumn == "" {
			continue
		}
		lc := strings.ToLower(m.SQLColumn)
		if key[lc] {
			continue // a keyed column is covered by the corruption tier above
		}
		if !observed[lc] {
			d.MissingMapped = append(d.MissingMapped, m.SQLColumn)
		}
	}
	return d
}

// ParkError returns a non-nil error wrapping ErrPrimaryKeyColumnMissing when the
// drift includes a missing primaryKey column — the caller must PARK the worker —
// or nil. It names the missing key columns for the operator.
func (d SchemaDrift) ParkError() error {
	if len(d.MissingKey) == 0 {
		return nil
	}
	return fmt.Errorf(
		"primaryKey column(s) %v missing from the source table's current schema (renamed or dropped after the ingestable was created); every CDC row would collapse onto one key — re-POST the ingestable with the current primary key, or restore the column: %w",
		d.MissingKey, ErrPrimaryKeyColumnMissing)
}
