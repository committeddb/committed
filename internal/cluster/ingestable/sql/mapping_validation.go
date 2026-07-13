package sql

import (
	"fmt"
	"strings"
)

// validateMappingColumns checks every mapping against the live source schema so a
// broken column reference fails loudly at POST instead of silently emitting null
// on every row (the deeper cause behind the mixed-case-column bug: nothing
// verified that a mapping actually resolves). colsByTable is the introspected
// column set per watched table (Dialect.SourceColumns).
//
// committed decodes column names case-insensitively — it lowercases them when it
// builds each row's decode map — so:
//
//   - A mapping whose column resolves case-insensitively to a source column is
//     fine, including a mixed-case config against a quoted CamelCase column.
//   - A mapping whose column resolves to nothing — a typo, or a column renamed or
//     dropped in the source since the config was written — is rejected. That case
//     otherwise reads back the zero value and emits a JSON null on every row.
//   - A source table with two columns differing only by case is rejected: the
//     lowercased decode map cannot distinguish them, so one would silently shadow
//     the other. Legal only via quoted identifiers in Postgres; pathological.
func validateMappingColumns(config *Config, colsByTable map[string][]string) error {
	// A table whose columns collide when lowercased can't be decoded unambiguously.
	for _, table := range config.Tables {
		lowerToActual := make(map[string]string, len(colsByTable[table]))
		for _, col := range colsByTable[table] {
			lc := strings.ToLower(col)
			if prev, ok := lowerToActual[lc]; ok && prev != col {
				return fmt.Errorf(
					"table %q has columns %q and %q that differ only by case; committed decodes column names case-insensitively and cannot distinguish them — rename one in the source",
					table, prev, col)
			}
			lowerToActual[lc] = col
		}
	}

	// Union of source columns (lowercased) across all watched tables. A mapping is
	// valid if it resolves in any watched table — a multi-table ingestable may map
	// a column that only some tables carry.
	sourceCols := make(map[string]bool)
	for _, table := range config.Tables {
		for _, col := range colsByTable[table] {
			sourceCols[strings.ToLower(col)] = true
		}
	}
	for _, m := range config.Mappings {
		if m.SQLColumn == "" {
			continue // malformed mappings are caught elsewhere; nothing to resolve here
		}
		if !sourceCols[strings.ToLower(m.SQLColumn)] {
			return fmt.Errorf(
				"mapping column %q not found in source table(s) %v — check the spelling and case, and that the column still exists; a mapping to a column the source lacks silently emits null on every row",
				m.SQLColumn, config.Tables)
		}
	}
	// Every primaryKey column must resolve too: a nonexistent PK column decodes to
	// nothing, so CompositeKey collapses every row onto the key "<nil>" (all rows
	// overwrite one another downstream, deletes tombstone the wrong key).
	//
	// Unlike a mapping, a PK column must resolve in EVERY watched table, not just
	// the union: a multi-table ingestable emits all tables' rows under ONE entity
	// key, so a PK column present in some tables but absent from another still
	// collapses that table's rows onto "<nil>". A column in NO table is a typo;
	// one in some-but-not-all is a coverage gap — distinct cases, each with its
	// own actionable message.
	for _, pk := range config.PrimaryKey {
		if pk == "" {
			continue
		}
		lc := strings.ToLower(pk)
		if !sourceCols[lc] {
			return fmt.Errorf(
				"primaryKey column %q not found in source table(s) %v — check the spelling and case; a nonexistent PK column collapses every row onto a single entity key",
				pk, config.Tables)
		}
		for _, table := range config.Tables {
			if !columnInTableFold(colsByTable[table], lc) {
				return fmt.Errorf(
					"primaryKey column %q is missing from watched table %q (it exists in another watched table, but a PK column must exist in EVERY table — otherwise that table's rows collapse onto the entity key \"<nil>\")",
					pk, table)
			}
		}
	}
	return nil
}

// columnInTableFold reports whether cols contains a column that equals lowerName
// when lowercased. lowerName must already be lowercase; committed decodes column
// names case-insensitively, so the match is case-folded.
func columnInTableFold(cols []string, lowerName string) bool {
	for _, c := range cols {
		if strings.ToLower(c) == lowerName {
			return true
		}
	}
	return false
}
