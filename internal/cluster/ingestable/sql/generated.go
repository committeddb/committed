package sql

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// committed cannot faithfully replicate a GENERATED / computed source column: the
// source's change stream (MySQL binlog / Postgres logical replication) omits its
// value, so it would be present on the initial snapshot but null on every later
// CDC row — a silent snapshot-vs-CDC divergence. So a generated column is refused
// if EXPLICITLY mapped (rejectGeneratedColumnRefs) and skipped under
// MapAllColumns (excludeGeneratedFromMapAll), both at POST.

// rejectGeneratedColumnRefs fails a config that explicitly maps, or keys on, a
// generated column. generatedByTable is the generated-column set per watched
// table (from Dialect.SourceColumns); a column generated in ANY watched table is
// treated as generated (mirroring how mapping resolution unions the tables).
func rejectGeneratedColumnRefs(config *Config, generatedByTable map[string][]string) error {
	generated := make(map[string]bool)
	for _, cols := range generatedByTable {
		for _, c := range cols {
			generated[strings.ToLower(c)] = true
		}
	}
	if len(generated) == 0 {
		return nil
	}
	for _, m := range config.Mappings {
		if m.SQLColumn != "" && generated[strings.ToLower(m.SQLColumn)] {
			return fmt.Errorf(
				"mapping column %q is a generated/computed column, which committed cannot replicate: the source's change stream omits it, so it would be present on the initial snapshot but null on every later change — remove it from the mapping",
				m.SQLColumn)
		}
	}
	for _, pk := range config.PrimaryKey {
		if pk != "" && generated[strings.ToLower(pk)] {
			return fmt.Errorf(
				"primaryKey column %q is a generated/computed column, which committed cannot replicate (the change stream omits it) — a generated column cannot be an ingest primary key",
				pk)
		}
	}
	return nil
}

// excludeGeneratedFromMapAll returns colsByTable with generated columns removed,
// so MapAllColumns mirrors only the columns committed can faithfully replicate,
// and logs each excluded column so it is not silently dropped.
func excludeGeneratedFromMapAll(colsByTable, generatedByTable map[string][]string) map[string][]string {
	out := make(map[string][]string, len(colsByTable))
	for table, cols := range colsByTable {
		gen := make(map[string]bool, len(generatedByTable[table]))
		for _, g := range generatedByTable[table] {
			gen[strings.ToLower(g)] = true
		}
		kept := make([]string, 0, len(cols))
		for _, c := range cols {
			if gen[strings.ToLower(c)] {
				zap.L().Warn("mapAllColumns: excluding generated column — committed cannot replicate it (present on the snapshot, omitted from the change stream); map it explicitly only if you accept null on every change",
					zap.String("table", table), zap.String("column", c))
				continue
			}
			kept = append(kept, c)
		}
		out[table] = kept
	}
	return out
}
