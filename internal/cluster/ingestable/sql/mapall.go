package sql

import "fmt"

// expandMapAllColumns turns a MapAllColumns config into an explicit Mappings
// list, frozen against the live source schema. colsByTable is the introspected
// column set per watched table (Dialect.SourceColumns). The rules:
//
//   - Every source column becomes a jsonName=column mapping, in source order,
//     unioned across Tables (first-seen wins, so a column shared by two tables
//     maps once).
//   - An explicit Mapping the user listed OVERRIDES the inferred one for that
//     column (the rename case) — its position in the output follows source
//     order, not config order, so the shape is deterministic.
//   - ExcludeColumns are dropped from the set.
//
// It fails the build (so a bad config degrades loudly at POST) when an override
// or an excludeColumns entry names a column the source does not have — a typo
// there silently does nothing, and a typo'd exclude would leak the very secret
// it was meant to drop — or when a column is both excluded and explicitly
// mapped, or when the result is empty.
func expandMapAllColumns(config *Config, colsByTable map[string][]string) error {
	overrides := make(map[string]Mapping, len(config.Mappings))
	for _, m := range config.Mappings {
		overrides[m.SQLColumn] = m
	}
	exclude := make(map[string]bool, len(config.ExcludeColumns))
	for _, c := range config.ExcludeColumns {
		exclude[c] = true
	}

	seen := make(map[string]bool)
	var mappings []Mapping
	for _, table := range config.Tables {
		for _, col := range colsByTable[table] {
			if seen[col] {
				continue
			}
			seen[col] = true

			if exclude[col] {
				if _, isOverride := overrides[col]; isOverride {
					return fmt.Errorf("column %q is both excluded and explicitly mapped", col)
				}
				continue
			}
			if ov, ok := overrides[col]; ok {
				mappings = append(mappings, ov)
			} else {
				mappings = append(mappings, Mapping{JsonName: col, SQLColumn: col})
			}
		}
	}

	for col := range overrides {
		if !seen[col] {
			return fmt.Errorf("mapping references column %q not found in the source table(s)", col)
		}
	}
	for col := range exclude {
		if !seen[col] {
			return fmt.Errorf("excludeColumns references column %q not found in the source table(s)", col)
		}
	}

	if len(mappings) == 0 {
		return fmt.Errorf("mapAllColumns produced no columns to map (every source column excluded?)")
	}

	config.Mappings = mappings
	return nil
}
