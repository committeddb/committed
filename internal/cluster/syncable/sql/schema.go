package sql

import (
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

// schemaComparable is a config's materialized destination schema + identity, built
// config-alone by SchemaFromConfig (no database resolution). It implements
// cluster.SyncableSchemaComparable so the config-change guard can compare a
// re-POST's prior and next shapes without building either syncable.
type schemaComparable struct {
	schema   SyncableSchema
	identity SyncableIdentity
}

// SchemaChange implements cluster.SyncableSchemaComparable. It rejects, in order,
// an identity change (a topic/table re-point — the inherited SyncableIndex
// checkpoint is stale for the new destination → data loss) then a materialized
// schema change (CREATE TABLE IF NOT EXISTS never ALTERs, so the change would
// silently no-op). Identity is checked first because a table rename makes a fresh
// table (not a schema change) but IS a stale-checkpoint hazard. A prior of a
// different, incomparable kind returns nil (fail open). The returned errors
// implement cluster.RebuildRequiredError.
func (s *schemaComparable) SchemaChange(prior cluster.SyncableSchemaComparable) error {
	p, ok := prior.(*schemaComparable)
	if !ok {
		return nil
	}
	if change := identityChange(p.identity, s.identity); change != nil {
		return change
	}
	if change := materializedSchemaChange(p.schema, s.schema); change != nil {
		return change
	}
	return nil
}

// SyncableSchema is a comparable description of the destination table a SQL
// syncable materializes: the table name, its columns (in declared order) with
// their SQL types, the primary key, and any indexes. It is exactly the shape
// the dialect's CreateDDL consumes.
//
// Two configs with an equal SyncableSchema produce the same table via
// CREATE TABLE IF NOT EXISTS. A difference matters because the table is created
// with CREATE TABLE IF NOT EXISTS and never ALTERed: re-POSTing a config whose
// schema changed would persist the new config but leave the live table
// untouched — a silent no-op. This whole model is SQL-specific and lives in the
// sql package; the generic layers see only cluster.SyncableSchemaComparable and
// cluster.RebuildRequiredError.
type SyncableSchema struct {
	Table      string
	Columns    []SchemaColumn
	PrimaryKey string
	Indexes    []SchemaIndex
	// ProjectionShape is a canonical, order-independent fingerprint of the parts
	// of a projection's destination shape that Columns does NOT capture: each
	// aggregate column's element fields / elementKey / elementKeyType, and each
	// lookup dimension's fields. A change to any of these needs a rebuild
	// (CREATE TABLE IF NOT EXISTS never re-materializes the aggregate/lookup
	// shape). Empty for a plain syncable and for a projection with no
	// aggregate/lookup source, so it never affects those.
	ProjectionShape string
}

// SchemaColumn is one column of a materialized table.
type SchemaColumn struct {
	Name string
	Type string
}

// SchemaIndex is one index of a materialized table. Columns is the dialect's
// comma-separated column list, compared verbatim.
type SchemaIndex struct {
	Name    string
	Columns string
}

// schemaChangeCode is the machine-readable code a deploy pipeline branches on
// to call the rebuild verb without scraping the message or the SQLSTATE. It is
// surfaced generically via cluster.RebuildRequiredError.Code.
const schemaChangeCode = "schema_change_requires_rebuild"

// SchemaChangeError reports that a re-POSTed config would change the
// materialized schema of an existing table. Because the table is created with
// CREATE TABLE IF NOT EXISTS and never ALTERed, persisting the new config would
// not change the live table — the change is a silent no-op. It implements
// cluster.RebuildRequiredError so the HTTP layer can render it (409 + code +
// structured details) without importing the sql package or knowing about
// tables and columns.
type SchemaChangeError struct {
	Table             string   `json:"table"`
	AddedColumns      []string `json:"addedColumns,omitempty"`
	RemovedColumns    []string `json:"removedColumns,omitempty"`
	ChangedColumns    []string `json:"changedColumns,omitempty"`
	PrimaryKeyChanged bool     `json:"primaryKeyChanged,omitempty"`
	IndexesChanged    bool     `json:"indexesChanged,omitempty"`
	// ProjectionShapeChanged is set when a projection's aggregate/lookup shape
	// (element fields, elementKey, elementKeyType, lookup fields) changed — the
	// part of the destination CREATE TABLE IF NOT EXISTS never re-materializes.
	ProjectionShapeChanged bool `json:"projectionShapeChanged,omitempty"`
}

func (e *SchemaChangeError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "syncable schema change for table %q will not be applied: committed creates the table with CREATE TABLE IF NOT EXISTS and never ALTERs it, so re-POSTing this config is a no-op. Rebuild the table in place with POST /v1/syncable/{id}/rebuild.", e.Table)
	if len(e.AddedColumns) > 0 {
		fmt.Fprintf(&b, " added columns: %s.", strings.Join(e.AddedColumns, ", "))
	}
	if len(e.RemovedColumns) > 0 {
		fmt.Fprintf(&b, " removed columns: %s.", strings.Join(e.RemovedColumns, ", "))
	}
	if len(e.ChangedColumns) > 0 {
		fmt.Fprintf(&b, " changed columns: %s.", strings.Join(e.ChangedColumns, ", "))
	}
	if e.PrimaryKeyChanged {
		b.WriteString(" primary key changed.")
	}
	if e.IndexesChanged {
		b.WriteString(" indexes changed.")
	}
	if e.ProjectionShapeChanged {
		b.WriteString(" projection aggregate/lookup shape changed (element fields, elementKey, elementKeyType, or lookup fields).")
	}
	return b.String()
}

// Code implements cluster.RebuildRequiredError.
func (e *SchemaChangeError) Code() string { return schemaChangeCode }

// Details implements cluster.RebuildRequiredError: the exported fields are the
// machine-readable payload (json tags above) the HTTP layer puts in the 409
// `details`.
func (e *SchemaChangeError) Details() any { return e }

// schemaOf converts a DDL Config into the schema descriptor used by the
// propose-time silent-no-op guard. It mirrors exactly what CreateDDL consumes —
// table, ordered columns with their SQL types, primary key, and indexes — so
// two configs compare equal iff they would produce the same CREATE TABLE.
func schemaOf(c *Config) SyncableSchema {
	cols := make([]SchemaColumn, 0, len(c.Mappings))
	for _, m := range c.Mappings {
		cols = append(cols, SchemaColumn{Name: m.Column, Type: m.SQLType})
	}
	idx := make([]SchemaIndex, 0, len(c.Indexes))
	for _, i := range c.Indexes {
		idx = append(idx, SchemaIndex{Name: i.IndexName, Columns: i.ColumnNames})
	}
	return SyncableSchema{
		Table:      c.Table,
		Columns:    cols,
		PrimaryKey: c.PrimaryKey,
		Indexes:    idx,
	}
}

// materializedSchemaChange compares the previously-persisted schema (old)
// against the incoming one (next) and returns a *SchemaChangeError describing
// the difference if the materialized table shape would change, or nil if the
// two produce the same table.
//
// Scope: only the same-table case is a silent no-op (CREATE TABLE IF NOT EXISTS
// sees the existing table and changes nothing). A different table name is not a
// schema change but an IDENTITY change — the inherited checkpoint is stale for
// the new table — and is caught earlier by identityChange (see
// schemaComparable.SchemaChange), so this function stays same-table-scoped and
// returns nil for a rename.
//
// Column names are compared exactly; SQL types are normalized (upper-cased and
// trimmed) so a cosmetic "varchar(128)" → "VARCHAR(128)" edit is not treated as
// a destructive change.
func materializedSchemaChange(old, next SyncableSchema) *SchemaChangeError {
	if old.Table != next.Table {
		return nil
	}

	oldCols := columnTypeMap(old.Columns)
	nextCols := columnTypeMap(next.Columns)

	var added, removed, changed []string
	for _, c := range next.Columns {
		if _, ok := oldCols[c.Name]; !ok {
			added = append(added, c.Name)
		}
	}
	for _, c := range old.Columns {
		nextType, ok := nextCols[c.Name]
		if !ok {
			removed = append(removed, c.Name)
			continue
		}
		if normalizeType(c.Type) != normalizeType(nextType) {
			changed = append(changed, fmt.Sprintf("%s (%s -> %s)", c.Name, c.Type, nextType))
		}
	}

	pkChanged := old.PrimaryKey != next.PrimaryKey
	indexesChanged := !indexesEqual(old.Indexes, next.Indexes)
	shapeChanged := old.ProjectionShape != next.ProjectionShape

	if len(added) == 0 && len(removed) == 0 && len(changed) == 0 && !pkChanged && !indexesChanged && !shapeChanged {
		return nil
	}

	return &SchemaChangeError{
		Table:                  next.Table,
		AddedColumns:           added,
		RemovedColumns:         removed,
		ChangedColumns:         changed,
		PrimaryKeyChanged:      pkChanged,
		IndexesChanged:         indexesChanged,
		ProjectionShapeChanged: shapeChanged,
	}
}

func columnTypeMap(cols []SchemaColumn) map[string]string {
	m := make(map[string]string, len(cols))
	for _, c := range cols {
		m[c.Name] = c.Type
	}
	return m
}

func normalizeType(t string) string {
	return strings.ToUpper(strings.TrimSpace(t))
}

func indexesEqual(a, b []SchemaIndex) bool {
	if len(a) != len(b) {
		return false
	}
	am := make(map[string]string, len(a))
	for _, idx := range a {
		am[idx.Name] = idx.Columns
	}
	for _, idx := range b {
		cols, ok := am[idx.Name]
		if !ok || cols != idx.Columns {
			return false
		}
	}
	return true
}
