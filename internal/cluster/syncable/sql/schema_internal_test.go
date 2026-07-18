package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func schema(table, pk string, cols ...[2]string) SyncableSchema {
	s := SyncableSchema{Table: table, PrimaryKey: pk}
	for _, c := range cols {
		s.Columns = append(s.Columns, SchemaColumn{Name: c[0], Type: c[1]})
	}
	return s
}

func TestMaterializedSchemaChange_Identical(t *testing.T) {
	s := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"name", "TEXT"})
	require.Nil(t, materializedSchemaChange(s, s))
}

func TestMaterializedSchemaChange_AddedColumn(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"})
	next := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.Equal(t, "tenants", change.Table)
	require.Equal(t, []string{"tier"}, change.AddedColumns)
	require.Empty(t, change.RemovedColumns)
	require.Empty(t, change.ChangedColumns)
	require.Contains(t, change.Error(), "rebuild")
	require.Contains(t, change.Error(), "tier")
}

func TestMaterializedSchemaChange_RemovedColumn(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})
	next := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.Equal(t, []string{"tier"}, change.RemovedColumns)
	require.Empty(t, change.AddedColumns)
}

func TestMaterializedSchemaChange_ChangedType(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"})
	next := schema("tenants", "id", [2]string{"id", "VARCHAR(256)"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.Len(t, change.ChangedColumns, 1)
	require.Contains(t, change.ChangedColumns[0], "id")
	require.Contains(t, change.ChangedColumns[0], "VARCHAR(128)")
	require.Contains(t, change.ChangedColumns[0], "VARCHAR(256)")
}

func TestMaterializedSchemaChange_TypeNormalization(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "varchar(128)"})
	next := schema("tenants", "id", [2]string{"id", "  VARCHAR(128) "})
	require.Nil(t, materializedSchemaChange(old, next))
}

func TestMaterializedSchemaChange_PrimaryKeyChanged(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"alt", "VARCHAR(128)"})
	next := schema("tenants", "alt", [2]string{"id", "VARCHAR(128)"}, [2]string{"alt", "VARCHAR(128)"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.True(t, change.PrimaryKeyChanged)
	require.Contains(t, change.Error(), "primary key")
}

func TestMaterializedSchemaChange_IndexesChanged(t *testing.T) {
	old := SyncableSchema{Table: "t", PrimaryKey: "id", Columns: []SchemaColumn{{Name: "id", Type: "INT"}}}
	next := old
	next.Indexes = []SchemaIndex{{Name: "idx_id", Columns: "id"}}

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.True(t, change.IndexesChanged)
}

func TestMaterializedSchemaChange_TableRenameOutOfScope(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"})
	next := schema("tenants_v2", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})
	require.Nil(t, materializedSchemaChange(old, next))
}

func TestMaterializedSchemaChange_ColumnReorderIsNoChange(t *testing.T) {
	old := schema("t", "id", [2]string{"id", "INT"}, [2]string{"a", "TEXT"}, [2]string{"b", "TEXT"})
	next := schema("t", "id", [2]string{"id", "INT"}, [2]string{"b", "TEXT"}, [2]string{"a", "TEXT"})
	require.Nil(t, materializedSchemaChange(old, next))
}

func TestMaterializedSchemaChange_ProjectionShapeChanged(t *testing.T) {
	// Identical table columns, but the aggregate/lookup shape differs — the case
	// the main-table schema alone (ddlConfig) misses.
	old := schema("bff", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"top_cast", "JSONB"})
	next := old
	old.ProjectionShape = "agg(col=top_cast,key=$.ordering,keyType=number,elem=name{from=$.name,lookup=,on=,select=})|"
	next.ProjectionShape = "agg(col=top_cast,key=$.ordering,keyType=number,elem=name{from=$.name,lookup=,on=,select=},role{from=$.category,lookup=,on=,select=})|"

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.True(t, change.ProjectionShapeChanged)
	require.Empty(t, change.AddedColumns) // the miss: no column-level diff
	require.Contains(t, change.Error(), "aggregate/lookup shape")
	require.Equal(t, "schema_change_requires_rebuild", change.Code())
}

// TestProjectionShapeFingerprint pins what counts as an aggregate/lookup shape
// change (rebuild-required) vs a benign reorder.
func TestProjectionShapeFingerprint(t *testing.T) {
	name := ProjectionElementField{Field: "name", From: "$.name"}
	role := ProjectionElementField{Field: "role", From: "$.category"}
	mk := func(keyType string, fields ...ProjectionElementField) *ProjectionConfig {
		return &ProjectionConfig{Sources: []ProjectionSource{{Aggregate: &ProjectionAggregate{
			Column: "top_cast", ElementKey: "$.ordering", ElementKeyType: keyType, Element: fields,
		}}}}
	}
	base := mk("number", name, role).projectionShapeFingerprint()

	require.Equal(t, base, mk("number", role, name).projectionShapeFingerprint(),
		"a field reorder is not a shape change")
	require.NotEqual(t, base, mk("number", name, role, ProjectionElementField{Field: "billing", From: "$.billing"}).projectionShapeFingerprint(),
		"adding an element field is a shape change")
	require.NotEqual(t, base, mk("text", name, role).projectionShapeFingerprint(),
		"flipping elementKeyType is a shape change")

	changedKey := mk("number", name, role)
	changedKey.Sources[0].Aggregate.ElementKey = "$.billing"
	require.NotEqual(t, base, changedKey.projectionShapeFingerprint(), "changing elementKey is a shape change")

	oneField := &ProjectionConfig{Sources: []ProjectionSource{{Lookup: &ProjectionLookup{
		Name: "names", Fields: []ProjectionElementField{{Field: "full", From: "$.full"}},
	}}}}
	twoField := &ProjectionConfig{Sources: []ProjectionSource{{Lookup: &ProjectionLookup{
		Name: "names", Fields: []ProjectionElementField{{Field: "full", From: "$.full"}, {Field: "born", From: "$.born"}},
	}}}}
	require.NotEqual(t, oneField.projectionShapeFingerprint(), twoField.projectionShapeFingerprint(),
		"adding a lookup field is a shape change")

	require.Equal(t, (&ProjectionConfig{}).projectionShapeFingerprint(),
		(&ProjectionConfig{Sources: []ProjectionSource{{Topic: "x"}}}).projectionShapeFingerprint(),
		"a projection with no aggregate/lookup source has a stable, empty shape")
}

func TestSchemaChangeError_NoFormatMishaps(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"old", "TEXT"})
	next := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"new", "TEXT"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.Equal(t, "schema_change_requires_rebuild", change.Code())
	require.False(t, strings.Contains(change.Error(), "%!"))
}
