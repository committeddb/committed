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

func TestSchemaChangeError_NoFormatMishaps(t *testing.T) {
	old := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"old", "TEXT"})
	next := schema("tenants", "id", [2]string{"id", "VARCHAR(128)"}, [2]string{"new", "TEXT"})

	change := materializedSchemaChange(old, next)
	require.NotNil(t, change)
	require.Equal(t, "schema_change_requires_rebuild", change.Code())
	require.False(t, strings.Contains(change.Error(), "%!"))
}
