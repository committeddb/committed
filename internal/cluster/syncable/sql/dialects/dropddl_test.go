package dialects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// DropDDL is the destructive mirror of CreateDDL: an idempotent
// DROP TABLE IF EXISTS that cascades to the table's indexes, so it needs no
// separate index-drop. Every dialect produces the same statement.
func TestDropDDL(t *testing.T) {
	cfg := &sql.Config{
		Table:      "mytable",
		PrimaryKey: "id",
		Mappings: []sql.Mapping{
			{Column: "id", SQLType: "VARCHAR(128)"},
			{Column: "name", SQLType: "TEXT"},
		},
		Indexes: []sql.Index{{IndexName: "idx_name", ColumnNames: "name"}},
	}

	want := "DROP TABLE IF EXISTS mytable;"

	require.Equal(t, want, (&dialects.PostgreSQLDialect{}).DropDDL(cfg))
	require.Equal(t, want, (&dialects.MySQLDialect{}).DropDDL(cfg))
}
