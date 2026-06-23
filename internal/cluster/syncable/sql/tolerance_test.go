package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// These tests pin decode tolerances that deployed configs may depend on
// (the golden corpus from .claude-scratch/tickets/viper-containment.md):
// the parser must keep accepting case-variant section and field names —
// historical viper behavior, load-bearing because stored configs are
// re-parsed from the log on every node restart. They are written against
// the current pipeline and must stay green across any decoder change.

// Case-variant section/field names decode identically to the canonical
// lowercase form.
func TestParseConfigToleratesCaseVariantKeys(t *testing.T) {
	variant := `
[SQL]
Topic      = "simple"
DB         = "testdb"
Table      = "foo"
PrimaryKey = "pk"
KEYCOLUMN  = "pk"

[[SQL.Mappings]]
JsonPath = "$.key"
Column   = "pk"
Type     = "TEXT"

[[SQL.Indexes]]
Name  = "firstIndex"
Index = "pk"
`
	v := readConfig(t, "toml", strings.NewReader(variant))
	dbs := map[string]cluster.Database{"testdb": testDB}
	config, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	require.NoError(t, err)

	require.Equal(t, "simple", config.Topic)
	require.Equal(t, "foo", config.Table)
	require.Equal(t, "pk", config.PrimaryKey)
	require.Equal(t, "pk", config.KeyColumn)
	require.Equal(t, []sql.Mapping{{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}}, config.Mappings)
	require.Equal(t, []sql.Index{{IndexName: "firstIndex", ColumnNames: "pk"}}, config.Indexes)
}

// JSON-mimetype configs (the API accepts application/json) decode to
// the same Config as their TOML twin.
func TestParseConfigJSONMimeType(t *testing.T) {
	jsonConfig := `{
  "sql": {
    "topic": "simple",
    "db": "testdb",
    "table": "foo",
    "primaryKey": "pk",
    "mappings": [
      { "jsonPath": "$.key", "column": "pk", "type": "TEXT" }
    ]
  }
}`
	v := readConfig(t, "json", strings.NewReader(jsonConfig))
	dbs := map[string]cluster.Database{"testdb": testDB}
	config, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	require.NoError(t, err)

	require.Equal(t, "simple", config.Topic)
	require.Equal(t, "foo", config.Table)
	require.Equal(t, "pk", config.PrimaryKey)
	require.Equal(t, []sql.Mapping{{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}}, config.Mappings)
}

// Projection rules keep user data (jsonpaths in path/from/keyPath
// values) byte-exact even when the operator case-varies the field
// names around them.
func TestParseProjectionToleratesCaseVariantKeys(t *testing.T) {
	variant := `
[SQL-PROJECTION]
Topic      = "t"
DB         = "testdb"
Table      = "rows"
PrimaryKey = "id"
KeyPath    = "$.Meta.ID"

[[SQL-PROJECTION.Columns]]
Name = "id"
Type = "TEXT"

[[SQL-PROJECTION.Columns]]
Name = "v"
Type = "TEXT"

[[SQL-PROJECTION.Rules]]
When = [ { Path = "$.eventType", Equals = "x" } ]
Set  = [ { Column = "v", From = "$.camelCase" } ]
`
	v := readConfig(t, "toml", strings.NewReader(variant))
	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.NoError(t, err)

	require.Equal(t, "$.Meta.ID", config.Sources[0].KeyPath, "user data in values must keep case")
	require.Equal(t, []sql.WhenClause{{Path: "$.eventType", Equals: "x"}}, config.Sources[0].Rules[0].When)
	require.Equal(t, "$.camelCase", config.Sources[0].Rules[0].Set[0].From)
}
