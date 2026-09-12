package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// TestSyncableSQLVocabulary_EqualsTheReads pins the syncable [sql] and the
// database [sql] declarations to what their parsers actually read.
func TestSyncableSQLVocabulary_EqualsTheReads(t *testing.T) {
	dbs := map[string]cluster.Database{"testdb": &TestDatabase{}}
	syncable := `
[sql]
topic      = "simple"
db         = "testdb"
table      = "foo"
primaryKey = "pk"
keyColumn  = "pk"
[[sql.mappings]]
jsonPath = "$.key"
column   = "pk"
type     = "TEXT"
[[sql.indexes]]
name  = "firstIndex"
index = "pk"
`
	read := cluster.ObserveConfigReads(func() {
		v := readConfig(t, "toml", strings.NewReader(syncable))
		p := &sql.SyncableParser{}
		_, err := p.ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
		require.NoError(t, err)
		_ = p.TopicsFromConfig(v)
		_ = p.DatabasesFromConfig(v)
		_, err = p.SchemaFromConfig(v, nil)
		require.NoError(t, err)
	})
	undeclared, unread := cluster.VocabularyDiff(sql.SyncableSQLKeys, read["sql"])
	require.Empty(t, undeclared, "syncable [sql]: keys read but not declared")
	require.Empty(t, unread, "syncable [sql]: keys declared but never read")

	// The database config's [sql]: both keys are read before the dialect
	// lookup, so an unregistered dialect still exercises the reads.
	read = cluster.ObserveConfigReads(func() {
		v := readConfig(t, "toml", strings.NewReader("[sql]\ndialect = \"nope\"\nconnectionString = \"postgres://u:${X}@h/db\"\n"))
		_, _ = (&sql.DBParser{Dialects: map[string]sql.Dialect{}}).Parse(v)
	})
	undeclared, unread = cluster.VocabularyDiff(sql.DatabaseSQLKeys, read["sql"])
	require.Empty(t, undeclared, "database [sql]: keys read but not declared")
	require.Empty(t, unread, "database [sql]: keys declared but never read")
}
