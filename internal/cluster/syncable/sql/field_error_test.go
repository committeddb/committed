package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

func TestSyncableParseConfig_MissingDB(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader("[sql]\ntopic = \"t\"\n"))
	_, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: map[string]cluster.Database{}})
	fe := requireFieldErr(t, err, "sql.db")
	require.Contains(t, fe.Issue, "required")
}

func TestSyncableParseConfig_DatabaseNotFound(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader("[sql]\ndb = \"ghost\"\ntopic = \"t\"\n"))
	_, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: map[string]cluster.Database{}})
	requireFieldErr(t, err, "sql.db")
	require.Contains(t, err.Error(), `database "ghost" not found`)
}

func TestSyncableParseConfig_MissingTopic(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader("[sql]\ndb = \"testdb\"\n"))
	dbs := map[string]cluster.Database{"testdb": testDB}
	_, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	requireFieldErr(t, err, "sql.topic")
}

func TestSyncableDBParser_UnknownDialect(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader("[sql]\ndialect = \"oracle\"\n"))
	p := &sql.DBParser{Dialects: map[string]sql.Dialect{"postgres": nil, "mysql": nil}}
	_, err := p.Parse(v)
	requireFieldErr(t, err, "sql.dialect")
	require.Contains(t, err.Error(), `unknown dialect "oracle"`)
	require.Contains(t, err.Error(), "mysql, postgres")
}

// requireFieldErr asserts err is (or wraps) a *cluster.FieldError on the given
// field, and returns it for further message assertions.
func requireFieldErr(t *testing.T, err error, field string) *cluster.FieldError {
	t.Helper()
	require.Error(t, err)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, field, fe.Field)
	return fe
}
