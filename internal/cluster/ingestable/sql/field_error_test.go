package sql_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

func ingestParser(t *testing.T, resolved *cluster.Type, resolveErr error) *sql.IngestableParser {
	t.Helper()
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(resolved, resolveErr)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["mysql"] = &mysql.MySQLDialect{}
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}
	return p
}

func parseSQL(t *testing.T, toml string) *cluster.ParsedConfig {
	t.Helper()
	v, err := cluster.ParseConfigBytes("text/toml", []byte(toml))
	require.NoError(t, err)
	return v
}

func TestIngestableParseConfig_UnknownDialect(t *testing.T) {
	v := parseSQL(t, "[sql]\ndialect = \"oracle\"\ntopic = \"t\"\n")
	_, _, err := ingestParser(t, simpleType, nil).ParseConfig(v)
	requireIngestField(t, err, "sql.dialect")
	require.Contains(t, err.Error(), `unknown dialect "oracle"`)
	require.Contains(t, err.Error(), "mysql, postgres")
}

func TestIngestableParseConfig_MissingTopic(t *testing.T) {
	v := parseSQL(t, "[sql]\ndialect = \"mysql\"\n")
	_, _, err := ingestParser(t, simpleType, nil).ParseConfig(v)
	requireIngestField(t, err, "sql.topic")
	require.Contains(t, err.Error(), "required")
}

func TestIngestableParseConfig_TypeNotFound(t *testing.T) {
	v := parseSQL(t, "[sql]\ndialect = \"mysql\"\ntopic = \"ghost\"\n")
	_, _, err := ingestParser(t, nil, errors.New("type missing")).ParseConfig(v)
	requireIngestField(t, err, "sql.topic")
	require.Contains(t, err.Error(), `type "ghost" not found`)
}

func requireIngestField(t *testing.T, err error, field string) {
	t.Helper()
	require.Error(t, err)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, field, fe.Field)
}
