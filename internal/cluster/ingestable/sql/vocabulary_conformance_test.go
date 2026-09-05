package sql_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

// TestSQLVocabulary_EqualsTheReads pins the ingest [sql] declaration to what
// ParseConfig (both forms) and TopicsFromConfig actually read.
func TestSQLVocabulary_EqualsTheReads(t *testing.T) {
	flat := `
[sql]
dialect          = "postgres"
connectionString = "postgres://user:pass@localhost:5432/db"
topic            = "simple"
tables           = ["t"]
primaryKey       = "pk"
mapAllColumns    = true
excludeColumns   = ["skip"]
jsonColumns      = ["doc"]
[[sql.mappings]]
jsonName = "pk"
column   = "pk"
[sql.options]
slot_name = "s"
[sql.postgres]
publication = "p"
`
	topics := `
[sql]
dialect          = "postgres"
connectionString = "postgres://user:pass@localhost:5432/db"
[[sql.topics]]
topic      = "simple"
tables     = ["t"]
primaryKey = "pk"
mapAllColumns = true
`
	newParser := func() *sql.IngestableParser {
		tiper := &sqlfakes.FakeTyper{}
		tiper.ResolveTypeReturns(simpleType, nil)
		p := sql.NewIngestableParser(tiper)
		p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}
		return p
	}
	read := cluster.ObserveConfigReads(func() {
		for _, doc := range []string{flat, topics} {
			v := readConfig(t, "toml", bytes.NewReader([]byte(doc)))
			p := newParser()
			_, _, err := p.ParseConfig(v)
			require.NoError(t, err)
			_ = p.TopicsFromConfig(v)
		}
	})
	undeclared, unread := cluster.VocabularyDiff(sql.SQLSectionKeys("postgres"), read["sql"])
	require.Empty(t, undeclared, "[sql]: keys read but not declared — they would be rejected at POST")
	require.Empty(t, unread, "[sql]: keys declared but never read — a typo there would be silently inert again")
}
