package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

func parsedTOML(t *testing.T, doc string) *cluster.ParsedConfig {
	t.Helper()
	v, err := cluster.ParseConfigBytes("text/toml", []byte(doc))
	require.NoError(t, err)
	return v
}

// TestTopicsFromConfig_FlatForm: the single-topic shape reports its one
// produced topic, read from the document alone (no dialect, no source).
func TestTopicsFromConfig_FlatForm(t *testing.T) {
	p := &sql.IngestableParser{}
	topics := p.TopicsFromConfig(parsedTOML(t, `[ingestable]
name = "i"
type = "sql"
[sql]
dialect = "mysql"
topic = "orders"
tables = ["orders"]
primaryKey = "id"`))
	require.Equal(t, []string{"orders"}, topics)
}

// TestTopicsFromConfig_MultiTopicForm: the [[sql.topics]] array reports one
// produced topic per entry, deduped — even when entries would fail full
// validation, because the producer guard must see every CLAIM.
func TestTopicsFromConfig_MultiTopicForm(t *testing.T) {
	p := &sql.IngestableParser{}
	topics := p.TopicsFromConfig(parsedTOML(t, `[ingestable]
name = "i"
type = "sql"
[sql]
dialect = "mysql"
[[sql.topics]]
topic = "orders"
tables = ["orders"]
primaryKey = "id"
[[sql.topics]]
topic = "customers"
[[sql.topics]]
topic = "orders"`))
	require.Equal(t, []string{"orders", "customers"}, topics)
}

// TestTopicsFromConfig_NoTopics: a document claiming nothing contributes no
// producer edges (nil, not an error).
func TestTopicsFromConfig_NoTopics(t *testing.T) {
	p := &sql.IngestableParser{}
	require.Nil(t, p.TopicsFromConfig(parsedTOML(t, `[ingestable]
name = "i"
type = "sql"
[sql]
dialect = "mysql"`)))
}
