package sql_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

// topicsParser builds a parser whose typer resolves every requested topic id to a
// same-named user type, so a multi-topic config yields distinct types per spec.
func topicsParser() *sql.IngestableParser {
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeStub = func(ref cluster.TypeRef) (*cluster.Type, error) {
		return &cluster.Type{ID: ref.ID, Name: ref.ID}, nil
	}
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}
	return p
}

// The [[sql.topics]] form parses one self-contained spec per entry: each topic
// keeps its own type, tables, primaryKey (scalar OR list), and mappings; the union
// table set and the singular fields mirror Topics[0]; and every table routes to its
// own topic.
func TestParseTopics_MultipleTopics(t *testing.T) {
	toml := `
[ingestable]
name = "db"
type = "sql"

[sql]
dialect = "postgres"
connectionString = "postgres://u:p@h:5432/db?sslmode=disable"

[sql.postgres]
slot_name = "s"

[[sql.topics]]
topic = "orders"
tables = ["orders_us", "orders_eu"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"

[[sql.topics]]
topic = "customers"
tables = ["customers"]
primaryKey = ["cust_id", "region"]
[[sql.topics.mappings]]
jsonName = "custId"
column = "cust_id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	config, _, err := topicsParser().ParseConfig(v)
	require.NoError(t, err)
	require.Len(t, config.Topics, 2)

	orders := config.Topics[0]
	require.Equal(t, "orders", orders.Type.ID)
	require.Equal(t, []string{"orders_us", "orders_eu"}, orders.Tables)
	require.Equal(t, []string{"id"}, orders.PrimaryKey) // scalar coerced to a one-element list
	require.Equal(t, []sql.Mapping{{JsonName: "id", SQLColumn: "id"}}, orders.Mappings)

	customers := config.Topics[1]
	require.Equal(t, "customers", customers.Type.ID)
	require.Equal(t, []string{"customers"}, customers.Tables)
	require.Equal(t, []string{"cust_id", "region"}, customers.PrimaryKey) // composite list form
	require.Equal(t, []sql.Mapping{{JsonName: "custId", SQLColumn: "cust_id"}}, customers.Mappings)

	// Union table set and singular mirror of Topics[0].
	require.Equal(t, []string{"orders_us", "orders_eu", "customers"}, config.Tables)
	require.Equal(t, "orders", config.Type.ID)
	require.Equal(t, orders.Mappings, config.Mappings)
	require.Equal(t, "postgres://u:p@h:5432/db?sslmode=disable", config.ConnectionString)

	// Routing: each source table resolves to its own topic.
	require.Equal(t, "orders", config.SpecForTable("orders_eu").Type.ID)
	require.Equal(t, "customers", config.SpecForTable("customers").Type.ID)
}

// A stray flat per-topic field alongside [[sql.topics]] is ambiguous and rejected.
func TestParseTopics_RejectsFlatFieldsAlongside(t *testing.T) {
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"
topic = "stray"

[[sql.topics]]
topic = "orders"
tables = ["orders"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topic", fe.Field)
	require.Contains(t, fe.Issue, "[[sql.topics]]")
}

// A table routed to two topics is rejected — the routing resolvers assume a table
// feeds exactly one topic.
func TestParseTopics_RejectsTableInTwoTopics(t *testing.T) {
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"

[[sql.topics]]
topic = "orders"
tables = ["shared", "orders_only"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"

[[sql.topics]]
topic = "customers"
tables = ["Shared"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topics[1].tables", fe.Field)
	require.Contains(t, fe.Issue, "only one topic")
	require.Contains(t, fe.Issue, "orders", "names the topic that already owns the table")
}

// Two entries claiming the same topic id are rejected.
func TestParseTopics_RejectsDuplicateTopic(t *testing.T) {
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"

[[sql.topics]]
topic = "orders"
tables = ["a"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"

[[sql.topics]]
topic = "orders"
tables = ["b"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topics[1].topic", fe.Field)
	require.Contains(t, fe.Issue, "already defined")
}

// A committed-internal/system type id cannot be an ingest topic in any spec.
func TestParseTopics_RejectsInternalTopic(t *testing.T) {
	const systemTypeID = "4698b77e-9a7c-41a2-aae4-984da0cd33c1"
	require.True(t, cluster.IsInternal(systemTypeID))
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"

[[sql.topics]]
topic = "` + systemTypeID + `"
tables = ["t"]
primaryKey = "id"
[[sql.topics.mappings]]
jsonName = "id"
column = "id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topics[0].topic", fe.Field)
	require.Contains(t, fe.Issue, "system-type")
}

// A required field missing from one spec fails, positioned at that spec's field.
func TestParseTopics_RejectsMissingPrimaryKey(t *testing.T) {
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"

[[sql.topics]]
topic = "orders"
tables = ["orders"]
[[sql.topics.mappings]]
jsonName = "id"
column = "id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topics[0].primaryKey", fe.Field)
}

// An empty [[sql.topics]] array is rejected rather than accepted as a no-op.
func TestParseTopics_RejectsEmptyArray(t *testing.T) {
	toml := `
[sql]
dialect = "postgres"
connectionString = "postgres://h/db"
topics = []
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	_, _, err := topicsParser().ParseConfig(v)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topics", fe.Field)
}
