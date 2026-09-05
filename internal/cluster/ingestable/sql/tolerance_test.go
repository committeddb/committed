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

// Pins decode tolerances deployed ingestable configs may depend on
// (the golden corpus from .claude-scratch/tickets/viper-containment.md):
// case-variant field names keep decoding, the tables list keeps its
// order and case (table names are user data), and dialect option keys
// are matched case-insensitively. Written against the current pipeline;
// must stay green across any decoder change.
func TestIngestableParseConfigToleratesCaseVariantKeys(t *testing.T) {
	variant := `
[INGESTABLE]
Name = "foo"
Type = "sql"

[SQL]
Dialect          = "postgres"
Topic            = "simple"
ConnectionString = "postgres://user:pass@localhost:5432/db"
PrimaryKey       = "pk"
Tables           = ["public.Orders", "public.customers"]

[[SQL.Mappings]]
JsonName = "pk"
Column   = "pk"

[SQL.Postgres]
Slot_Name   = "my_slot"
Publication = "my_pub"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(variant)))

	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}

	config, dialect, err := p.ParseConfig(v)
	require.NoError(t, err)
	require.Equal(t, &postgres.PostgreSQLDialect{}, dialect)

	require.Equal(t, "postgres://user:pass@localhost:5432/db", config.ConnectionString)
	require.Equal(t, []string{"pk"}, config.PrimaryKey)
	require.Equal(t, []string{"public.Orders", "public.customers"}, config.Tables,
		"table names are user data — case preserved")
	require.Equal(t, []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}}, config.Mappings)
	require.Equal(t, map[string]string{
		"slot_name":   "my_slot",
		"publication": "my_pub",
	}, config.Options, "dialect option keys match case-insensitively (lowercased)")
}

// The [sql] vocabulary is closed: a typo'd key is rejected naming the key and
// its nearest known sibling, at every spelling case.
func TestIngestableParseConfigRejectsUnknownKeys(t *testing.T) {
	variant := `
[sql]
dialect          = "postgres"
topic            = "simple"
connectionString = "postgres://user:pass@localhost:5432/db"
primaryKey       = "pk"
tables           = ["t"]
mapAllColumn     = true
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(variant)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}
	_, _, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Equal(t, "sql.mapAllColumn", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `did you mean "mapAllColumns"?`)
}

// [sql.options] is the documented, dialect-neutral option table; the older
// [sql.<dialect>] spelling still reads, and an option set in both is refused.
func TestIngestableParseConfigReadsSQLOptions(t *testing.T) {
	base := `
[sql]
dialect          = "postgres"
topic            = "simple"
connectionString = "postgres://user:pass@localhost:5432/db"
primaryKey       = "pk"
tables           = ["t"]
mapAllColumns    = true
`
	parse := func(toml string) (*sql.Config, error) {
		v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
		tiper := &sqlfakes.FakeTyper{}
		tiper.ResolveTypeReturns(simpleType, nil)
		p := sql.NewIngestableParser(tiper)
		p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}
		config, _, err := p.ParseConfig(v)
		return config, err
	}
	config, err := parse(base + "[sql.options]\nslot_name = \"neutral\"\n[sql.postgres]\npublication = \"dialect\"\n")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"slot_name": "neutral", "publication": "dialect"}, config.Options,
		"both tables read into one option set")

	_, err = parse(base + "[sql.options]\nslot_name = \"a\"\n[sql.postgres]\nslot_name = \"b\"\n")
	require.Error(t, err)
	require.Equal(t, "sql.options.slot_name", cluster.NewConfigError(err).Field)
}
