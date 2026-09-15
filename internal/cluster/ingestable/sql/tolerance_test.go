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
// (the reference corpus from .claude-scratch/tickets/viper-containment.md):
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

[SQL.Options]
SlotName   = "my_slot"
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
	require.Equal(t, sql.Options{SlotName: "my_slot", Publication: "my_pub"}, config.Options,
		"option keys match case-insensitively")
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

// [sql.options] is committed's own table: the pre-0.8.0 [sql.<dialect>]
// spelling and the snake_case keys park with the rename (a stored config
// under either resumes from its checkpoint once re-POSTed), and a key the
// configured dialect does not read is refused rather than ignored.
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
	config, err := parse(base + "[sql.options]\nslotName = \"neutral\"\npublication = \"p\"\nbatchSize = 250\n")
	require.NoError(t, err)
	require.Equal(t, sql.Options{SlotName: "neutral", Publication: "p", BatchSize: 250}, config.Options)

	_, err = parse(base + "[sql.postgres]\nslot_name = \"old\"\n")
	require.Error(t, err)
	require.Equal(t, "sql.postgres", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), "removed in 0.8.0")
	require.Contains(t, err.Error(), "slot_name → slotName")

	_, err = parse(base + "[sql.options]\nslot_name = \"old\"\n")
	require.Error(t, err)
	require.Equal(t, "sql.options.slot_name", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), `spell it "slotName"`)

	_, err = parse(base + "[sql.options]\nsnapshotReaders = 4\n")
	require.Error(t, err)
	require.Equal(t, "sql.options.snapshotReaders", cluster.NewConfigError(err).Field)
	require.Contains(t, err.Error(), "not read by the postgres dialect")

	_, err = parse(base + "[sql.options]\nbatchSize = 0\n")
	require.Error(t, err)
	require.Equal(t, "sql.options.batchSize", cluster.NewConfigError(err).Field)

	_, err = parse(base + "[sql.options]\nbatchSize = \"10\"\n")
	require.Error(t, err, "a quoted number is a string, not the integer the key takes")
}
