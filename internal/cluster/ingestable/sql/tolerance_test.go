package sql_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

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
