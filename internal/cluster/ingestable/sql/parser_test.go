package sql_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

var simpleType = &cluster.Type{
	ID:   "bar",
	Name: "bar",
}

func TestParse(t *testing.T) {
	tests := []struct {
		name           string
		configFileName string
		config         *sql.Config
		dialect        sql.Dialect
	}{
		{
			"mysql_simple",
			"./simple_ingestable.toml",
			simpleConfig(),
			&mysql.MySQLDialect{},
		},
		{
			"mysql_with_tables",
			"./mysql_with_tables_ingestable.toml",
			mysqlWithTablesConfig(),
			&mysql.MySQLDialect{},
		},
		{
			"postgres_with_options",
			"./postgres_ingestable.toml",
			postgresConfig(),
			&postgres.PostgreSQLDialect{},
		},
		{
			"postgres_multi_table",
			"./postgres_multi_table_ingestable.toml",
			postgresMultiTableConfig(),
			&postgres.PostgreSQLDialect{},
		},
		{
			"postgres_default_options",
			"./postgres_defaults_ingestable.toml",
			postgresDefaultsConfig(),
			&postgres.PostgreSQLDialect{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)

			v := readConfig(t, "toml", bytes.NewReader(bs))

			tiper := &sqlfakes.FakeTyper{}
			tiper.ResolveTypeReturns(simpleType, nil)
			p := sql.NewIngestableParser(tiper)

			p.Dialects["mysql"] = &mysql.MySQLDialect{}
			p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}

			config, dialect, err := p.ParseConfig(v)
			require.Nil(t, err)

			require.Equal(t, tt.config, config)
			require.Equal(t, tt.dialect, dialect)
		})
	}
}

func TestParseUnknownDialect(t *testing.T) {
	toml := `
[ingestable]
name="foo"
type="sql"

[sql]
dialect="oracle"
topic = "simple"
connectionString="foo"
primaryKey = "pk"

[[sql.mappings]]
jsonName = "pk"
column = "pk"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))

	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)

	_, _, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "oracle")
}

// TestParseMapAllColumns: ParseConfig records mapAllColumns + excludeColumns and
// leaves the listed mappings as the (unexpanded) overrides — Parse expands them
// against the live schema, which ParseConfig does not touch.
func TestParseMapAllColumns(t *testing.T) {
	toml := `
[ingestable]
name="movies"
type="sql"

[sql]
dialect="postgres"
topic = "movie"
connectionString="postgres://u:p@localhost:5432/db?sslmode=disable"
primaryKey = "movie_id"
tables = ["ingress.movie"]
mapAllColumns = true
excludeColumns = ["internal_notes"]

[sql.postgres]
slot_name = "s"

[[sql.mappings]]
jsonName = "movieId"
column = "movie_id"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}

	config, _, err := p.ParseConfig(v)
	require.NoError(t, err)
	require.True(t, config.MapAllColumns)
	require.Equal(t, []string{"internal_notes"}, config.ExcludeColumns)
	require.Equal(t, []sql.Mapping{{JsonName: "movieId", SQLColumn: "movie_id"}}, config.Mappings)
}

// TestParseExcludeColumnsRequiresMapAll: excludeColumns without mapAllColumns is
// a misconfiguration (it would silently do nothing), caught at parse.
func TestParseExcludeColumnsRequiresMapAll(t *testing.T) {
	toml := `
[ingestable]
name="movies"
type="sql"

[sql]
dialect="postgres"
topic = "movie"
connectionString="postgres://u:p@localhost:5432/db?sslmode=disable"
primaryKey = "movie_id"
tables = ["ingress.movie"]
excludeColumns = ["secret"]

[sql.postgres]
slot_name = "s"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}

	_, _, err := p.ParseConfig(v)
	require.ErrorContains(t, err, "excludeColumns requires sql.mapAllColumns")
}

// TestParseRejectsMissingRequiredFields is the fail-fast regression: an
// ingestable missing primaryKey, tables, or a mapping source (mappings /
// mapAllColumns) is rejected at POST with a field-scoped error, rather than
// accepted and left to wedge — an empty primaryKey collapses every row onto the
// single "[]" key and the snapshot's `ORDER BY ""` retries forever; empty tables
// ingests nothing.
func TestParseRejectsMissingRequiredFields(t *testing.T) {
	base := func(extra string) string {
		return `
[ingestable]
name="foo"
type="sql"

[sql]
dialect="mysql"
topic = "simple"
connectionString="foo"
` + extra
	}
	mappingsBlock := "\n[[sql.mappings]]\njsonName = \"pk\"\ncolumn = \"pk\"\n"

	tests := []struct {
		name  string
		toml  string
		field string
	}{
		{"missing primaryKey", base(`tables = ["t"]` + mappingsBlock), "sql.primaryKey"},
		{"missing tables", base(`primaryKey = "pk"` + mappingsBlock), "sql.tables"},
		{"no mappings and not mapAllColumns", base("primaryKey = \"pk\"\ntables = [\"t\"]\n"), "sql.mappings"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := readConfig(t, "toml", bytes.NewReader([]byte(tt.toml)))
			tiper := &sqlfakes.FakeTyper{}
			tiper.ResolveTypeReturns(simpleType, nil)
			p := sql.NewIngestableParser(tiper)
			p.Dialects["mysql"] = &mysql.MySQLDialect{}

			_, _, err := p.ParseConfig(v)

			var fe *cluster.FieldError
			require.ErrorAs(t, err, &fe)
			require.Equal(t, tt.field, fe.Field)
		})
	}
}

// TestParseRejectsInternalTopicType is the ingest half of the cluster-brick guard
// (the sibling of the AddProposal / ParseType guards): an ingest sql.topic whose id
// is a committed-internal/system type must be rejected at admission. Emitted rows
// carry this type, and at apply resolveType is systemType-first, so an internal
// topic would route user row bytes into an internal config handler that Fatals —
// crash-looping every node. ParseType blocks CREATING such a type on a fresh
// cluster, but a colliding type from a pre-guard binary or a restore must not be
// usable as an ingest topic either, so the guard must fire structurally, BEFORE the
// topic type is even resolved (asserted via ResolveTypeCallCount == 0).
func TestParseRejectsInternalTopicType(t *testing.T) {
	// databaseType's grandfathered built-in id (frozen; cluster.IsInternal == true).
	const systemTypeID = "4698b77e-9a7c-41a2-aae4-984da0cd33c1"
	require.True(t, cluster.IsInternal(systemTypeID), "guard test needs a real internal type id")

	toml := `
[ingestable]
name="foo"
type="sql"

[sql]
dialect="mysql"
topic = "` + systemTypeID + `"
connectionString="foo"
primaryKey = "pk"
tables = ["t"]

[[sql.mappings]]
jsonName = "pk"
column = "pk"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil) // even if a colliding user type exists...
	p := sql.NewIngestableParser(tiper)
	p.Dialects["mysql"] = &mysql.MySQLDialect{}

	_, _, err := p.ParseConfig(v)

	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.topic", fe.Field)
	require.Equal(t, 0, tiper.ResolveTypeCallCount(),
		"the guard must reject before resolving the topic type — a pre-existing colliding type must not be usable as an ingest topic")
}

// TestParseAcceptsMapAllColumnsWithoutMappings confirms the mapping requirement
// is satisfied by mapAllColumns alone — no explicit [[sql.mappings]] needed.
func TestParseAcceptsMapAllColumnsWithoutMappings(t *testing.T) {
	toml := `
[ingestable]
name="foo"
type="sql"

[sql]
dialect="mysql"
topic = "simple"
connectionString="foo"
primaryKey = "pk"
tables = ["t"]
mapAllColumns = true
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(toml)))
	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(simpleType, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["mysql"] = &mysql.MySQLDialect{}

	config, _, err := p.ParseConfig(v)
	require.NoError(t, err)
	require.True(t, config.MapAllColumns)
}

func simpleConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "foo",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       []string{"pk"},
		Tables:           []string{"simple"},
		Options:          map[string]string{},
	}
}

func mysqlWithTablesConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "mysql://user:pass@host:3306/db",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       []string{"pk"},
		Tables:           []string{"orders", "customers"},
		Options:          map[string]string{},
	}
}

func postgresConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       []string{"pk"},
		Tables:           []string{"public.orders"},
		Options: map[string]string{
			"slot_name":   "my_slot",
			"publication": "my_pub",
		},
	}
}

func postgresMultiTableConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       []string{"pk"},
		Tables:           []string{"public.orders", "public.customers", "public.items"},
		Options: map[string]string{
			"slot_name":   "multi_slot",
			"publication": "multi_pub",
		},
	}
}

func postgresDefaultsConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       []string{"pk"},
		Tables:           []string{"public.orders"},
		Options:          map[string]string{},
	}
}

func readConfig(t *testing.T, configType string, r io.Reader) *cluster.ParsedConfig {
	bs, err := io.ReadAll(r)
	require.Nil(t, err)
	mimeType := "text/toml"
	if configType == "json" {
		mimeType = "application/json"
	}
	v, err := cluster.ParseConfigBytes(mimeType, bs)
	require.Nil(t, err)

	return v
}

type TestDatabase struct{}

func (d *TestDatabase) Close() error {
	return nil
}

func (d *TestDatabase) GetType() string {
	return "test"
}

type TestDatabaseStorage struct {
	dbs map[string]cluster.Database
}

func (s *TestDatabaseStorage) Database(id string) (cluster.Database, error) {
	db, ok := s.dbs[id]
	if ok {
		return db, nil
	}

	return nil, fmt.Errorf("not found")
}
