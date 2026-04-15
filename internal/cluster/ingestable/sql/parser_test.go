package sql_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/sqlfakes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
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

func simpleConfig() *sql.Config {
	m1 := sql.Mapping{JsonName: "pk", SQLColumn: "pk"}
	m2 := sql.Mapping{JsonName: "one", SQLColumn: "one"}
	m := []sql.Mapping{m1, m2}

	return &sql.Config{
		ConnectionString: "foo",
		Type:             simpleType,
		Mappings:         m,
		PrimaryKey:       "pk",
		Tables:           nil,
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
		PrimaryKey:       "pk",
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
		PrimaryKey:       "pk",
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
		PrimaryKey:       "pk",
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
		PrimaryKey:       "pk",
		Tables:           []string{"public.orders"},
		Options:          map[string]string{},
	}
}

func readConfig(t *testing.T, configType string, r io.Reader) *viper.Viper {
	v := viper.New()
	v.SetConfigType(configType)
	err := v.ReadConfig(r)
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
