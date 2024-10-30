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
		configFileName string
		config         *sql.Config
		dialect        sql.Dialect
	}{
		{"./simple_ingestable.toml", simpleConfig(), &mysql.MySQLDialect{}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)

			v := readConfig(t, "toml", bytes.NewReader(bs))

			tiper := &sqlfakes.FakeTyper{}
			tiper.TypeReturns(simpleType, nil)
			p := sql.NewIngestableParser(tiper)

			p.Dialects["mysql"] = &mysql.MySQLDialect{}

			config, dialect, err := p.ParseConfig(v)
			require.Nil(t, err)

			require.Equal(t, tt.config, config)
			require.Equal(t, tt.dialect, dialect)
		})
	}
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
