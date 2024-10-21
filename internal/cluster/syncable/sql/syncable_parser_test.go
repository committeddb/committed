package sql_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

var testDB = &TestDatabase{}

func TestParse(t *testing.T) {
	tests := []struct {
		configFileName string
		config         *sql.Config
	}{
		{"./simple_syncable.toml", simpleConfig(testDB)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)

			v := readConfig(t, "toml", bytes.NewReader(bs))

			p := &sql.SyncableParser{}

			dbs := make(map[string]cluster.Database)
			dbs["testdb"] = testDB
			config, err := p.ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
			require.Nil(t, err)

			require.Equal(t, tt.config, config)
		})
	}
}

func simpleConfig(db cluster.Database) *sql.Config {
	m1 := sql.Mapping{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}
	m2 := sql.Mapping{JsonPath: "$.one", Column: "one", SQLType: "TEXT"}
	m := []sql.Mapping{m1, m2}

	i1 := sql.Index{IndexName: "firstIndex", ColumnNames: "one"}
	i := []sql.Index{i1}
	return &sql.Config{Database: db, Topic: "simple", Table: "foo", Mappings: m, Indexes: i, PrimaryKey: "pk"}
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
