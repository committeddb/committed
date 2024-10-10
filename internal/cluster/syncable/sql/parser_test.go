package sql_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		configFileName string
		config         *sql.Config
	}{
		{"./simple.toml", simpleConfig()},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			bs, err := os.ReadFile(tt.configFileName)
			require.Nil(t, err)
			config := sql.Parse("toml", bytes.NewReader(bs))

			require.Equal(t, tt.config, config)
		})
	}
}

func simpleConfig() *sql.Config {
	m1 := sql.Mapping{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}
	m2 := sql.Mapping{JsonPath: "$.one", Column: "one", SQLType: "TEXT"}
	m := []sql.Mapping{m1, m2}

	i1 := sql.Index{IndexName: "firstIndex", ColumnNames: "one"}
	i := []sql.Index{i1}
	return &sql.Config{SQLDB: "testdb", Topic: "test1", Table: "foo", Mappings: m, Indexes: i, PrimaryKey: "pk"}
}
