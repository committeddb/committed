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
		config         *sql.SQLConfig
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

func simpleConfig() *sql.SQLConfig {
	m1 := sql.SQLMapping{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}
	m2 := sql.SQLMapping{JsonPath: "$.one", Column: "one", SQLType: "TEXT"}
	m := []sql.SQLMapping{m1, m2}

	i1 := sql.Index{IndexName: "firstIndex", ColumnNames: "one"}
	i := []sql.Index{i1}
	return &sql.SQLConfig{SQLDB: "testdb", Topic: "test1", Table: "foo", Mappings: m, Indexes: i, PrimaryKey: "pk"}
}
