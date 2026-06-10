package sql_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
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

// wholePayloadTOML is the event-log manifest shape: a scalar envelope column
// plus a "$" mapping landing the whole payload in one column of payloadType.
func wholePayloadTOML(payloadType string) string {
	return `
[sql]
topic      = "controlplane-event"
db         = "testdb"
table      = "tenant_events"
primaryKey = "event_id"

[[sql.mappings]]
jsonPath = "$.event_id"
column   = "event_id"
type     = "VARCHAR(64)"

[[sql.mappings]]
jsonPath = "$"
column   = "payload"
type     = "` + payloadType + `"
`
}

func TestParseWholePayloadMapping(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(wholePayloadTOML("JSONB")))

	dbs := map[string]cluster.Database{"testdb": testDB}
	config, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	require.Nil(t, err)
	require.Equal(t, []sql.Mapping{
		{JsonPath: "$.event_id", Column: "event_id", SQLType: "VARCHAR(64)"},
		{JsonPath: "$", Column: "payload", SQLType: "JSONB"},
	}, config.Mappings)
}

// TestParseRejectsWholePayloadIntoNonTextColumn: a "$" mapping into a column
// that cannot hold the document must fail at parse time — before consensus,
// before any DDL — with an error naming the column and the offending type.
func TestParseRejectsWholePayloadIntoNonTextColumn(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(wholePayloadTOML("INT")))

	dbs := map[string]cluster.Database{"testdb": testDB}
	_, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
	require.Error(t, err)
	require.Contains(t, err.Error(), `"payload"`)
	require.Contains(t, err.Error(), `"INT"`)
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
