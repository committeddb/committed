package sql_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

var testDB = &TestDatabase{}

// Topics with a declared entity kind, so the parser can resolve a kind and
// exercise the kind-aware primaryKey requirement.
var (
	snapshotType = &cluster.Type{ID: "acct", Name: "acct", EntityKind: cluster.EntityKindSnapshot}
	eventType    = &cluster.Type{ID: "evt", Name: "evt", EntityKind: cluster.EntityKindEvent}
)

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

// TestParseRejectsMissingRequiredFields: a destination table and at least one
// column mapping are required for any SQL syncable regardless of entity kind — an
// empty one is rejected at POST with a field-scoped error instead of failing
// later as malformed DDL.
func TestParseRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name  string
		toml  string
		field string
	}{
		{
			"missing table",
			`
[sql]
topic = "t"
db = "testdb"
primaryKey = "pk"

[[sql.mappings]]
jsonPath = "$.key"
column   = "pk"
type     = "TEXT"
`,
			"sql.table",
		},
		{
			"missing mappings",
			`
[sql]
topic = "t"
db = "testdb"
table = "foo"
primaryKey = "pk"
`,
			"sql.mappings",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := readConfig(t, "toml", strings.NewReader(tt.toml))
			dbs := map[string]cluster.Database{"testdb": testDB}
			_, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: dbs})
			var fe *cluster.FieldError
			require.ErrorAs(t, err, &fe)
			require.Equal(t, tt.field, fe.Field)
		})
	}
}

// TestParseSnapshotTopicRequiresPrimaryKey: a snapshot-kind topic is a
// key-addressed LWW-per-key upsert sink, so an empty primaryKey — which would
// silently duplicate rows and dead-letter every delete (RTBF loss) — is rejected
// at POST.
func TestParseSnapshotTopicRequiresPrimaryKey(t *testing.T) {
	toml := `
[sql]
topic = "acct"
db = "testdb"
table = "foo"

[[sql.mappings]]
jsonPath = "$.key"
column   = "pk"
type     = "TEXT"
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"acct": snapshotType},
	}
	_, err := (&sql.SyncableParser{}).ParseConfig(v, storage)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.primaryKey", fe.Field)
}

// TestParseEventTopicAllowsEmptyPrimaryKey: an event-kind topic lands as an
// append-only log (whole-payload "$"), which legitimately has no domain primary
// key — the snapshot-only requirement must not reject it.
func TestParseEventTopicAllowsEmptyPrimaryKey(t *testing.T) {
	toml := `
[sql]
topic = "evt"
db = "testdb"
table = "events"

[[sql.mappings]]
jsonPath = "$"
column   = "payload"
type     = "JSONB"
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"evt": eventType},
	}
	config, err := (&sql.SyncableParser{}).ParseConfig(v, storage)
	require.NoError(t, err)
	require.Empty(t, config.PrimaryKey)
}

// TestParseKeylessTableTooLongRejected: a keyless (event-topic, no primaryKey)
// syncable whose table name makes the dedup sidecar (<table>__committed_applied)
// exceed the 63-char identifier limit is rejected at parse — the database would
// otherwise silently truncate the sidecar name and collide with the base table.
func TestParseKeylessTableTooLongRejected(t *testing.T) {
	longTable := strings.Repeat("t", 50) // 50 + len("__committed_applied")=19 = 69 > 63
	toml := `
[sql]
topic = "evt"
db = "testdb"
table = "` + longTable + `"

[[sql.mappings]]
jsonPath = "$"
column   = "payload"
type     = "JSONB"
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"evt": eventType},
	}
	_, err := (&sql.SyncableParser{}).ParseConfig(v, storage)
	var fe *cluster.FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, "sql.table", fe.Field)
	require.Contains(t, fe.Issue, "keyless")
}

func simpleConfig(db cluster.Database) *sql.Config {
	m1 := sql.Mapping{JsonPath: "$.key", Column: "pk", SQLType: "TEXT"}
	m2 := sql.Mapping{JsonPath: "$.one", Column: "one", SQLType: "TEXT"}
	m := []sql.Mapping{m1, m2}

	i1 := sql.Index{IndexName: "firstIndex", ColumnNames: "one"}
	i := []sql.Index{i1}
	return &sql.Config{Database: db, DatabaseID: "testdb", Topic: "simple", Table: "foo", Mappings: m, Indexes: i, PrimaryKey: "pk"}
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
