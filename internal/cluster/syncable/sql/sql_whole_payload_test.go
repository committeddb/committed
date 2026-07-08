package sql_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/dolthub/go-mysql-server/driver"
	"github.com/dolthub/go-mysql-server/memory"
	sqle "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

// wholePayloadConfig is the read-model shape the "$" mapping exists for: one
// scalar envelope column for indexing plus the entire payload document in a
// single column of the given type.
func wholePayloadConfig(payloadType string) *sql.Config {
	return &sql.Config{
		Topic: "simple",
		Table: "events",
		Mappings: []sql.Mapping{
			{JsonPath: "$.key", Column: "pk", SQLType: "VARCHAR(64)"},
			{JsonPath: "$", Column: "payload", SQLType: payloadType},
		},
		PrimaryKey: "pk",
	}
}

// decodeJSON parses with UseNumber so numbers compare by their exact digits
// rather than as float64 — a semantic-equality assertion must not hide the
// precision loss the raw-bytes binding exists to prevent.
func decodeJSON(t *testing.T, s string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	require.Nil(t, dec.Decode(&v))
	return v
}

// TestSyncWholePayloadBindsRawBytes: a "$" mapping binds the exact submitted
// document alongside scalar mappings. The payload deliberately carries an
// integer above 2^53 and non-alphabetical key order: binding a re-marshal of
// the unmarshaled document would corrupt the integer (float64 round trip)
// and could reorder keys, so the exact-args expectation proves the raw-bytes
// path.
func TestSyncWholePayloadBindsRawBytes(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	config := wholePayloadConfig("TEXT")
	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	insertPrepare := mock.ExpectPrepare(dialect.CreateSQL(config))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(config))

	syncable := sql.New(db, config)
	require.Nil(t, syncable.Init())

	raw := `{"key":"key1","big":9007199254740993,"zfirst":true,"after":"kept"}`

	mock.ExpectBegin()
	// The sqlmock dialect doubles the args like MySQL.
	insertPrepare.ExpectExec().WithArgs("key1", raw, "key1", raw).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ss, err := syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(simpleType, []byte("key1"), []byte(raw)),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)
	require.Nil(t, mock.ExpectationsWereMet())
}

// TestInitRejectsWholePayloadIntoNonTextColumn: "$" into INT used to surface
// only at exec time, as a driver bind error that IsPermanent cannot classify
// — an endless retry loop. It must now fail Init before any DDL reaches the
// destination database; the mock has no expectations, so a DDL exec would
// also fail the test.
func TestInitRejectsWholePayloadIntoNonTextColumn(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.Nil(t, err)
	db, err := sql.NewDB(dialect, "")
	require.Nil(t, err)
	defer db.Close()

	err = sql.New(db, wholePayloadConfig("INT")).Init()
	require.Error(t, err)
	require.Contains(t, err.Error(), `"payload"`)
	require.Contains(t, err.Error(), `"INT"`)
	require.Nil(t, mock.ExpectationsWereMet())
}

type memDBs []sqle.Database

var _ mysqldriver.Provider = memDBs{}

func (d memDBs) Resolve(name string, options *mysqldriver.Options) (string, sqle.DatabaseProvider, error) {
	return name, memory.NewDBProvider(d...), nil
}

// newGoMySQLServerDB opens an in-process go-mysql-server database, the same
// MySQL-dialect-family path http_test.go drives end-to-end.
func newGoMySQLServerDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	memdb := memory.NewDatabase(name)
	memdb.EnablePrimaryKeyIndexes()
	drv := mysqldriver.New(memDBs{memdb}, nil)
	db, err := sql.NewDB(&testdialects.GoMySQLServerDialect{Driver: drv}, name)
	require.Nil(t, err)
	return db
}

// TestSyncWholePayloadMySQLJSON lands a "$" mapping in a JSON column on the
// MySQL path and reads it back semantically equal. Native JSON columns
// normalize key order and whitespace, so byte equality is not the contract
// here — TestSyncWholePayloadMySQLTextByteExact covers that for TEXT.
func TestSyncWholePayloadMySQLJSON(t *testing.T) {
	db := newGoMySQLServerDB(t, "wholepayloadjson")
	defer db.Close()

	syncable := sql.New(db, wholePayloadConfig("JSON"))
	require.Nil(t, syncable.Init())

	raw := `{"key":"key1","zfirst":"z","event":"tenant-created","count":42}`
	ss, err := syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(simpleType, []byte("key1"), []byte(raw)),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)

	var pk, payload string
	require.Nil(t, db.DB.QueryRow("SELECT pk, payload FROM events WHERE pk = ?", "key1").Scan(&pk, &payload))
	require.Equal(t, "key1", pk)
	require.Equal(t, decodeJSON(t, raw), decodeJSON(t, payload))
}

// TestSyncWholePayloadMySQLTextByteExact: a TEXT column keeps the document
// byte-exact on the MySQL path, including an integer above 2^53 — a float64
// round trip would come back as …992 and fail the comparison.
func TestSyncWholePayloadMySQLTextByteExact(t *testing.T) {
	db := newGoMySQLServerDB(t, "wholepayloadtext")
	defer db.Close()

	syncable := sql.New(db, wholePayloadConfig("TEXT"))
	require.Nil(t, syncable.Init())

	raw := `{"key":"key1","big":9007199254740993,"z":"last"}`
	ss, err := syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(simpleType, []byte("key1"), []byte(raw)),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)

	var payload string
	require.Nil(t, db.DB.QueryRow("SELECT payload FROM events WHERE pk = ?", "key1").Scan(&payload))
	require.Equal(t, raw, payload)
}
