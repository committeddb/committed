package sql_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

func teardownConfig() *sql.Config {
	return &sql.Config{
		Table:      "events",
		PrimaryKey: "id",
		Mappings:   []sql.Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
	}
}

// Teardown runs the dialect's DROP TABLE IF EXISTS against the destination DB.
// It is the destructive mirror of Init and needs none of Init's prepared
// statements — only the config + DB handle — which is what lets the delete
// path reconstruct a teardown handle from the pre-delete config.
func TestSyncable_Teardown(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectExec(dialect.DropDDL(config)).WillReturnResult(driver.ResultNoRows)

	require.NoError(t, syncable.Teardown())
	require.NoError(t, mock.ExpectationsWereMet())
}

// A failed drop returns a typed, wrapped error (never panics) so the caller
// can log it and continue — the logical delete has already succeeded.
func TestSyncable_Teardown_WrapsError(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectExec(dialect.DropDDL(config)).WillReturnError(errors.New("permission denied"))

	err = syncable.Teardown()
	require.Error(t, err)
	require.Contains(t, err.Error(), "teardown")
	require.Contains(t, err.Error(), "permission denied")
}

// Idempotency: a second teardown of an already-dropped table is still just a
// DROP TABLE IF EXISTS, a no-op at the database, not an error.
func TestSyncable_Teardown_Idempotent(t *testing.T) {
	dialect, mock, err := dialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectExec(dialect.DropDDL(config)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(dialect.DropDDL(config)).WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, syncable.Teardown())
	require.NoError(t, syncable.Teardown())
	require.NoError(t, mock.ExpectationsWereMet())
}
