package sql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

func teardownConfig() *sql.Config {
	return &sql.Config{
		Table:      "events",
		PrimaryKey: []string{"id"},
		Mappings:   []sql.Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
	}
}

// noteRows is the destination note as the mock returns it.
func noteRows(owned bool) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"rendering_version", "owned"}).AddRow(int64(sql.SinkRenderingVersion), owned)
}

// Teardown of a table committed created: read the note (owned), run the
// dialect's DROP TABLE IF EXISTS, remove the note. It is the destructive
// mirror of Init and needs none of Init's prepared statements — only the
// config + DB handle — which is what lets the delete path reconstruct a
// teardown handle from the pre-delete config.
func TestSyncable_Teardown(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(noteRows(true))
	mock.ExpectExec(dialect.DropDDL(config)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(dialect.SinkMetaDeleteSQL()).WithArgs(config.Table).WillReturnResult(driver.ResultNoRows)

	dropped, err := syncable.Teardown(false)
	require.NoError(t, err)
	require.True(t, dropped)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The ownership protocol: a table committed attached to (no note, or a note
// that says not owned) is left in place, note and all — committed drops only
// what it created.
func TestSyncable_Teardown_AttachedTableStays(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{"never noted", sqlmock.NewRows([]string{"rendering_version", "owned"})},
		{"noted, not owned", noteRows(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialect, mock, err := testdialects.NewSQLMockDialect()
			require.NoError(t, err)
			db, err := sql.NewDB(dialect, "")
			require.NoError(t, err)

			config := teardownConfig()
			mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(tc.rows)

			dropped, err := sql.New(db, config).Teardown(false)
			require.NoError(t, err)
			require.False(t, dropped)
			require.NoError(t, mock.ExpectationsWereMet(), "no DROP, no note removal")
		})
	}
}

// keepData: the teardown runs in keep mode — the note is flipped to not
// owned so no later delete drops the table, and nothing is removed.
func TestSyncable_Teardown_KeepDisowns(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	mock.ExpectExec(dialect.SinkMetaDisownSQL()).WithArgs(config.Table).WillReturnResult(driver.ResultNoRows)

	dropped, err := sql.New(db, config).Teardown(true)
	require.NoError(t, err)
	require.False(t, dropped)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Init's half of the protocol: when the table was absent, CREATE is followed
// by a claim — the note says owned, rendered by this binary. (The mock
// answers "existed" unless told otherwise, so every other Init test pins an
// attach, which claims nothing.)
func TestSyncable_Init_ClaimsTheTableItCreated(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	dialect.CreatesTables = true
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	config.Mappings[0].JsonPath = "$.id" // Init compiles the mapping; teardown alone does not
	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(dialect.SinkMetaClaimSQL()).WithArgs(config.Table, int64(sql.SinkRenderingVersion)).WillReturnResult(driver.ResultNoRows)
	mock.ExpectPrepare(dialect.CreateGenerationUpsertSQL(config))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(config))
	mock.ExpectPrepare(dialect.CreateGenerationSweepSQL(config))

	require.NoError(t, sql.New(db, config).Init())
	require.NoError(t, mock.ExpectationsWereMet())
}

// A failed drop returns a typed, wrapped error (never panics) so the caller
// can log it and continue — the logical delete has already succeeded.
func TestSyncable_Teardown_WrapsError(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(noteRows(true))
	mock.ExpectExec(dialect.DropDDL(config)).WillReturnError(errors.New("permission denied"))

	dropped, err := syncable.Teardown(false)
	require.Error(t, err)
	require.False(t, dropped)
	require.Contains(t, err.Error(), "teardown")
	require.Contains(t, err.Error(), "permission denied")
}

// Idempotency: the first teardown drops the table and its note; the second
// finds no note, so it is a no-op at the database — not an error.
func TestSyncable_Teardown_Idempotent(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)

	config := teardownConfig()
	syncable := sql.New(db, config)

	mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(noteRows(true))
	mock.ExpectExec(dialect.DropDDL(config)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(dialect.SinkMetaDeleteSQL()).WithArgs(config.Table).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(sqlmock.NewRows([]string{"rendering_version", "owned"}))

	dropped, err := syncable.Teardown(false)
	require.NoError(t, err)
	require.True(t, dropped)
	dropped, err = syncable.Teardown(false)
	require.NoError(t, err)
	require.False(t, dropped)
	require.NoError(t, mock.ExpectationsWereMet())
}

// OwnsDestination is the rebuild verb's admission question: yes for a table
// committed created (the note says so) and for a table that does not exist
// yet (Init will create and claim it); no for a table committed attached to,
// whether it carries a not-owned note or none at all.
func TestSyncable_OwnsDestination(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    *sqlmock.Rows
		absent  bool // the mock answers "no such table"
		wantOwn bool
	}{
		{"owned note", noteRows(true), false, true},
		{"not-owned note, table exists", noteRows(false), false, false},
		{"no note, table exists", sqlmock.NewRows([]string{"rendering_version", "owned"}), false, false},
		{"no note, no table", sqlmock.NewRows([]string{"rendering_version", "owned"}), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialect, mock, err := testdialects.NewSQLMockDialect()
			require.NoError(t, err)
			dialect.CreatesTables = tc.absent
			db, err := sql.NewDB(dialect, "")
			require.NoError(t, err)
			config := teardownConfig()
			mock.ExpectQuery(dialect.SinkMetaSelectSQL()).WithArgs(config.Table).WillReturnRows(tc.rows)

			owns, err := sql.New(db, config).OwnsDestination(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.wantOwn, owns)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
