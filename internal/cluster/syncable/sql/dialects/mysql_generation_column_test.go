package dialects_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// TestMySQLDialect_EnsureGenerationColumn_SchemaQualifiedIntrospection pins the
// idempotency fix for a SCHEMA-QUALIFIED sink (`db.tbl`). EnsureGenerationColumn
// checks information_schema before adding the committed-managed generation
// column. The bug: it bound the WHOLE "db.tbl" to table_name, which never
// matches a real row (information_schema.table_name holds only "tbl"), so the
// count was always 0 and every Init re-ran the ALTER. The first Init added the
// column; a second Init (node restart / syncable re-register) then failed with
// duplicate-column (1060), which wedged the keyed syncable's Init forever.
//
// The fix splits the qualifier off and binds table_schema and table_name
// separately. We assert that exact introspection shape and args here — when the
// column already exists (count 1), EnsureGenerationColumn must return without
// issuing any ALTER, i.e. the idempotent second-Init path. sqlmock's
// QueryMatcherEqual fails the whole call if the emitted query differs, so the
// pre-fix DATABASE()/whole-name form does not satisfy this expectation.
func TestMySQLDialect_EnsureGenerationColumn_SchemaQualifiedIntrospection(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	config := &sql.Config{
		Table:      "otherdb.widget",
		PrimaryKey: "wid",
		Mappings: []sql.Mapping{
			{Column: "wid", SQLType: "VARCHAR(64)"},
			{Column: "name", SQLType: "VARCHAR(255)"},
		},
	}

	// Schema-qualified introspection: table_schema and table_name bound apart,
	// column already present → return early, no ALTER.
	mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?").
		WithArgs("otherdb", "widget", sql.GenerationColumn).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	require.NoError(t, (&dialects.MySQLDialect{}).EnsureGenerationColumn(db, config))
	require.NoError(t, mock.ExpectationsWereMet(), "no ALTER should be issued when the column already exists")
}

// TestMySQLDialect_EnsureGenerationColumn_SchemaQualifiedAddsColumn covers the
// first-Init path for a schema-qualified sink: the column is absent (count 0),
// so EnsureGenerationColumn introspects with the split schema/table and then
// ALTERs the fully-qualified table to add the column.
func TestMySQLDialect_EnsureGenerationColumn_SchemaQualifiedAddsColumn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	config := &sql.Config{Table: "otherdb.widget", PrimaryKey: "wid"}

	mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?").
		WithArgs("otherdb", "widget", sql.GenerationColumn).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
	mock.ExpectExec("ALTER TABLE otherdb.widget ADD COLUMN " + sql.GenerationColumn + " BIGINT NOT NULL DEFAULT 1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, (&dialects.MySQLDialect{}).EnsureGenerationColumn(db, config))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestMySQLDialect_EnsureGenerationColumn_UnqualifiedIntrospection is the
// regression guard for the common UNQUALIFIED sink (`tbl`, resolved against the
// connection's default database): the fix must leave that path untouched —
// table_schema = DATABASE() with the bare name bound to table_name.
func TestMySQLDialect_EnsureGenerationColumn_UnqualifiedIntrospection(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	config := &sql.Config{Table: "widget", PrimaryKey: "wid"}

	mock.ExpectQuery("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?").
		WithArgs("widget", sql.GenerationColumn).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	require.NoError(t, (&dialects.MySQLDialect{}).EnsureGenerationColumn(db, config))
	require.NoError(t, mock.ExpectationsWereMet())
}
