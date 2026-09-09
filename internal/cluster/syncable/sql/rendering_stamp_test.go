package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

// The stamp lives beside the rows: read on first contact, written on
// convergence, removed with the table. The mock dialect pins the statement
// sequence a worker start and a teardown drive.
func TestRenderingStamp_ReadWriteDelete(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	defer db.Close()
	sink := sql.New(db, &sql.Config{Topic: "t", Table: "events", PrimaryKey: []string{"id"}})
	ctx := context.Background()

	selectSQL := (&dialects.MySQLDialect{}).SinkMetaSelectSQL()
	upsertSQL := (&dialects.MySQLDialect{}).SinkMetaUpsertSQL()
	deleteSQL := (&dialects.MySQLDialect{}).SinkMetaDeleteSQL()

	// Never stamped: present=false, no error.
	mock.ExpectQuery(selectSQL).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"rendering_version"}))
	_, present, err := sink.RenderingStamp(ctx)
	require.NoError(t, err)
	require.False(t, present)

	// Stamp: the current version, keyed by the table name as configured.
	mock.ExpectExec(upsertSQL).WithArgs("events", int64(sql.SinkRenderingVersion)).WillReturnResult(driver.ResultNoRows)
	require.NoError(t, sink.StampRendering(ctx))
	require.Equal(t, sql.SinkRenderingVersion, sink.RenderingVersion())

	// Stamped: the recorded version comes back verbatim.
	mock.ExpectQuery(selectSQL).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"rendering_version"}).AddRow(int64(7)))
	version, present, err := sink.RenderingStamp(ctx)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(7), version)

	// Teardown drops the table (keyed: no applied sidecar) and the stamp with it.
	mock.ExpectExec((&dialects.MySQLDialect{}).DropDDL(&sql.Config{Table: "events"})).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(deleteSQL).WithArgs("events").WillReturnResult(driver.ResultNoRows)
	require.NoError(t, sink.Teardown())
	require.NoError(t, mock.ExpectationsWereMet())
}

// The production dialects' stamp statements are pinned verbatim: they are
// DDL and DML committed runs in the customer's database, and the golden
// destination dumps (sink_rendering_golden_test.go) depend on their shape.
func TestRenderingStamp_DialectStatements(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	require.Equal(t, `SELECT rendering_version FROM "committed__sink_meta" WHERE table_name = $1`, pg.SinkMetaSelectSQL())
	require.Equal(t, `INSERT INTO "committed__sink_meta" (table_name, rendering_version, materialized_at) VALUES ($1, $2, now()) ON CONFLICT (table_name) DO UPDATE SET rendering_version = EXCLUDED.rendering_version, materialized_at = now()`, pg.SinkMetaUpsertSQL())
	require.Equal(t, `DELETE FROM "committed__sink_meta" WHERE table_name = $1`, pg.SinkMetaDeleteSQL())

	my := &dialects.MySQLDialect{}
	require.Equal(t, "SELECT rendering_version FROM `committed__sink_meta` WHERE table_name = ?", my.SinkMetaSelectSQL())
	require.Equal(t, "INSERT INTO `committed__sink_meta` (table_name, rendering_version, materialized_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE rendering_version = VALUES(rendering_version), materialized_at = CURRENT_TIMESTAMP", my.SinkMetaUpsertSQL())
	require.Equal(t, "DELETE FROM `committed__sink_meta` WHERE table_name = ?", my.SinkMetaDeleteSQL())
}
