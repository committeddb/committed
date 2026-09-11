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
	stampSQL := (&dialects.MySQLDialect{}).SinkMetaStampSQL()
	deleteSQL := (&dialects.MySQLDialect{}).SinkMetaDeleteSQL()

	// Never stamped: present=false, no error.
	mock.ExpectQuery(selectSQL).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"rendering_version", "owned"}))
	_, present, err := sink.RenderingStamp(ctx)
	require.NoError(t, err)
	require.False(t, present)

	// Stamp: the current version, keyed by the table name as configured.
	mock.ExpectExec(stampSQL).WithArgs("events", int64(sql.SinkRenderingVersion)).WillReturnResult(driver.ResultNoRows)
	require.NoError(t, sink.StampRendering(ctx))
	require.Equal(t, sql.SinkRenderingVersion, sink.RenderingVersion())

	// Stamped: the recorded version comes back verbatim.
	mock.ExpectQuery(selectSQL).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"rendering_version", "owned"}).AddRow(int64(7), true))
	version, present, err := sink.RenderingStamp(ctx)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(7), version)

	// Teardown of an owned table drops it (keyed: no applied sidecar) and the
	// note with it.
	mock.ExpectQuery(selectSQL).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"rendering_version", "owned"}).AddRow(int64(7), true))
	mock.ExpectExec((&dialects.MySQLDialect{}).DropDDL(&sql.Config{Table: "events"})).WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec(deleteSQL).WithArgs("events").WillReturnResult(driver.ResultNoRows)
	dropped, err := sink.Teardown(false)
	require.NoError(t, err)
	require.True(t, dropped)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The production dialects' note statements are pinned verbatim: they are
// DDL and DML committed runs in the customer's database, and the reference
// destination dumps (sink_rendering_reference_test.go) depend on their shape.
// A stamp inserts not-owned and never updates ownership; a claim sets it.
func TestRenderingStamp_DialectStatements(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	require.Equal(t, `SELECT rendering_version, owned FROM "committed__sink_meta" WHERE table_name = $1`, pg.SinkMetaSelectSQL())
	require.Equal(t, `INSERT INTO "committed__sink_meta" (table_name, rendering_version, owned, materialized_at) VALUES ($1, $2, false, now()) ON CONFLICT (table_name) DO UPDATE SET rendering_version = EXCLUDED.rendering_version, materialized_at = now()`, pg.SinkMetaStampSQL())
	require.Equal(t, `INSERT INTO "committed__sink_meta" (table_name, rendering_version, owned, materialized_at) VALUES ($1, $2, true, now()) ON CONFLICT (table_name) DO UPDATE SET rendering_version = EXCLUDED.rendering_version, owned = true, materialized_at = now()`, pg.SinkMetaClaimSQL())
	require.Equal(t, `UPDATE "committed__sink_meta" SET owned = false WHERE table_name = $1`, pg.SinkMetaDisownSQL())
	require.Equal(t, `DELETE FROM "committed__sink_meta" WHERE table_name = $1`, pg.SinkMetaDeleteSQL())

	my := &dialects.MySQLDialect{}
	require.Equal(t, "SELECT rendering_version, owned FROM `committed__sink_meta` WHERE table_name = ?", my.SinkMetaSelectSQL())
	require.Equal(t, "INSERT INTO `committed__sink_meta` (table_name, rendering_version, owned, materialized_at) VALUES (?, ?, FALSE, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE rendering_version = VALUES(rendering_version), materialized_at = CURRENT_TIMESTAMP", my.SinkMetaStampSQL())
	require.Equal(t, "INSERT INTO `committed__sink_meta` (table_name, rendering_version, owned, materialized_at) VALUES (?, ?, TRUE, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE rendering_version = VALUES(rendering_version), owned = TRUE, materialized_at = CURRENT_TIMESTAMP", my.SinkMetaClaimSQL())
	require.Equal(t, "UPDATE `committed__sink_meta` SET owned = FALSE WHERE table_name = ?", my.SinkMetaDisownSQL())
	require.Equal(t, "DELETE FROM `committed__sink_meta` WHERE table_name = ?", my.SinkMetaDeleteSQL())
}
