package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// The invoice acceptance columns: a due date 30 days after creation, a
// whole-day count between two timestamps from different sources'
// renderings, and the quarter a payment fell in. Timestamps bind as their
// canonical text (+00:00, which every SQL destination accepts).
func TestProjectionTimeComputesColumns(t *testing.T) {
	config := &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      "invoices",
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "BIGINT"},
			{Name: "due", SQLType: "TIMESTAMPTZ"},
			{Name: "days_open", SQLType: "INT"},
			{Name: "quarter", SQLType: "VARCHAR(8)"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{
				{Column: "due", Expr: "cast($.created as timestamptz) + interval '30 days'"},
				{Column: "days_open", Expr: "trunc(extract(epoch from (cast($.closed as timestamptz) - cast($.created as timestamptz))) / 86400)"},
				{Column: "quarter", Expr: "extract(year from $.created::date) || '-Q' || extract(quarter from $.created::date)"},
			},
		}},
	}
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	sync := func(key string, payload map[string]any, want []driver.Value) {
		t.Helper()
		mock.ExpectBegin()
		rules[0].ExpectExec().WithArgs(append(want, want...)...).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_, err := projection.Sync(context.Background(), eventActual(t, key, payload))
		require.NoError(t, err)
	}
	// created from a PG-style rendering, closed from SQL Server's.
	sync("1", map[string]any{"id": 1, "created": "2024-01-15 07:00:45.123456+00", "closed": "2024-02-20T06:00:00.1234567Z"},
		[]driver.Value{int64(1), "2024-02-14 07:00:45.123456+00:00", int64(35), "2024-Q1"})
	// Not yet closed: the arithmetic over a null is null.
	sync("2", map[string]any{"id": 2, "created": "2024-11-30T23:30:00-05:00"},
		[]driver.Value{int64(2), "2024-12-31 04:30:00+00:00", nil, "2024-Q4"})
	require.NoError(t, mock.ExpectationsWereMet())
}
