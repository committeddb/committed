package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// A producer that stringifies numbers: cast lets them take part in
// arithmetic and clamping, try_cast skips an unparsable value as null
// instead of failing the event, and an integer cast rounds half away from
// zero.
func TestProjectionCastComputesColumns(t *testing.T) {
	config := &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      "lines",
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "BIGINT"},
			{Name: "qty", SQLType: "DECIMAL(12,2)"},
			{Name: "units", SQLType: "BIGINT"},
			{Name: "discount", SQLType: "DECIMAL(12,2)"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{
				{Column: "qty", Expr: "greatest(cast($.qty as numeric), 0)"},
				{Column: "units", Expr: "$.qty::numeric::integer"},
				{Column: "discount", Expr: "try_cast($.discount as numeric)"},
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
	sync("1", map[string]any{"id": 1, "qty": "2.5000", "discount": "0.10"},
		[]driver.Value{int64(1), "2.5", int64(3), "0.1"})
	sync("2", map[string]any{"id": 2, "qty": "-4", "discount": "n/a"},
		[]driver.Value{int64(2), int64(0), int64(-4), nil})
	require.NoError(t, mock.ExpectationsWereMet())
}
