package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// A CASE set entry computes its column per event: a string branch binds
// as text, a numeric branch through the exact-decimal path, and an
// unmatched CASE with no ELSE binds SQL NULL. The guarded division in the
// unselected branch never dead-letters the zero-divisor event.
func TestProjectionCaseComputesColumn(t *testing.T) {
	config := &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      "jobs",
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "BIGINT"},
			{Name: "band", SQLType: "VARCHAR(16)"},
			{Name: "rate", SQLType: "DECIMAL(12,2)"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{
				{Column: "band", Expr: "case when $.hours < 8 then 'small' when $.hours < 40 then 'medium' end"},
				{Column: "rate", Expr: "case when $.hours = 0 then null else round($.amount / $.hours, 2) end"},
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
	sync("1", map[string]any{"id": 1, "hours": 4, "amount": 10}, []driver.Value{int64(1), "small", "2.5"})
	sync("2", map[string]any{"id": 2, "hours": 0, "amount": 10}, []driver.Value{int64(2), "small", nil})
	sync("3", map[string]any{"id": 3, "hours": 50, "amount": 100}, []driver.Value{int64(3), nil, int64(2)})
	require.NoError(t, mock.ExpectationsWereMet())
}
