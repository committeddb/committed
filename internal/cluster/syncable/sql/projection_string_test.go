package sql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// String-function set entries bind their text results, a like predicate
// binds a boolean, and a null operand to || binds SQL NULL.
func TestProjectionStringFunctionsComputeColumns(t *testing.T) {
	config := &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      "customers",
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "BIGINT"},
			{Name: "display_name", SQLType: "VARCHAR(256)"},
			{Name: "code", SQLType: "VARCHAR(16)"},
			{Name: "is_service", SQLType: "BOOLEAN"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{
				{Column: "display_name", Expr: "$.first || ' ' || $.last"},
				{Column: "code", Expr: "upper(trim($.code))"},
				{Column: "is_service", Expr: "$.sku like 'SVC-%'"},
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
	sync("1", map[string]any{"id": 1, "first": "Ada", "last": "Lovelace", "code": " ab1 ", "sku": "SVC-1"},
		[]driver.Value{int64(1), "Ada Lovelace", "AB1", true})
	sync("2", map[string]any{"id": 2, "first": "Grace", "code": "x", "sku": "PRT-1"},
		[]driver.Value{int64(2), nil, "X", false})
	require.NoError(t, mock.ExpectationsWereMet())
}
