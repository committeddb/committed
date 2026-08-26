//go:build docker || integration

package dialects_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// projectionScalarAggregatesFlow drives scalar aggregate columns against a
// real destination: count, decimal-exact sum, filtered count, and a lexical
// max over ISO dates — all recomputed from the sidecar on every child change
// (upsert and delete). The JSON extraction + casts are engine-specific SQL,
// which is exactly where a mock could lie; this runs them for real.
func projectionScalarAggregatesFlow(t *testing.T, db *sql.DB, table, quoteL, quoteR string, placeholder func(i int) string) {
	visitType := &cluster.Type{ID: "visit-" + table, Name: "Visit"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(32)"},
			{Name: "n", SQLType: "INT"},
			{Name: "hours_sum", SQLType: "DECIMAL(12,2)"},
			{Name: "done_count", SQLType: "INT"},
			{Name: "last_date", SQLType: "VARCHAR(32)"},
			{Name: "live_count", SQLType: "INT"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:    visitType.ID,
			KeyPath:  []string{"$.job_id"},
			OnDelete: "remove-from-aggregate",
			Aggregate: &sql.ProjectionAggregate{
				ElementKey: "$.id",
				Element: []sql.ProjectionElementField{
					{Field: "hours", From: "$.hours"},
					{Field: "status", From: "$.status"},
					{Field: "date", From: "$.date"},
					{Field: "deleted_at", From: "$.deletedAt"},
				},
				Scalars: []sql.ProjectionScalar{
					{Column: "n", Fn: "count"},
					{Column: "hours_sum", Fn: "sum", Of: "hours"},
					{Column: "done_count", Fn: "count", Where: []sql.ScalarWhere{{Field: "status", Equals: "done"}}},
					{Column: "last_date", Fn: "max", Of: "date"},
					{Column: "live_count", Fn: "count", Where: []sql.ScalarWhere{{Field: "deleted_at", Null: true}}},
				},
			},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	upsert := func(id, hours, status, date string) {
		// v1 and v2 carry explicit JSON nulls (mirrored source rows are
		// canonicalized with every column present); v3 carries a value —
		// the null-where filter counts the nulls and excludes the value.
		field := `,"deletedAt":null`
		if id == "v3" {
			field = `,"deletedAt":"2026-08-10"`
		}
		payload := fmt.Sprintf(`{"job_id":"j1","id":"%s","hours":%s,"status":"%s","date":"%s"%s}`, id, hours, status, date, field)
		a := &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(visitType, []byte(id), []byte(payload)),
		}}
		_, err := p.Sync(context.Background(), a)
		require.NoError(t, err)
	}
	upsert("v1", "2.5", "done", "2026-08-01")
	upsert("v2", "1.25", "open", "2026-08-15")
	upsert("v3", "0.25", "done", "2026-08-03")

	qt := quoteL + table + quoteR
	read := func() (count, done, live int, sum, last string) {
		require.NoError(t, db.DB.QueryRow(fmt.Sprintf(
			"SELECT n, done_count, live_count, hours_sum, last_date FROM %s WHERE job_id = %s",
			qt, placeholder(0)), "j1").Scan(&count, &done, &live, &sum, &last))
		return
	}
	count, done, live, sum, last := read()
	require.Equal(t, 3, count)
	require.Equal(t, 2, done, "filtered count folds only matching children")
	require.Equal(t, 2, live, "null-where counts JSON nulls, excludes values")
	require.Equal(t, "4.00", sum, "decimal sum is cent-exact at the column's scale")
	require.Equal(t, "2026-08-15", last, "lexical max orders ISO dates correctly")

	// A child delete recomputes every scalar from the surviving set.
	del := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(visitType, []byte("v2")),
	}}
	_, err := p.Sync(context.Background(), del)
	require.NoError(t, err)

	count, done, live, sum, last = read()
	require.Equal(t, 2, count)
	require.Equal(t, 2, done)
	require.Equal(t, 1, live, "one null-deletedAt child survived the delete")
	require.Equal(t, "2.75", sum)
	require.Equal(t, "2026-08-03", last)
}

func TestPostgreSQLIntegration_ProjScalars(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)
	defer dropTable(t, table+"__n") // the scalars-only sidecar

	projectionScalarAggregatesFlow(t, db, table, `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_ProjScalars(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)
	defer dropTableMySQL(t, db, table+"__n") // the scalars-only sidecar

	projectionScalarAggregatesFlow(t, db, table, "`", "`",
		func(int) string { return "?" })
}
