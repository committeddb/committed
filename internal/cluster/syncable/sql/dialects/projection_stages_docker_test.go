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

// projectionStagesFlow drives the stage engine against a real destination:
// topic entities fold through a reshape→aggregate chain in the stage
// store, and the stage-fed table source lands the aggregate's deltas as
// rows — exact decimal sums, refold on delete, and full retraction when a
// key's last input vanishes.
func projectionStagesFlow(t *testing.T, db *sql.DB, table, quoteL, quoteR string, placeholder func(i int) string) {
	txnType := &cluster.Type{ID: "txns", Name: "Txn"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(64)"},
			{Name: "total", SQLType: "DECIMAL(12,2)"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "live", From: "txns", KeyPath: []string{"$.id"},
				Emit: []sql.StageEmit{{Field: "job", From: "$.jobId"}, {Field: "amt", From: "$.amount"}},
			},
			{
				Name: "by-job", From: "live", KeyPath: []string{"$.job"},
				Reduce: "aggregate", Emit: []sql.StageEmit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
			},
		},
		Sources: []sql.ProjectionSource{{
			FromStage: "by-job",
			KeyPath:   []string{"$.job"},
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "total", From: "$.total"},
				{Column: "n", From: "$.n"},
			}}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	var idx uint64
	sync := func(e *cluster.Entity) {
		t.Helper()
		idx++
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{e}})
		require.NoError(t, err)
	}
	qt := quoteL + table + quoteR
	row := func(job string) (total string, n, count int) {
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&count))
		err := db.DB.QueryRow(
			fmt.Sprintf("SELECT total, n FROM %s WHERE job_id = %s", qt, placeholder(0)), job).
			Scan(&total, &n)
		if err != nil {
			return "", 0, count
		}
		return total, n, count
	}

	sync(cluster.NewUpsertEntity(txnType, []byte("t1"), []byte(`{"id":"t1","jobId":"j1","amount":2.5}`)))
	sync(cluster.NewUpsertEntity(txnType, []byte("t2"), []byte(`{"id":"t2","jobId":"j1","amount":1.25}`)))
	total, n, count := row("j1")
	require.Equal(t, "3.75", total, "exact decimal sum through the chain")
	require.Equal(t, 2, n)
	require.Equal(t, 1, count)

	// Rekey: t2 moves to j2 — both jobs refold.
	sync(cluster.NewUpsertEntity(txnType, []byte("t2"), []byte(`{"id":"t2","jobId":"j2","amount":1.25}`)))
	total, n, count = row("j1")
	require.Equal(t, "2.50", total)
	require.Equal(t, 1, n)
	require.Equal(t, 2, count)

	// Full retraction: deleting j1's last txn removes its ROW.
	sync(cluster.NewDeleteEntity(txnType, []byte("t1")))
	_, _, count = row("j1")
	require.Equal(t, 1, count, "the emptied key's row retracts; j2 survives")
}

func TestPostgreSQLIntegration_ProjStages(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	projectionStagesFlow(t, db, table, `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_ProjStages(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	projectionStagesFlow(t, db, table, "`", "`",
		func(int) string { return "?" })
}
