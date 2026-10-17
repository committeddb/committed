//go:build docker || integration

package dialects_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// projectionBatchWritesFlow runs one random history of inserts, changes,
// moves between keys and deletes through a stage-fed projection twice — one
// entry at a time, and in batches of random size, whose rows go out as
// multi-row upserts — and requires the two tables to be equal.
func projectionBatchWritesFlow(t *testing.T, db *sql.DB, perEntryTable, batchedTable, quoteL, quoteR string) {
	txnType := &cluster.Type{ID: "txns", Name: "Txn"}
	config := func(table string) *sql.ProjectionConfig {
		return &sql.ProjectionConfig{
			Table:      table,
			PrimaryKey: []string{"job_id"},
			Columns: []sql.ProjectionColumn{
				{Name: "job_id", SQLType: "VARCHAR(64)"},
				{Name: "total", SQLType: "DECIMAL(12,2)"},
				{Name: "n", SQLType: "INT"},
			},
			Stages: []sql.ProjectionStage{{
				Name: "by-job", From: "txns", KeyPath: []string{"$.jobId"},
				Reduce: "aggregate", Emit: []sql.StageEmit{{Field: "total", Sum: "$.amount"}, {Field: "n", Count: true}},
			}},
			Sources: []sql.ProjectionSource{{
				FromStage: "by-job",
				KeyPath:   []string{"$.job"},
				Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
					{Column: "total", From: "$.total"},
					{Column: "n", From: "$.n"},
				}}},
			}},
		}
	}
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // G404: a reproducible test history
	var history []*cluster.Actual
	for i := 1; i <= 1500; i++ {
		id := fmt.Sprintf("t%d", rng.Intn(200))
		var e *cluster.Entity
		if rng.Intn(5) == 0 {
			e = cluster.NewDeleteEntity(txnType, []byte(id))
		} else {
			e = cluster.NewUpsertEntity(txnType, []byte(id),
				fmt.Appendf(nil, `{"id":%q,"jobId":"j%d","amount":%d.25}`, id, rng.Intn(40), rng.Intn(100)))
		}
		history = append(history, &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{e}}) //nolint:gosec // G115: i is positive
	}
	ctx := context.Background()
	open := func(table string) *sql.Projection {
		p := sql.NewProjection(db, config(table), nil, table)
		p.SetStoreDir(t.TempDir())
		require.NoError(t, p.Init())
		t.Cleanup(func() { _ = p.Close() })
		return p
	}

	perEntry := open(perEntryTable)
	for _, a := range history {
		_, err := perEntry.Sync(ctx, a)
		require.NoError(t, err)
	}
	batched := open(batchedTable)
	for rest := history; len(rest) > 0; {
		n := min(len(rest), 1+rng.Intn(500))
		ok, err := batched.SyncBatch(ctx, rest[:n])
		require.NoError(t, err)
		require.True(t, ok)
		rest = rest[n:]
	}

	read := func(table string) map[string]string {
		rows, err := db.DB.Query("SELECT job_id, total, n FROM " + quoteL + table + quoteR)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		out := map[string]string{}
		for rows.Next() {
			var job, total string
			var n int
			require.NoError(t, rows.Scan(&job, &total, &n))
			out[job] = total + "/" + fmt.Sprint(n)
		}
		require.NoError(t, rows.Err())
		return out
	}
	want := read(perEntryTable)
	require.NotEmpty(t, want)
	require.Equal(t, want, read(batchedTable))
}

func TestPostgreSQLIntegration_ProjBatchWrites(t *testing.T) {
	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.NoError(t, err)
	defer db.Close()
	perEntry, batched := uniqueTable(t), uniqueTable(t)
	defer dropTable(t, perEntry)
	defer dropTable(t, batched)
	projectionBatchWritesFlow(t, db, perEntry, batched, `"`, `"`)
}

func TestMySQLIntegration_ProjBatchWrites(t *testing.T) {
	db, err := sql.NewDB(&dialects.MySQLDialect{}, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()
	perEntry, batched := uniqueTable(t), uniqueTable(t)
	defer dropTableMySQL(t, db, perEntry)
	defer dropTableMySQL(t, db, batched)
	projectionBatchWritesFlow(t, db, perEntry, batched, "`", "`")
}
