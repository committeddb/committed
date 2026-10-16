package sql_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

func batchWritesConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Table:      "job_totals",
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(64)"},
			{Name: "total", SQLType: "BIGINT"},
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

func readJobTotals(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.DB.Query("SELECT job_id, total, n FROM job_totals")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var job string
		var total, n int64
		require.NoError(t, rows.Scan(&job, &total, &n))
		out[job] = fmt.Sprintf("%d/%d", total, n)
	}
	require.NoError(t, rows.Err())
	return out
}

// A stage-fed projection's batch path, which collects each batch's rows and
// writes them as multi-row upserts, lands exactly the table the one-entry-at-
// a-time path does: the same random history of inserts, changes, moves
// between keys and deletes, cut into batches of random size, against a real
// SQL engine.
func TestProjectionBatchWritesMatchPerEntry(t *testing.T) {
	txnType := &cluster.Type{ID: "txns", Name: "Txn"}
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // G404: a reproducible test history
	var history []*cluster.Actual
	for i := 1; i <= 600; i++ {
		id := fmt.Sprintf("t%d", rng.Intn(80))
		var e *cluster.Entity
		if rng.Intn(5) == 0 {
			e = cluster.NewDeleteEntity(txnType, []byte(id))
		} else {
			e = cluster.NewUpsertEntity(txnType, []byte(id),
				fmt.Appendf(nil, `{"id":%q,"jobId":"j%d","amount":%d}`, id, rng.Intn(20), rng.Intn(100)))
		}
		history = append(history, &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{e}}) //nolint:gosec // G115: i is positive
	}
	ctx := context.Background()

	perEntryDB := newGoMySQLServerDB(t, "batchwrites_single")
	defer perEntryDB.Close()
	perEntry := sql.NewProjection(perEntryDB, batchWritesConfig(), nil, "job_totals")
	perEntry.SetStoreDir(t.TempDir())
	require.NoError(t, perEntry.Init())
	t.Cleanup(func() { _ = perEntry.Close() })
	for _, a := range history {
		_, err := perEntry.Sync(ctx, a)
		require.NoError(t, err)
	}

	batchedDB := newGoMySQLServerDB(t, "batchwrites_batched")
	defer batchedDB.Close()
	batched := sql.NewProjection(batchedDB, batchWritesConfig(), nil, "job_totals")
	batched.SetStoreDir(t.TempDir())
	require.NoError(t, batched.Init())
	t.Cleanup(func() { _ = batched.Close() })
	for rest := history; len(rest) > 0; {
		n := min(len(rest), 1+rng.Intn(60))
		ok, err := batched.SyncBatch(ctx, rest[:n])
		require.NoError(t, err)
		require.True(t, ok)
		rest = rest[n:]
	}

	want := readJobTotals(t, perEntryDB)
	require.NotEmpty(t, want)
	require.Equal(t, want, readJobTotals(t, batchedDB))
}
