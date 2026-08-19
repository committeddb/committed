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

// projectionForEachFlow drives the fan-out engine against a real
// destination with an INTEGER primary key — the risk case for the
// reconciliation path, whose row deletes bind the sidecar's stored TEXT
// child key against the typed key column. One event fans N rows, a
// re-emitted parent reconciles a vanished element away, and a parent
// tombstone cascades.
func projectionForEachFlow(t *testing.T, db *sql.DB, table, quoteL, quoteR string, placeholder func(i int) string) {
	txnType := &cluster.Type{ID: "txn", Name: "Txn"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"element_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "element_id", SQLType: "BIGINT"},
			{Name: "amount", SQLType: "DECIMAL(12,2)"},
			{Name: "txn_id", SQLType: "VARCHAR(32)"},
		},
		Sources: []sql.ProjectionSource{{
			Topic:   txnType.ID,
			KeyPath: []string{"$.id"},
			ForEach: "$.items[*]",
			Rules: []sql.ProjectionRule{{
				Set: []sql.ProjectionSet{
					{Column: "amount", From: "$.amount"},
					{Column: "txn_id", From: "$parent.txn"},
				},
			}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	upsert := func(payload string) {
		t.Helper()
		a := &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(txnType, []byte("p1"), []byte(payload)),
		}}
		_, err := p.Sync(context.Background(), a)
		require.NoError(t, err)
	}
	qt := quoteL + table + quoteR
	count := func() (n int) {
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&n))
		return
	}

	upsert(`{"txn":"p1","items":[{"id":101,"amount":"2.50"},{"id":102,"amount":"1.25"}]}`)
	require.Equal(t, 2, count(), "one event fans one row per element")
	var amount, txn string
	require.NoError(t, db.DB.QueryRow(
		fmt.Sprintf("SELECT amount, txn_id FROM %s WHERE element_id = %s", qt, placeholder(0)),
		102).Scan(&amount, &txn))
	require.Equal(t, "1.25", amount)
	require.Equal(t, "p1", txn, "$parent paths reach the enclosing event")

	// Reconcile: element 102 vanished from the re-emitted parent.
	upsert(`{"txn":"p1","items":[{"id":101,"amount":"3.00"}]}`)
	require.Equal(t, 1, count(), "the vanished element's row reconciles away")

	// Cascade: the parent tombstone removes every fanned row.
	del := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(txnType, []byte("p1")),
	}}
	_, err := p.Sync(context.Background(), del)
	require.NoError(t, err)
	require.Equal(t, 0, count(), "the parent tombstone cascades to all fanned rows")
}

func TestPostgreSQLIntegration_ProjForEach(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)
	defer dropTable(t, sql.ForEachSidecarName(table, "txn"))

	projectionForEachFlow(t, db, table, `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_ProjForEach(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)
	defer dropTableMySQL(t, db, sql.ForEachSidecarName(table, "txn"))

	projectionForEachFlow(t, db, table, "`", "`",
		func(int) string { return "?" })
}
