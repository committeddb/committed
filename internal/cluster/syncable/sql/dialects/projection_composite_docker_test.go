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

// projectionCompositeFlow drives a composite-keyed sql-projection against a
// real destination — the VisitWorkareaStatuses shape from the pilot (a
// latest-event-wins reduction keyed by a composite identity, previously only
// expressible as a window function). Init creates the real two-column
// PRIMARY KEY, each rule upsert converges on the column PAIR via the
// defaulted per-column keyPaths, and a payload-less composite tombstone —
// the cluster.CompositeKey encoding a composite-keyed ingest produces —
// removes exactly the one addressed row.
func projectionCompositeFlow(t *testing.T, db *sql.DB, table, quoteL, quoteR string, placeholder func(i int) string) {
	cfg := &sql.ProjectionConfig{
		Topic:      eventType.ID,
		Table:      table,
		PrimaryKey: []string{"visit_id", "workarea_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "visit_id", SQLType: "VARCHAR(32)"},
			{Name: "workarea_id", SQLType: "VARCHAR(32)"},
			{Name: "status", SQLType: "VARCHAR(32)"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{{Column: "status", From: "$.status"}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	require.NoError(t, p.Init(), "composite projection DDL must be valid SQL")
	defer p.Close()

	key := func(visit, workarea string) string {
		return cluster.CompositeKey(
			map[string]any{"visit_id": visit, "workarea_id": workarea},
			cfg.PrimaryKey)
	}
	up := func(visit, workarea, status string) *cluster.Actual {
		return &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(eventType, []byte(key(visit, workarea)),
				[]byte(fmt.Sprintf(`{"visit_id":%q,"workarea_id":%q,"status":%q}`, visit, workarea, status))),
		}}
	}

	// Three logical rows; (v1,w1) written twice must converge to the LATEST
	// status — the latest-event-wins reduction on the composite identity.
	for _, a := range []*cluster.Actual{
		up("v1", "w1", "started"), up("v1", "w2", "sibling"), up("v2", "w1", "other-visit"), up("v1", "w1", "done"),
	} {
		_, err := p.Sync(context.Background(), a)
		require.NoError(t, err)
	}

	qt := quoteL + table + quoteR
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&count))
	require.Equal(t, 3, count, "the fold must key on the column PAIR, not either column alone")

	var status string
	require.NoError(t, db.DB.QueryRow(
		fmt.Sprintf("SELECT status FROM %s WHERE visit_id = %s AND workarea_id = %s", qt, placeholder(0), placeholder(1)),
		"v1", "w1").Scan(&status))
	require.Equal(t, "done", status, "latest event wins on the composite identity")

	// The composite tombstone must remove exactly (v1,w1): the sibling
	// workarea of v1 and the same workarea of v2 both survive.
	del := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(eventType, []byte(key("v1", "w1"))),
	}}
	_, err := p.Sync(context.Background(), del)
	require.NoError(t, err)

	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&count))
	require.Equal(t, 2, count, "the tombstone must remove exactly the one addressed row")
	require.NoError(t, db.DB.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE visit_id = %s", qt, placeholder(0)), "v1").Scan(&count))
	require.Equal(t, 1, count, "the sibling row sharing visit_id must survive")
}

func TestPostgreSQLIntegration_ProjectionCompositePrimaryKey(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	projectionCompositeFlow(t, db, table, `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_ProjectionCompositePrimaryKey(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	projectionCompositeFlow(t, db, table, "`", "`",
		func(int) string { return "?" })
}
