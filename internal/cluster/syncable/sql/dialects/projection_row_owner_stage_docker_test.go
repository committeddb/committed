//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// projectionRowOwnerStageFlow drives a table fed by BOTH a row-owning
// topic source (row admission) and a stage-fed decorating source (one
// column) against a real destination — the multi-source shape the pilot's
// jobs_to_invoice uses. It pins the ownership contract: the decorator's
// column lands regardless of arrival order, and an owner retract/re-assert
// cycle gets its decorations back (the stage store is the retention).
func projectionRowOwnerStageFlow(t *testing.T, db *sql.DB, table, quoteL, quoteR string, placeholder func(i int) string) {
	jobType := &cluster.Type{ID: "jobs", Name: "Job"}
	propType := &cluster.Type{ID: "proposals", Name: "Proposal"}
	custType := &cluster.Type{ID: "customers", Name: "Customer"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(64)"},
			{Name: "name", SQLType: "VARCHAR(64)"},
			{Name: "latest", SQLType: "VARCHAR(64)"},
			{Name: "cust", SQLType: "VARCHAR(64)"},
			{Name: "cust_name", SQLType: "VARCHAR(64)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "latest-prop", From: "proposals", KeyPath: []string{"$.jobId"},
			Emit: []sql.StageEmit{
				{Field: "job", From: "$.jobId"},
				{Field: "pid", From: "$.id"},
				{Field: "status", From: "$.status"},
				{Field: "custId", From: "$.custId"},
			},
		}},
		Sources: []sql.ProjectionSource{
			{
				Topic:    "jobs",
				KeyPath:  []string{"$.id"},
				RowOwner: true,
				Rules:    []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "name", From: "$.name"}}}},
			},
			{
				FromStage: "latest-prop",
				When:      []sql.WhenClause{{Path: "$.status", Equals: "active"}},
				Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
					{Column: "latest", From: "$.pid"},
					{Column: "cust", From: "$.custId"},
					{Column: "cust_name", Lookup: "cust_dim", On: "cust", Select: "name"},
				}}},
			},
			{
				Topic: "customers", KeyPath: []string{"$.id"},
				Lookup: &sql.ProjectionLookup{
					Name:   "cust_dim",
					Fields: []sql.ProjectionElementField{{Field: "name", From: "$.name"}},
				},
			},
		},
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
	count := func() (n int) {
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&n))
		return n
	}
	row := func(job string) (name, latest gosql.NullString) {
		t.Helper()
		err := db.DB.QueryRow(
			fmt.Sprintf("SELECT name, latest FROM %s WHERE job_id = %s", qt, placeholder(0)), job).
			Scan(&name, &latest)
		require.NoError(t, err)
		return name, latest
	}

	// Forward order: the owner admits j1, then its proposal arrives.
	sync(cluster.NewUpsertEntity(jobType, []byte("j1"), []byte(`{"id":"j1","name":"A"}`)))
	sync(cluster.NewUpsertEntity(propType, []byte("p1"), []byte(`{"id":"p1","jobId":"j1","status":"active"}`)))
	name, latest := row("j1")
	require.Equal(t, "A", name.String, "owner column")
	require.True(t, latest.Valid, "decoration lands on the owner-created row")
	require.Equal(t, "p1", latest.String)

	// Owner retract / re-assert: deleting and re-asserting j1 must get its
	// decoration BACK — the stage retains it; a row delete cannot silently
	// strip a column forever (delta suppression makes the stage stay quiet).
	sync(cluster.NewDeleteEntity(jobType, []byte("j1")))
	require.Equal(t, 0, count(), "owner delete removes the row")
	sync(cluster.NewUpsertEntity(jobType, []byte("j1"), []byte(`{"id":"j1","name":"A2"}`)))
	name, latest = row("j1")
	require.Equal(t, "A2", name.String)
	require.True(t, latest.Valid, "re-asserted row recovers its retained decoration")
	require.Equal(t, "p1", latest.String)

	// When-flip retraction: p1 stops matching the decorator's when — the
	// decoration retracts (clears) while the owner's row survives.
	sync(cluster.NewUpsertEntity(propType, []byte("p1"), []byte(`{"id":"p1","jobId":"j1","status":"void"}`)))
	name, latest = row("j1")
	require.Equal(t, "A2", name.String, "owner row survives a decoration retraction")
	require.False(t, latest.Valid, "a live delta that stops matching the when RETRACTS the decoration")

	// Flip back: re-admission re-lands it.
	sync(cluster.NewUpsertEntity(propType, []byte("p1"), []byte(`{"id":"p1","jobId":"j1","status":"active"}`)))
	_, latest = row("j1")
	require.Equal(t, "p1", latest.String, "re-admitted decoration re-lands")

	// Stage retraction (source delete): the decoration clears, row survives.
	sync(cluster.NewDeleteEntity(propType, []byte("p1")))
	name, latest = row("j1")
	require.Equal(t, "A2", name.String)
	require.False(t, latest.Valid, "stage retraction clears the decoration, not the row")

	// Reverse order: j2's proposal arrives BEFORE the owner admits j2 —
	// no row appears (the owner owns row existence)...
	sync(cluster.NewUpsertEntity(propType, []byte("p2"), []byte(`{"id":"p2","jobId":"j2","status":"active"}`)))
	require.Equal(t, 1, count(), "a decorator never creates a row the owner has not admitted")
	// ...and the owner's admission pulls the retained decoration.
	sync(cluster.NewUpsertEntity(jobType, []byte("j2"), []byte(`{"id":"j2","name":"B"}`)))
	name, latest = row("j2")
	require.Equal(t, "B", name.String)
	require.Equal(t, "p2", latest.String, "owner admission pulls the retained decoration")

	// Decorated-FK enrichment (the pilot's customer_display_name shape):
	// the FK arrives ON the decorator, riding its enriched update.
	cust := func(job string) (fk, disp gosql.NullString) {
		t.Helper()
		err := db.DB.QueryRow(
			fmt.Sprintf("SELECT cust, cust_name FROM %s WHERE job_id = %s", qt, placeholder(0)), job).
			Scan(&fk, &disp)
		require.NoError(t, err)
		return fk, disp
	}
	// A fresh proposal for j1 carries the customer FK; the dimension row
	// has not arrived — the FK lands, the display is NULL.
	sync(cluster.NewUpsertEntity(propType, []byte("p4"), []byte(`{"id":"p4","jobId":"j1","status":"active","custId":"c1"}`)))
	fk, disp := cust("j1")
	require.Equal(t, "c1", fk.String, "the decorated FK lands")
	require.False(t, disp.Valid, "no dimension row yet — display starts NULL")

	// The dimension arrives: the fan-out heals the display regardless of
	// which source wrote the FK.
	sync(cluster.NewUpsertEntity(custType, []byte("c1"), []byte(`{"id":"c1","name":"Acme"}`)))
	_, disp = cust("j1")
	require.Equal(t, "Acme", disp.String, "dimension arrival heals a decorator-written FK")

	// A dimension rename fans out.
	sync(cluster.NewUpsertEntity(custType, []byte("c1"), []byte(`{"id":"c1","name":"Acme Corp"}`)))
	_, disp = cust("j1")
	require.Equal(t, "Acme Corp", disp.String)

	// Owner delete / re-admit: the pull re-lands the decoration INCLUDING
	// the enrichment (the enriched update resolves the dimension live).
	sync(cluster.NewDeleteEntity(jobType, []byte("j1")))
	sync(cluster.NewUpsertEntity(jobType, []byte("j1"), []byte(`{"id":"j1","name":"A3"}`)))
	name, latest = row("j1")
	require.Equal(t, "A3", name.String)
	require.Equal(t, "p4", latest.String, "re-admitted row recovers the retained decoration")
	fk, disp = cust("j1")
	require.Equal(t, "c1", fk.String)
	require.Equal(t, "Acme Corp", disp.String, "the pull re-resolves the enrichment from the dimension")

	require.Equal(t, 2, count(), "no rows the owner never admitted")
}

func TestPostgreSQLIntegration_ProjRowOwnerStage(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	projectionRowOwnerStageFlow(t, db, table, `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_ProjRowOwnerStage(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	projectionRowOwnerStageFlow(t, db, table, "`", "`",
		func(int) string { return "?" })
}
