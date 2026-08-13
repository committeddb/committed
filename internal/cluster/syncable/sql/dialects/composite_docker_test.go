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

// compositeFlow drives the full composite-PK syncable life against a real
// destination: Init creates the table with a REAL multi-column PRIMARY KEY,
// upserts converge on the column pair, and a payload-less delete tombstone —
// whose key is the cluster.CompositeKey JSON-array encoding, exactly what a
// composite-keyed ingest produces — removes precisely the one addressed row.
// This is the field finding's acceptance shape (the pilot's two held tables).
func compositeFlow(t *testing.T, db *sql.DB, table, keyType, quoteL, quoteR string, placeholder func(i int) string) {
	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.t", Column: "tenant_id", SQLType: keyType},
			{JsonPath: "$.p", Column: "project_id", SQLType: keyType},
			{JsonPath: "$.v", Column: "v", SQLType: "TEXT"},
		},
		PrimaryKey: []string{"tenant_id", "project_id"},
	}
	syncable := sql.New(db, cfg)
	require.NoError(t, syncable.Init(), "composite CreateDDL must be valid SQL")
	defer syncable.Close()

	key := func(tenant, project string) string {
		return cluster.CompositeKey(
			map[string]any{"tenant_id": tenant, "project_id": project},
			cfg.PrimaryKey)
	}
	up := func(tenant, project, v string) *cluster.Actual {
		return &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(eventType, []byte(key(tenant, project)),
				[]byte(fmt.Sprintf(`{"t":%q,"p":%q,"v":%q}`, tenant, project, v))),
		}}
	}

	// Three logical rows; (7,42) written twice must converge to one row with
	// the last value — the composite conflict target working.
	for _, a := range []*cluster.Actual{
		up("7", "42", "first"), up("7", "43", "sibling"), up("8", "42", "other-tenant"), up("7", "42", "second"),
	} {
		_, err := syncable.Sync(context.Background(), a)
		require.NoError(t, err)
	}

	qt := quoteL + table + quoteR
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&count))
	require.Equal(t, 3, count, "upsert must converge on the column PAIR, not either column alone")

	var v string
	require.NoError(t, db.DB.QueryRow(
		fmt.Sprintf("SELECT v FROM %s WHERE tenant_id = %s AND project_id = %s", qt, placeholder(0), placeholder(1)),
		"7", "42").Scan(&v))
	require.Equal(t, "second", v)

	// The delete tombstone: key-only entity, no payload. Must remove exactly
	// (7,42) — the sibling sharing tenant 7 and the row sharing project 42
	// both survive (the multi-row hazard a single-column key would cause).
	del := &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(eventType, []byte(key("7", "42"))),
	}}
	_, err := syncable.Sync(context.Background(), del)
	require.NoError(t, err)

	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+qt).Scan(&count))
	require.Equal(t, 2, count, "the tombstone must remove exactly the one addressed row")
	require.NoError(t, db.DB.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE tenant_id = %s", qt, placeholder(0)), "7").Scan(&count))
	require.Equal(t, 1, count, "the sibling row sharing tenant_id must survive")
}

func TestPostgreSQLIntegration_CompositePrimaryKeyFlow(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	compositeFlow(t, db, table, "VARCHAR(32)", `"`, `"`,
		func(i int) string { return fmt.Sprintf("$%d", i+1) })
}

func TestMySQLIntegration_CompositePrimaryKeyFlow(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	compositeFlow(t, db, table, "VARCHAR(32)", "`", "`",
		func(int) string { return "?" })
}
