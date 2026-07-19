//go:build docker || integration

package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// qualifiedSchema is a SECOND database, distinct from the DSN's default (dbName),
// used to prove committed's MySQL ingest honors a schema-qualified `tables` entry.
const qualifiedSchema = "otherschema"

// setupQualifiedSchema creates the second database with a data table (two rows)
// and a spatial table, using the privileged test user, and drops it on cleanup.
func setupQualifiedSchema(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	mk := func(q string) { _, err := db.Exec(q); require.NoError(t, err) }

	mk("CREATE DATABASE IF NOT EXISTS " + qualifiedSchema)
	mk(fmt.Sprintf("DROP TABLE IF EXISTS %s.widget", qualifiedSchema))
	mk(fmt.Sprintf("CREATE TABLE %s.widget (pk VARCHAR(32) PRIMARY KEY, name VARCHAR(64))", qualifiedSchema))
	mk(fmt.Sprintf("INSERT INTO %s.widget (pk, name) VALUES ('w1','alpha'),('w2','beta')", qualifiedSchema))
	mk(fmt.Sprintf("DROP TABLE IF EXISTS %s.spatial_widget", qualifiedSchema))
	mk(fmt.Sprintf("CREATE TABLE %s.spatial_widget (pk VARCHAR(32) PRIMARY KEY, g POINT)", qualifiedSchema))

	t.Cleanup(func() {
		c := createDB(t)
		defer c.Close()
		_, _ = c.Exec("DROP DATABASE IF EXISTS " + qualifiedSchema)
	})
}

// TestSchemaQualified_SnapshotThenStream is the end-to-end parity proof: an
// ingestable whose `tables` names a table in a NON-default database
// (otherschema.widget) snapshots the right table (the FROM clause splits the
// qualifier via sqlident.Table) and then streams a subsequent INSERT from the
// server-wide binlog (watches matches the qualified schema.table). Postgres has
// always supported schema-qualified names; this pins MySQL to the same.
func TestSchemaQualified_SnapshotThenStream(t *testing.T) {
	setupQualifiedSchema(t)

	cfg := &sql.Config{
		Type:             &cluster.Type{ID: "widget", Name: "widget"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "name", SQLColumn: "name"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{qualifiedSchema + ".widget"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- (&mysql.MySQLDialect{}).Ingest(ctx, cfg, nil, 0, proposalChan, positionChan) }()

	// collect drains proposals into seen until it holds want distinct keys, or fails.
	seen := map[string]bool{}
	collect := func(want int, whatFailed string) {
		deadline := time.After(20 * time.Second)
		for len(seen) < want {
			select {
			case p := <-proposalChan:
				for _, e := range p.Entities {
					if e.IsRefreshBoundary() {
						continue // the snapshot's closing marker is not a data row
					}
					seen[string(e.Key)] = true
				}
			case <-positionChan:
			case <-deadline:
				t.Fatalf("%s: have %d of %d distinct rows", whatFailed, len(seen), want)
			}
		}
	}

	// Snapshot: both existing rows of otherschema.widget must arrive. If the FROM
	// clause mis-quoted the qualifier the SELECT would error and none would.
	collect(2, "snapshot of the schema-qualified table")

	// Stream: an INSERT after the snapshot must be tailed from the server-wide
	// binlog and pass the watches filter for the qualified schema.table.
	sdb := createDB(t)
	_, err := sdb.Exec(fmt.Sprintf("INSERT INTO %s.widget (pk, name) VALUES ('w3','gamma')", qualifiedSchema))
	require.NoError(t, err)
	sdb.Close()
	collect(3, "CDC stream of the schema-qualified table")

	cancel()
	select {
	case <-ingestErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Ingest did not exit after cancel")
	}
}

// TestSchemaQualified_PreflightSpatialReject proves the spatial/VECTOR reject
// (checkUnsupportedColumnTypes) queries the entry's schema, not DATABASE(): a
// POINT column in otherschema.spatial_widget is caught, and the non-spatial
// qualified table passes.
func TestSchemaQualified_PreflightSpatialReject(t *testing.T) {
	setupQualifiedSchema(t)
	dialect := &mysql.MySQLDialect{}

	err := dialect.Preflight(&sql.Config{
		Type:             &cluster.Type{ID: "sp", Name: "sp"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "g", SQLColumn: "g"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{qualifiedSchema + ".spatial_widget"},
	})
	require.Error(t, err, "a POINT column in the qualified table must be rejected")
	require.Contains(t, err.Error(), "g (point)")

	err = dialect.Preflight(&sql.Config{
		Type:             &cluster.Type{ID: "w", Name: "w"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "name", SQLColumn: "name"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{qualifiedSchema + ".widget"},
	})
	require.NoError(t, err, "the non-spatial qualified table passes preflight")
}

// TestSchemaQualified_SourceColumns proves column introspection (mysqlTableColumns,
// used by MapAll) resolves a schema-qualified entry against the right schema.
func TestSchemaQualified_SourceColumns(t *testing.T) {
	setupQualifiedSchema(t)

	cols, err := (&mysql.MySQLDialect{}).SourceColumns(&sql.Config{
		ConnectionString: ingestURL,
		Tables:           []string{qualifiedSchema + ".widget"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"pk", "name"}, cols[qualifiedSchema+".widget"])
}
