//go:build docker || integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlfakes"
)

// TestPostgresMapAllColumns is the acceptance scenario: a config with
// mapAllColumns (and one excludeColumns entry, no per-column mappings) ingests
// every source column 1:1 except the excluded one — proving SourceColumns
// introspection (against a schema-qualified table), the parser's expansion, and
// that the resulting worker ingests the inferred columns end-to-end.
func TestPostgresMapAllColumns(t *testing.T) {
	db := createDB(t)
	_, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ingress`)
	require.NoError(t, err)
	_, err = db.Exec(`DROP TABLE IF EXISTS ingress.mapall_movie`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ingress.mapall_movie (
		movie_id       INT  PRIMARY KEY,
		title          TEXT NOT NULL,
		year           INT,
		internal_notes TEXT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO ingress.mapall_movie VALUES (1, 'The Slawshank Redemption', 1994, 'do not ship')`)
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_mapall", "pub_mapall")

	toml := fmt.Sprintf(`
[ingestable]
name = "mapall"
type = "sql"

[sql]
dialect = "postgres"
topic = "movie"
connectionString = %q
primaryKey = "movie_id"
tables = ["ingress.mapall_movie"]
mapAllColumns = true
excludeColumns = ["internal_notes"]

[sql.postgres]
slot_name = "slot_mapall"
publication = "pub_mapall"
`, connString)

	v, err := cluster.ParseConfigBytes("text/toml", []byte(toml))
	require.NoError(t, err)

	tiper := &sqlfakes.FakeTyper{}
	tiper.ResolveTypeReturns(&cluster.Type{ID: "movie", Name: "movie"}, nil)
	p := sql.NewIngestableParser(tiper)
	p.Dialects["postgres"] = &postgres.PostgreSQLDialect{}

	// Parse introspects the source, expands map-all to explicit mappings, and
	// preflights — all against the live schema.
	ing, err := p.Parse(v)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 10)
	po := make(chan cluster.Position, 10)
	go func() { _ = ing.Ingest(ctx, nil, pr, po) }()

	deadline := time.After(20 * time.Second)
	var got *cluster.Entity
	for got == nil {
		select {
		case prop := <-pr:
			for _, e := range prop.Entities {
				if string(e.Key) == "1" {
					got = e
				}
			}
		case <-po:
		case <-deadline:
			t.Fatal("timed out waiting for the snapshot proposal")
		}
	}

	var data map[string]any
	require.NoError(t, json.Unmarshal(got.Data, &data))

	// Every column inferred 1:1 (jsonName = column)...
	require.Contains(t, data, "movie_id")
	require.Contains(t, data, "title")
	require.Contains(t, data, "year")
	require.Equal(t, "The Slawshank Redemption", data["title"])
	// ...except the excluded one.
	require.NotContains(t, data, "internal_notes")
}
