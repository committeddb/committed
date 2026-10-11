//go:build docker || integration

package dialects_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// array_agg / string_agg with ORDER BY against their oracle: a projection
// lands ordered arrays (JSONB) and delimited text per movie while the same
// writes go to a raw Postgres table; after every step the projection must
// equal PostgreSQL's own aggregates over the raw rows. The ordering keys
// are tie-free, where PG's order is specified.
func TestPostgreSQLIntegration_ProjOrderedAggregates(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	raw := "raw_" + table // a prefix: PG truncates names at 63 bytes, so a suffix could collide
	defer dropTable(t, table)
	defer dropTable(t, raw)
	_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (id TEXT PRIMARY KEY, movie TEXT, person TEXT, billing NUMERIC, role TEXT)`, raw))
	require.NoError(t, err)

	creditType := &cluster.Type{ID: "credits", Name: "Credit"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"movie"},
		Columns: []sql.ProjectionColumn{
			{Name: "movie", SQLType: "VARCHAR(32)"},
			{Name: "cast_ids", SQLType: "JSONB"},
			{Name: "cast_names", SQLType: "TEXT"},
			{Name: "roles", SQLType: "JSONB"},
			{Name: "late", SQLType: "JSONB"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "by-movie", From: "credits", KeyPath: []string{"$.movie"}, Reduce: "aggregate",
			Emit: []sql.StageEmit{
				{Field: "cast_ids", Expr: "array_agg($.person order by $.billing)"},
				{Field: "cast_names", Expr: "string_agg(upper($.person), ', ' order by $.billing desc)"},
				{Field: "roles", Expr: "array_agg(distinct $.role order by $.role desc)"},
				{Field: "late", Expr: "array_agg($.billing order by $.billing) filter (where $.billing > 2)"},
			},
		}},
		Sources: []sql.ProjectionSource{{
			FromStage: "by-movie",
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "cast_ids", From: "$.cast_ids"},
				{Column: "cast_names", From: "$.cast_names"},
				{Column: "roles", From: "$.roles"},
				{Column: "late", From: "$.late"},
			}}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	var idx uint64
	upsert := func(id, movie, person, billing, role string) {
		t.Helper()
		idx++
		payload := fmt.Sprintf(`{"id":%q,"movie":%q,"person":%q,"billing":%s,"role":%q}`, id, movie, person, billing, role)
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(creditType, []byte(id), []byte(payload)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`INSERT INTO %q VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET movie = EXCLUDED.movie, person = EXCLUDED.person, billing = EXCLUDED.billing, role = EXCLUDED.role`, raw),
			id, movie, person, billing, role)
		require.NoError(t, err)
	}
	del := func(id string) {
		t.Helper()
		idx++
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(creditType, []byte(id)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`DELETE FROM %q WHERE id = $1`, raw), id)
		require.NoError(t, err)
	}
	dump := func(query string) string {
		t.Helper()
		rows, err := db.DB.Query(query)
		require.NoError(t, err)
		defer rows.Close()
		var lines []string
		for rows.Next() {
			vals := make([]*string, 5)
			ptrs := make([]any, len(vals))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(vals))
			for i, v := range vals {
				parts[i] = "NULL"
				if v != nil {
					parts[i] = *v
				}
			}
			lines = append(lines, strings.Join(parts, " | "))
		}
		require.NoError(t, rows.Err())
		return strings.Join(lines, "\n")
	}
	assertMatchesOracle := func(step string) {
		t.Helper()
		got := dump(fmt.Sprintf(`SELECT movie, cast_ids::text, cast_names, roles::text, late::text FROM %q ORDER BY movie`, table))
		want := dump(fmt.Sprintf(`SELECT movie,
				to_jsonb(array_agg(person order by billing))::text,
				string_agg(upper(person), ', ' order by billing desc),
				to_jsonb(array_agg(distinct role order by role desc))::text,
				to_jsonb(array_agg(billing order by billing) filter (where billing > 2))::text
			FROM %q GROUP BY movie ORDER BY movie`, raw))
		require.NotEmpty(t, want, "%s: the oracle must have rows to compare", step)
		require.Equal(t, want, got, step)
	}

	upsert("c1", "m1", "p-ada", "2", "actor")
	upsert("c2", "m1", "p-bea", "1", "director")
	upsert("c3", "m1", "p-cy", "3", "actor")
	upsert("c4", "m2", "p-di", "1", "writer")
	assertMatchesOracle("initial")

	upsert("c2", "m1", "p-bea", "5", "director")
	assertMatchesOracle("a billing change reorders the arrays")

	upsert("c5", "m2", "p-ed", "4", "actor")
	del("c4")
	assertMatchesOracle("an add and a delete on m2")

	upsert("c3", "m2", "p-cy", "3", "actor")
	assertMatchesOracle("a credit moves between movies")
}

// Collapsed top-N against its oracle: array subscripts and slices on an
// ordered array_agg — the top cast as one column, the lead actor, a
// tail, and a count — equal PostgreSQL's own subscripts after every step.
func TestPostgreSQLIntegration_ProjArraySubscripts(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	raw := "raw_" + table
	defer dropTable(t, table)
	defer dropTable(t, raw)
	_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (id TEXT PRIMARY KEY, movie TEXT, person TEXT, billing NUMERIC)`, raw))
	require.NoError(t, err)

	creditType := &cluster.Type{ID: "credits", Name: "Credit"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"movie"},
		Columns: []sql.ProjectionColumn{
			{Name: "movie", SQLType: "VARCHAR(32)"},
			{Name: "top_cast", SQLType: "JSONB"},
			{Name: "lead_actor", SQLType: "TEXT"},
			{Name: "supporting", SQLType: "JSONB"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "by-movie", From: "credits", KeyPath: []string{"$.movie"}, Reduce: "aggregate",
			Emit: []sql.StageEmit{
				{Field: "top_cast", Expr: "(array_agg($.person order by $.billing))[1:3]"},
				{Field: "lead_actor", Expr: "(array_agg($.person order by $.billing))[1]"},
				{Field: "supporting", Expr: "(array_agg($.person order by $.billing))[2:]"},
				{Field: "n", Expr: "cardinality(array_agg($.person))"},
			},
		}},
		Sources: []sql.ProjectionSource{{
			FromStage: "by-movie",
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "top_cast", From: "$.top_cast"},
				{Column: "lead_actor", From: "$.lead_actor"},
				{Column: "supporting", From: "$.supporting"},
				{Column: "n", From: "$.n"},
			}}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	var idx uint64
	upsert := func(id, movie, person, billing string) {
		t.Helper()
		idx++
		payload := fmt.Sprintf(`{"id":%q,"movie":%q,"person":%q,"billing":%s}`, id, movie, person, billing)
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(creditType, []byte(id), []byte(payload)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`INSERT INTO %q VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET movie = EXCLUDED.movie, person = EXCLUDED.person, billing = EXCLUDED.billing`, raw),
			id, movie, person, billing)
		require.NoError(t, err)
	}
	del := func(id string) {
		t.Helper()
		idx++
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(creditType, []byte(id)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`DELETE FROM %q WHERE id = $1`, raw), id)
		require.NoError(t, err)
	}
	dump := func(query string) string {
		t.Helper()
		rows, err := db.DB.Query(query)
		require.NoError(t, err)
		defer rows.Close()
		var lines []string
		for rows.Next() {
			vals := make([]*string, 5)
			ptrs := make([]any, len(vals))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(vals))
			for i, v := range vals {
				parts[i] = "NULL"
				if v != nil {
					parts[i] = *v
				}
			}
			lines = append(lines, strings.Join(parts, " | "))
		}
		require.NoError(t, rows.Err())
		return strings.Join(lines, "\n")
	}
	assertMatchesOracle := func(step string) {
		t.Helper()
		got := dump(fmt.Sprintf(`SELECT movie, top_cast::text, lead_actor, supporting::text, n::text FROM %q ORDER BY movie`, table))
		want := dump(fmt.Sprintf(`SELECT movie,
				to_jsonb((array_agg(person order by billing))[1:3])::text,
				(array_agg(person order by billing))[1],
				to_jsonb((array_agg(person order by billing))[2:])::text,
				cardinality(array_agg(person))::text
			FROM %q GROUP BY movie ORDER BY movie`, raw))
		require.NotEmpty(t, want, "%s: the oracle must have rows to compare", step)
		require.Equal(t, want, got, step)
	}

	upsert("c1", "m1", "p-ada", "2")
	upsert("c2", "m1", "p-bea", "1")
	upsert("c3", "m1", "p-cy", "3")
	upsert("c4", "m1", "p-di", "4")
	upsert("c5", "m2", "p-ed", "1") // one credit: an empty supporting slice
	assertMatchesOracle("initial")

	upsert("c4", "m1", "p-di", "0.5")
	assertMatchesOracle("a new lead pushes the third out of the top three")

	del("c2")
	assertMatchesOracle("a delete")
}
