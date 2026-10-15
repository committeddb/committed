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

// Aggregate expressions and HAVING against their oracle: a projection
// folds jobs' visits through one aggregate stage — a ratio of sums, an avg
// under round, FILTER, a distinct count, a sum — under a having, while the
// same writes go to a raw Postgres table. After every step (inserts, an
// update that flips the having off and back on, a delete, a key move) the
// projection must equal PostgreSQL's own GROUP BY … HAVING over the raw
// rows.
func TestPostgreSQLIntegration_ProjAggregateExpressions(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	raw := "raw_" + table // a prefix: PG truncates names at 63 bytes, so a suffix could collide
	defer dropTable(t, table)
	defer dropTable(t, raw)
	_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (id TEXT PRIMARY KEY, k TEXT, rev NUMERIC, cost NUMERIC, rate NUMERIC, status TEXT, tech TEXT)`, raw))
	require.NoError(t, err)

	visitType := &cluster.Type{ID: "visits", Name: "Visit"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"k"},
		Columns: []sql.ProjectionColumn{
			{Name: "k", SQLType: "VARCHAR(32)"},
			{Name: "margin", SQLType: "DECIMAL(12,4)"},
			{Name: "avg_rate", SQLType: "DECIMAL(12,2)"},
			{Name: "done", SQLType: "INT"},
			{Name: "techs", SQLType: "INT"},
			{Name: "total", SQLType: "DECIMAL(12,2)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "by-job", From: "visits", KeyPath: []string{"$.k"}, Reduce: "aggregate",
			Emit: []sql.StageEmit{
				{Field: "margin", Expr: "round((sum($.rev) - sum($.cost)) / nullif(sum($.rev), 0), 4)"},
				{Field: "avg_rate", Expr: "round(avg($.rate), 2)"},
				{Field: "done", Expr: "count(*) filter (where $.status = 'done')"},
				{Field: "techs", Expr: "count(distinct lower($.tech))"},
				{Field: "total", Sum: "$.rev"},
			},
			Having: "sum($.rev) > 0 and count(*) >= 2",
		}},
		Sources: []sql.ProjectionSource{{
			FromStage: "by-job",
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "margin", From: "$.margin"},
				{Column: "avg_rate", From: "$.avg_rate"},
				{Column: "done", From: "$.done"},
				{Column: "techs", From: "$.techs"},
				{Column: "total", From: "$.total"},
			}}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	var idx uint64
	upsert := func(id, k, rev, cost, rate, status, tech string) {
		t.Helper()
		idx++
		payload := fmt.Sprintf(`{"id":%q,"k":%q,"rev":%s,"cost":%s,"rate":%s,"status":%q,"tech":%q}`, id, k, rev, cost, rate, status, tech)
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(visitType, []byte(id), []byte(payload)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`INSERT INTO %q VALUES ($1, $2, $3, $4, $5::numeric, $6, $7) ON CONFLICT (id) DO UPDATE SET k = EXCLUDED.k, rev = EXCLUDED.rev, cost = EXCLUDED.cost, rate = EXCLUDED.rate, status = EXCLUDED.status, tech = EXCLUDED.tech`, raw),
			id, k, rev, cost, nullIfJSONNull(rate), status, tech)
		require.NoError(t, err)
	}
	del := func(id string) {
		t.Helper()
		idx++
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(visitType, []byte(id)),
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
			cols := make([]any, 6)
			vals := make([]*string, 6)
			for i := range cols {
				cols[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(cols...))
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
		got := dump(fmt.Sprintf(`SELECT k, margin::text, avg_rate::text, done::text, techs::text, total::text FROM %q ORDER BY k`, table))
		want := dump(fmt.Sprintf(`SELECT k,
				round((sum(rev) - sum(cost)) / nullif(sum(rev), 0), 4)::numeric(12,4)::text,
				round(avg(rate), 2)::numeric(12,2)::text,
				(count(*) filter (where status = 'done'))::text,
				count(distinct lower(tech))::text,
				sum(rev)::numeric(12,2)::text
			FROM %q GROUP BY k HAVING sum(rev) > 0 and count(*) >= 2 ORDER BY k`, raw))
		require.Equal(t, want, got, step)
	}

	upsert("v1", "j1", "100", "60", "10", "done", "Ada")
	upsert("v2", "j1", "50", "45", "15.5", "open", "ada")
	upsert("v3", "j1", "0", "5", "null", "open", "Bea")
	upsert("v4", "j2", "30", "10", "7", "done", "Cy")
	upsert("v5", "j3", "20", "25", "8", "done", "Di")
	upsert("v6", "j3", "-5", "0", "8.25", "open", "di")
	assertMatchesOracle("initial: j2 has one visit and is held back by count(*) >= 2")
	require.NotEmpty(t, dump(fmt.Sprintf(`SELECT k, '', '', '', '', '' FROM %q`, table)), "the comparison is not vacuous")

	upsert("v6", "j3", "-30", "0", "8.25", "open", "di")
	assertMatchesOracle("j3's revenue goes negative: held back")

	upsert("v6", "j3", "5", "0", "8.25", "done", "di")
	assertMatchesOracle("j3 flips back in")

	del("v1")
	assertMatchesOracle("deleting a visit refolds j1")

	upsert("v5", "j2", "20", "25", "8", "done", "Di")
	assertMatchesOracle("v5 moves to j2: j2 qualifies, j3 drops to one visit")
}

// nullIfJSONNull maps the fixture's JSON null spelling to SQL NULL.
func nullIfJSONNull(s string) any {
	if s == "null" {
		return nil
	}
	return s
}
