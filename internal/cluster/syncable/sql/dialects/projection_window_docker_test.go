//go:build docker || integration

package dialects_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// Window stages against their oracle: each of the window ticket's four
// acceptance read models runs as a projection while the same writes land
// in a raw Postgres table, and after every step the projection must equal
// PostgreSQL's own window functions over the raw rows — through in-order
// appends, an out-of-order insert, an update, a partition move, and a
// delete. Where PG's order is arbitrary (ties), the data is tie-free on
// the full PG ORDER BY; where PG defines ties (peers under rank and the
// default frame), the data has them.

// windowOracle drives one projection and its raw twin.
type windowOracle struct {
	t      *testing.T
	db     *sql.DB
	p      *sql.Projection
	typ    *cluster.Type
	raw    string
	table  string
	cols   []string // raw columns, id first
	idx    uint64
	sqlFor map[string]string // column → a SQL cast for its parameter
}

func newWindowOracle(t *testing.T, cfg *sql.ProjectionConfig, topic, rawDDL string, cols []string, casts map[string]string) *windowOracle {
	t.Helper()
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	raw := "raw_" + cfg.Table
	t.Cleanup(func() { dropTable(t, cfg.Table); dropTable(t, raw) })
	_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (%s)`, raw, rawDDL))
	require.NoError(t, err)
	p := sql.NewProjection(db, cfg, nil, cfg.Table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	t.Cleanup(func() { _ = p.Close() })
	return &windowOracle{t: t, db: db, p: p, typ: &cluster.Type{ID: topic, Name: topic}, raw: raw, table: cfg.Table, cols: cols, sqlFor: casts}
}

func (o *windowOracle) upsert(row map[string]any) {
	o.t.Helper()
	o.idx++
	id := fmt.Sprint(row["id"])
	payload, err := json.Marshal(row)
	require.NoError(o.t, err)
	_, err = o.p.Sync(context.Background(), &cluster.Actual{Index: o.idx, Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(o.typ, []byte(id), payload),
	}})
	require.NoError(o.t, err)
	ph := make([]string, len(o.cols))
	sets := make([]string, 0, len(o.cols))
	args := make([]any, len(o.cols))
	for i, c := range o.cols {
		ph[i] = fmt.Sprintf("$%d%s", i+1, o.sqlFor[c])
		args[i] = row[c]
		if i > 0 {
			sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
		}
	}
	_, err = o.db.DB.Exec(fmt.Sprintf(`INSERT INTO %q (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s`,
		o.raw, strings.Join(o.cols, ", "), strings.Join(ph, ", "), strings.Join(sets, ", ")), args...)
	require.NoError(o.t, err)
}

func (o *windowOracle) del(id string) {
	o.t.Helper()
	o.idx++
	_, err := o.p.Sync(context.Background(), &cluster.Actual{Index: o.idx, Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(o.typ, []byte(id)),
	}})
	require.NoError(o.t, err)
	_, err = o.db.DB.Exec(fmt.Sprintf(`DELETE FROM %q WHERE id = $1`, o.raw), id)
	require.NoError(o.t, err)
}

// dump renders a query's rows as text lines (NULL for nulls).
func (o *windowOracle) dump(query string) string {
	o.t.Helper()
	rows, err := o.db.DB.Query(query)
	require.NoError(o.t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(o.t, err)
	var lines []string
	for rows.Next() {
		vals := make([]*string, len(cols))
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(o.t, rows.Scan(ptrs...))
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = "NULL"
			if v != nil {
				parts[i] = *v
			}
		}
		lines = append(lines, strings.Join(parts, " | "))
	}
	require.NoError(o.t, rows.Err())
	return strings.Join(lines, "\n")
}

func (o *windowOracle) assertMatches(step, got, want string) {
	o.t.Helper()
	g, w := o.dump(fmt.Sprintf(got, o.table)), o.dump(fmt.Sprintf(want, o.raw))
	require.NotEmpty(o.t, w, "%s: the oracle must have rows to compare", step)
	require.Equal(o.t, w, g, step)
}

// 1. Time in status: a window stage's lead spans, summed per (job,
// status) by an aggregate stage.
func TestPostgreSQLIntegration_ProjWindowTimeInStatus(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"job", "status"},
		Columns: []sql.ProjectionColumn{
			{Name: "job", SQLType: "VARCHAR(32)"}, {Name: "status", SQLType: "VARCHAR(32)"}, {Name: "secs", SQLType: "NUMERIC(18,6)"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "spans", From: "job-status", Reduce: "window",
				PartitionBy: []string{"$.job"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"$.seq"},
				Emit: []sql.StageEmit{
					{Field: "job", From: "$.job"},
					{Field: "status", From: "$.status"},
					{Field: "secs", Expr: "extract(epoch from (lead($.at::timestamptz) - $.at::timestamptz))"},
				},
			},
			{
				Name: "time-in-status", From: "spans", KeyPath: []string{"$.job", "$.status"}, Reduce: "aggregate",
				Emit: []sql.StageEmit{{Field: "secs", Expr: "sum($.secs)"}},
			},
		},
		Sources: []sql.ProjectionSource{{FromStage: "time-in-status", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "secs", From: "$.secs"}}}}}},
	}
	o := newWindowOracle(t, cfg, "job-status", `id TEXT PRIMARY KEY, job TEXT, status TEXT, at TIMESTAMPTZ, seq INT`,
		[]string{"id", "job", "status", "at", "seq"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT job, status, secs::numeric(18,6)::text FROM %q ORDER BY job, status`
	want := `SELECT job, status, sum(secs)::numeric(18,6)::text FROM (
		SELECT job, status, extract(epoch from (lead(at) OVER w - at)) AS secs
		FROM %q WINDOW w AS (PARTITION BY job ORDER BY at, seq)) s
		GROUP BY job, status ORDER BY job, status`

	ev := func(id, job, status, at string, seq int) map[string]any {
		return map[string]any{"id": id, "job": job, "status": status, "at": at, "seq": seq}
	}
	o.upsert(ev("e1", "j1", "scheduled", "2024-01-01T10:00:00Z", 1))
	o.upsert(ev("e2", "j1", "running", "2024-01-01T10:05:30Z", 2))
	o.upsert(ev("e3", "j1", "done", "2024-01-01T11:00:00.25Z", 3))
	o.upsert(ev("e4", "j2", "scheduled", "2024-01-02T09:00:00Z", 4))
	o.upsert(ev("e5", "j2", "running", "2024-01-02T09:30:00Z", 5))
	o.assertMatches("in-order appends", got, want)

	o.upsert(ev("e6", "j1", "paused", "2024-01-01T10:20:00Z", 6))
	o.assertMatches("an out-of-order insert re-splits a span", got, want)

	o.upsert(ev("e6", "j1", "running", "2024-01-01T10:20:00Z", 6))
	o.assertMatches("an update merges two statuses' time", got, want)

	o.upsert(ev("e2", "j2", "running", "2024-01-02T08:00:00Z", 2))
	o.assertMatches("an event moves to another job", got, want)

	o.del("e1")
	o.assertMatches("a delete", got, want)
}

// 2. Running balance: the default frame's peers (equal timestamps) share
// one balance, as PG's RANGE … CURRENT ROW defines — no tieBy needed.
func TestPostgreSQLIntegration_ProjWindowRunningBalance(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "VARCHAR(32)"},
			{Name: "account", SQLType: "VARCHAR(32)"},
			{Name: "balance", SQLType: "NUMERIC(18,2)"},
			{Name: "n", SQLType: "INT"},
			{Name: "rnk", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "balances", From: "txns", Reduce: "window",
			PartitionBy: []string{"$.account"}, WindowOrderBy: []string{"$.at::timestamptz"},
			Emit: []sql.StageEmit{
				{Field: "account", From: "$.account"},
				{Field: "balance", Expr: "sum($.amount)"},
				{Field: "n", Expr: "count(*)"},
				{Field: "rnk", Expr: "dense_rank()"},
			},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "balances", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "account", From: "$.account"}, {Column: "balance", From: "$.balance"}, {Column: "n", From: "$.n"}, {Column: "rnk", From: "$.rnk"},
		}}}}},
	}
	o := newWindowOracle(t, cfg, "txns", `id TEXT PRIMARY KEY, account TEXT, at TIMESTAMPTZ, amount NUMERIC`,
		[]string{"id", "account", "at", "amount"}, map[string]string{"at": "::timestamptz", "amount": "::numeric"})
	got := `SELECT id, account, balance::numeric(18,2)::text, n, rnk FROM %q ORDER BY id`
	want := `SELECT id, account, (sum(amount) OVER w)::numeric(18,2)::text, count(*) OVER w, dense_rank() OVER w
		FROM %q WINDOW w AS (PARTITION BY account ORDER BY at) ORDER BY id`

	tx := func(id, acct, at, amount string) map[string]any {
		return map[string]any{"id": id, "account": acct, "at": at, "amount": json.Number(amount)}
	}
	o.upsert(tx("t1", "a", "2024-03-01T00:00:00Z", "100"))
	o.upsert(tx("t2", "a", "2024-03-02T00:00:00Z", "-20.50"))
	o.upsert(tx("t3", "a", "2024-03-02T00:00:00Z", "5.25")) // a peer of t2
	o.upsert(tx("t4", "b", "2024-03-01T12:00:00Z", "7"))
	o.upsert(tx("t5", "a", "2024-03-05T00:00:00Z", "1"))
	o.assertMatches("in-order appends with peers", got, want)

	o.upsert(tx("t6", "a", "2024-02-28T00:00:00Z", "50"))
	o.assertMatches("a backdated transaction shifts every later balance", got, want)

	o.upsert(tx("t3", "b", "2024-03-02T00:00:00Z", "5.25"))
	o.assertMatches("a transaction moves accounts", got, want)

	o.del("t2")
	o.assertMatches("a delete", got, want)
}

// 3. An audit of status changes: lag, IS DISTINCT FROM, the first row's
// null.
func TestPostgreSQLIntegration_ProjWindowChangeAudit(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "VARCHAR(32)"},
			{Name: "changed", SQLType: "BOOLEAN"},
			{Name: "prev_status", SQLType: "VARCHAR(32)"},
			{Name: "status", SQLType: "VARCHAR(32)"},
			{Name: "next_status", SQLType: "VARCHAR(32)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "changes", From: "job-status", Reduce: "window",
			PartitionBy: []string{"$.job"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"$.seq"},
			Emit: []sql.StageEmit{
				{Field: "changed", Expr: "lag($.status) is distinct from $.status"},
				{Field: "prev", Expr: "lag($.status)"},
				{Field: "status", From: "$.status"},
				{Field: "next", Expr: "lead($.status, 1, 'open')"},
			},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "changes", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "changed", From: "$.changed"}, {Column: "prev_status", From: "$.prev"}, {Column: "status", From: "$.status"}, {Column: "next_status", From: "$.next"},
		}}}}},
	}
	o := newWindowOracle(t, cfg, "job-status", `id TEXT PRIMARY KEY, job TEXT, status TEXT, at TIMESTAMPTZ, seq INT`,
		[]string{"id", "job", "status", "at", "seq"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT id, changed, prev_status, status, next_status FROM %q ORDER BY id`
	want := `SELECT id, lag(status) OVER w IS DISTINCT FROM status, lag(status) OVER w, status, lead(status, 1, 'open') OVER w
		FROM %q WINDOW w AS (PARTITION BY job ORDER BY at, seq) ORDER BY id`

	ev := func(id, job, status, at string, seq int) map[string]any {
		return map[string]any{"id": id, "job": job, "status": status, "at": at, "seq": seq}
	}
	o.upsert(ev("a1", "j1", "new", "2024-01-01T00:00:00Z", 1))
	o.upsert(ev("a2", "j1", "new", "2024-01-01T01:00:00Z", 2))
	o.upsert(ev("a3", "j1", "done", "2024-01-01T02:00:00Z", 3))
	o.upsert(ev("a4", "j1", "done", "2024-01-01T02:00:00Z", 4)) // same instant: seq orders it
	o.assertMatches("in-order appends", got, want)

	o.upsert(ev("a5", "j1", "held", "2024-01-01T00:30:00Z", 5))
	o.assertMatches("an out-of-order insert", got, want)

	o.upsert(ev("a2", "j2", "new", "2024-01-01T01:00:00Z", 2))
	o.assertMatches("an event moves to another job", got, want)

	o.del("a1")
	o.assertMatches("a delete makes a new first row", got, want)
}

// 4. Top 3 per game, per row: row_number in a window stage, filtered by
// a reshape stage downstream — a row sliding out of the top 3 retracts.
func TestPostgreSQLIntegration_ProjWindowTopN(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"game", "player"},
		Columns: []sql.ProjectionColumn{
			{Name: "game", SQLType: "VARCHAR(32)"}, {Name: "player", SQLType: "VARCHAR(32)"}, {Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "ranked", From: "scores", Reduce: "window",
				PartitionBy: []string{"$.game"}, WindowOrderBy: []string{"$.points desc", "$.at::timestamptz"}, WindowTieBy: []string{"$.player"},
				Emit: []sql.StageEmit{{Field: "n", Expr: "row_number()"}, {Field: "player", From: "$.player"}, {Field: "game", From: "$.game"}},
			},
			{
				Name: "podium", From: "ranked", KeyPath: []string{"$.game", "$.player"},
				When: []sql.WhenClause{{Expr: "$.n <= 3"}},
				Emit: []sql.StageEmit{{Field: "n", From: "$.n"}},
			},
		},
		Sources: []sql.ProjectionSource{{FromStage: "podium", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "n", From: "$.n"}}}}}},
	}
	o := newWindowOracle(t, cfg, "scores", `id TEXT PRIMARY KEY, game TEXT, player TEXT, points INT, at TIMESTAMPTZ`,
		[]string{"id", "game", "player", "points", "at"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT game, player, n FROM %q ORDER BY game, player`
	want := `SELECT game, player, n FROM (
		SELECT game, player, row_number() OVER (PARTITION BY game ORDER BY points DESC, at, player) AS n FROM %q) s
		WHERE n <= 3 ORDER BY game, player`

	sc := func(game, player string, points int, at string) map[string]any {
		return map[string]any{"id": game + ":" + player, "game": game, "player": player, "points": points, "at": at}
	}
	o.upsert(sc("g1", "ann", 10, "2024-01-01T00:00:00Z"))
	o.upsert(sc("g1", "bob", 30, "2024-01-01T00:01:00Z"))
	o.upsert(sc("g1", "cat", 20, "2024-01-01T00:02:00Z"))
	o.upsert(sc("g1", "dan", 20, "2024-01-01T00:01:30Z")) // ties cat on points; earlier at wins
	o.upsert(sc("g2", "eve", 5, "2024-01-01T00:00:00Z"))
	o.assertMatches("appends: ann is fourth", got, want)

	o.upsert(sc("g1", "ann", 40, "2024-01-01T00:00:00Z"))
	o.assertMatches("ann climbs to first; cat slides out", got, want)

	o.del("g1:bob")
	o.assertMatches("a delete lets cat back in", got, want)
}
