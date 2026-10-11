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

// UNION ALL stages (a list `from`) against their oracle: each acceptance
// read model runs as a projection while the same writes land in one raw
// Postgres table per source topic, and after every step the projection
// must equal PostgreSQL's own query over a UNION ALL of the raw tables —
// through inserts, a delete in each source, and a key collision across
// sources. Where PG's order is arbitrary, the data is tie-free on the full
// PG ORDER BY.

// unionOracle drives one projection fed by several topics and one raw
// table per topic, all with the same columns.
type unionOracle struct {
	t      *testing.T
	db     *sql.DB
	p      *sql.Projection
	table  string
	raws   map[string]string // topic → raw table
	cols   []string          // raw columns, id first
	sqlFor map[string]string // column → a SQL cast for its parameter
	idx    uint64
}

func newUnionOracle(t *testing.T, cfg *sql.ProjectionConfig, topics []string, rawDDL string, cols []string, casts map[string]string) *unionOracle {
	t.Helper()
	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	o := &unionOracle{t: t, db: db, table: cfg.Table, raws: map[string]string{}, cols: cols, sqlFor: casts}
	t.Cleanup(func() { dropTable(t, cfg.Table) })
	for i, topic := range topics {
		raw := fmt.Sprintf("r%d_%s", i, cfg.Table) // a prefix: PG truncates names at 63 bytes
		o.raws[topic] = raw
		t.Cleanup(func() { dropTable(t, raw) })
		_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (%s)`, raw, rawDDL))
		require.NoError(t, err)
	}
	o.p = sql.NewProjection(db, cfg, nil, cfg.Table)
	o.p.SetStoreDir(t.TempDir())
	require.NoError(t, o.p.Init())
	t.Cleanup(func() { _ = o.p.Close() })
	return o
}

func (o *unionOracle) upsert(topic string, row map[string]any) {
	o.t.Helper()
	o.idx++
	id := fmt.Sprint(row["id"])
	payload, err := json.Marshal(row)
	require.NoError(o.t, err)
	typ := &cluster.Type{ID: topic, Name: topic}
	_, err = o.p.Sync(context.Background(), &cluster.Actual{Index: o.idx, Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(typ, []byte(id), payload),
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
		o.raws[topic], strings.Join(o.cols, ", "), strings.Join(ph, ", "), strings.Join(sets, ", ")), args...)
	require.NoError(o.t, err)
}

func (o *unionOracle) del(topic, id string) {
	o.t.Helper()
	o.idx++
	typ := &cluster.Type{ID: topic, Name: topic}
	_, err := o.p.Sync(context.Background(), &cluster.Actual{Index: o.idx, Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(typ, []byte(id)),
	}})
	require.NoError(o.t, err)
	_, err = o.db.DB.Exec(fmt.Sprintf(`DELETE FROM %q WHERE id = $1`, o.raws[topic]), id)
	require.NoError(o.t, err)
}

// assertMatches compares the projection (got, the table as %q) with the
// oracle query (want, each raw table as {topic}).
func (o *unionOracle) assertMatches(step, got, want string) {
	o.t.Helper()
	for topic, raw := range o.raws {
		want = strings.ReplaceAll(want, "{"+topic+"}", fmt.Sprintf("%q", raw))
	}
	dumper := &windowOracle{t: o.t, db: o.db}
	g, w := dumper.dump(fmt.Sprintf(got, o.table)), dumper.dump(want)
	require.NotEmpty(o.t, w, "%s: the oracle must have rows to compare", step)
	require.Equal(o.t, w, g, step)
}

// 1. Cross-system revenue: invoices from two systems fold per customer as
// rows — avg, count(distinct), and an ordered array_agg across both
// sides — with equal invoice keys in both systems kept as two rows.
func TestPostgreSQLIntegration_ProjUnionCrossSystemRevenue(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"customer"},
		Columns: []sql.ProjectionColumn{
			{Name: "customer", SQLType: "VARCHAR(32)"},
			{Name: "avg_amount", SQLType: "NUMERIC(18,2)"},
			{Name: "products", SQLType: "INT"},
			{Name: "amounts", SQLType: "JSONB"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "revenue", FromList: []string{"invoices-a", "invoices-b"}, KeyPath: []string{"$.customer"}, Reduce: "aggregate",
			Emit: []sql.StageEmit{
				{Field: "avg_amount", Expr: "round(avg($.amount), 2)"},
				{Field: "products", Expr: "count(distinct $.product)"},
				{Field: "amounts", Expr: "array_agg($.amount order by $.at::timestamptz, source())"},
				{Field: "n", Expr: "count(*)"},
			},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "revenue", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "avg_amount", From: "$.avg_amount"},
			{Column: "products", From: "$.products"},
			{Column: "amounts", From: "$.amounts"},
			{Column: "n", From: "$.n"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"invoices-a", "invoices-b"},
		`id TEXT PRIMARY KEY, customer TEXT, product TEXT, amount NUMERIC, at TIMESTAMPTZ`,
		[]string{"id", "customer", "product", "amount", "at"}, map[string]string{"at": "::timestamptz", "amount": "::numeric"})
	got := `SELECT customer, avg_amount::text, products::text, amounts::text, n::text FROM %q ORDER BY customer`
	want := `SELECT customer, round(avg(amount), 2)::text, count(distinct product)::text,
			to_jsonb(array_agg(amount ORDER BY at, src))::text, count(*)::text
		FROM (SELECT 'invoices-a' AS src, * FROM {invoices-a} UNION ALL SELECT 'invoices-b', * FROM {invoices-b}) u
		GROUP BY customer ORDER BY customer`

	inv := func(id, customer, product string, amount float64, at string) map[string]any {
		return map[string]any{"id": id, "customer": customer, "product": product, "amount": amount, "at": at}
	}
	o.upsert("invoices-a", inv("i1", "c1", "p1", 10, "2026-01-01T10:00:00Z"))
	o.upsert("invoices-a", inv("i2", "c1", "p2", 20.5, "2026-01-03T10:00:00Z"))
	o.upsert("invoices-b", inv("i1", "c1", "p1", 31, "2026-01-02T10:00:00Z"))
	o.upsert("invoices-b", inv("i2", "c2", "p3", 5, "2026-01-02T10:00:00Z"))
	o.assertMatches("inserts, with invoice i1 in both systems", got, want)

	o.upsert("invoices-b", inv("i3", "c1", "p2", 7, "2026-01-03T10:00:00Z"))
	o.assertMatches("a cross-source tie on at, ordered by source()", got, want)

	o.del("invoices-a", "i1")
	o.assertMatches("a delete in system a leaves b's i1", got, want)

	o.del("invoices-b", "i2")
	o.upsert("invoices-a", inv("i4", "c2", "p1", 2.25, "2026-01-04T10:00:00Z"))
	o.assertMatches("a delete in system b", got, want)

	o.upsert("invoices-a", inv("i2", "c2", "p2", 20.5, "2026-01-03T10:00:00Z"))
	o.assertMatches("an invoice moves customers", got, want)
}

// 2. Account liveness: a liveSet whose create and delete events live in
// different topics, classified by source().
func TestPostgreSQLIntegration_ProjUnionAccountLiveness(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"account"},
		Columns:    []sql.ProjectionColumn{{Name: "account", SQLType: "VARCHAR(32)"}, {Name: "owner", SQLType: "VARCHAR(32)"}},
		Stages: []sql.ProjectionStage{{
			Name: "accounts", FromList: []string{"account-opened", "account-closed"}, KeyPath: []string{"$.account"},
			Reduce: "liveSet", DeleteWhen: []sql.WhenClause{{Expr: "source() = 'account-closed'"}},
			Emit: []sql.StageEmit{{Field: "owner", From: "$.owner"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "accounts", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "owner", From: "$.owner"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"account-opened", "account-closed"},
		`id TEXT PRIMARY KEY, account TEXT, owner TEXT`, []string{"id", "account", "owner"}, nil)
	got := `SELECT account, owner FROM %q ORDER BY account`
	// Live = opened and never closed; a re-opened account shows its
	// greatest event's owner (the liveSet's input-identity rule).
	want := `SELECT DISTINCT ON (account) account, owner
		FROM (SELECT 'account-opened' AS src, * FROM {account-opened} UNION ALL SELECT 'account-closed', * FROM {account-closed}) u
		WHERE src = 'account-opened' AND account NOT IN (SELECT account FROM {account-closed})
		ORDER BY account, id COLLATE "C" DESC`

	ev := func(id, account, owner string) map[string]any {
		return map[string]any{"id": id, "account": account, "owner": owner}
	}
	o.upsert("account-opened", ev("e1", "a1", "ada"))
	o.upsert("account-opened", ev("e2", "a2", "bea"))
	o.assertMatches("opened", got, want)

	o.upsert("account-closed", map[string]any{"id": "e1", "account": "a1"}) // the opened topic's key e1 too
	o.assertMatches("a close in the other topic, colliding with an open's key", got, want)

	o.upsert("account-opened", ev("e3", "a1", "cy"))
	o.assertMatches("a re-open while the close stands", got, want)

	o.del("account-closed", "e1")
	o.assertMatches("retracting the close revives the account", got, want)

	o.upsert("account-closed", map[string]any{"id": "e9", "account": "a2"})
	o.del("account-opened", "e1")
	o.assertMatches("a delete in each source", got, want)
}

// 3. Last activity: latest over three event topics, ordered by the
// business time and tied by source() — PostgreSQL's DISTINCT ON … ORDER
// BY at DESC, src DESC (where a null time sorts first and wins).
func TestPostgreSQLIntegration_ProjUnionLastActivity(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"user_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "user_id", SQLType: "VARCHAR(32)"}, {Name: "kind", SQLType: "VARCHAR(32)"}, {Name: "at", SQLType: "TIMESTAMPTZ"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "last", FromList: []string{"logins", "orders", "tickets"}, KeyPath: []string{"$.user_id"}, Reduce: "latest",
			WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"source()"},
			Emit: []sql.StageEmit{{Field: "kind", Expr: "source()"}, {Field: "at", From: "$.at"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "last", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "kind", From: "$.kind"}, {Column: "at", From: "$.at"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"logins", "orders", "tickets"},
		`id TEXT PRIMARY KEY, user_id TEXT, at TIMESTAMPTZ`, []string{"id", "user_id", "at"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT user_id, kind, at::text FROM %q ORDER BY user_id`
	want := `SELECT DISTINCT ON (user_id) user_id, src, at::text
		FROM (SELECT 'logins' AS src, * FROM {logins} UNION ALL SELECT 'orders', * FROM {orders} UNION ALL SELECT 'tickets', * FROM {tickets}) u
		ORDER BY user_id, at DESC, src DESC`

	ev := func(id, user string, at any) map[string]any {
		return map[string]any{"id": id, "user_id": user, "at": at}
	}
	o.upsert("logins", ev("e1", "u1", "2026-01-01T10:00:00Z"))
	o.upsert("orders", ev("e1", "u1", "2026-01-01T09:00:00-05:00")) // 14:00Z: later, and logins' key e1 too
	o.upsert("tickets", ev("e1", "u2", "2026-01-01T08:00:00Z"))
	o.assertMatches("inserts across three topics", got, want)

	o.upsert("logins", ev("e3", "u1", "2026-01-02T10:00:00Z"))
	o.upsert("tickets", ev("e4", "u1", "2026-01-02T10:00:00Z"))
	o.assertMatches("a cross-topic tie on at, broken by source()", got, want)

	o.del("tickets", "e4")
	o.assertMatches("a delete hands the win back", got, want)

	o.upsert("orders", ev("e5", "u2", nil))
	o.assertMatches("a null time wins, as DESC sorts nulls first", got, want)

	o.del("orders", "e5")
	o.upsert("logins", ev("e3", "u1", "2025-12-31T10:00:00Z"))
	o.assertMatches("a delete in another source and an update", got, want)
}

// latest's list form on a single topic: orderBy [at desc, priority] is the
// LAST row in that order — the earliest time, then the highest priority —
// PostgreSQL's DISTINCT ON … ORDER BY at ASC, priority DESC, id DESC, nulls
// included (each key's direction and nulls placement reverse together).
func TestPostgreSQLIntegration_ProjLatestOrderList(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"k"},
		Columns:    []sql.ProjectionColumn{{Name: "k", SQLType: "VARCHAR(32)"}, {Name: "id", SQLType: "VARCHAR(32)"}},
		Stages: []sql.ProjectionStage{{
			Name: "first", From: "tasks", KeyPath: []string{"$.k"}, Reduce: "latest",
			WindowOrderBy: []string{"$.at::timestamptz desc", "$.priority"}, WindowTieBy: []string{"$.id"},
			Emit: []sql.StageEmit{{Field: "id", From: "$.id"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "first", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "id", From: "$.id"}}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"tasks"},
		`id TEXT PRIMARY KEY, k TEXT, at TIMESTAMPTZ, priority INT`, []string{"id", "k", "at", "priority"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT k, id FROM %q ORDER BY k`
	want := `SELECT DISTINCT ON (k) k, id FROM {tasks} ORDER BY k, at ASC, priority DESC, id COLLATE "C" DESC`

	task := func(id, k string, at, priority any) map[string]any {
		return map[string]any{"id": id, "k": k, "at": at, "priority": priority}
	}
	o.upsert("tasks", task("t1", "k1", "2026-01-02T00:00:00Z", 1))
	o.upsert("tasks", task("t2", "k1", "2026-01-01T00:00:00Z", 1))
	o.upsert("tasks", task("t3", "k2", "2026-01-01T00:00:00Z", 2))
	o.assertMatches("the earliest wins", got, want)

	o.upsert("tasks", task("t4", "k1", "2026-01-01T00:00:00Z", 5))
	o.assertMatches("an equal time: the higher priority wins", got, want)

	o.upsert("tasks", task("t5", "k1", "2026-01-01T00:00:00Z", nil))
	o.upsert("tasks", task("t6", "k2", nil, 9))
	o.assertMatches("a null priority wins its tie; a null time never wins", got, want)

	o.upsert("tasks", task("t7", "k2", "2026-01-01T00:00:00Z", 2))
	o.assertMatches("a full tie falls to the tie key", got, want)

	o.del("tasks", "t5")
	o.assertMatches("a delete", got, want)
}

// A window over a union: a cross-topic timeline whose rows key by
// (source, event key) — a two-column primaryKey at the table — with
// source() breaking ties in the business order.
func TestPostgreSQLIntegration_ProjUnionOrderTimeline(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"kind", "event_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "kind", SQLType: "VARCHAR(32)"},
			{Name: "event_id", SQLType: "VARCHAR(32)"},
			{Name: "order_id", SQLType: "VARCHAR(32)"},
			{Name: "secs", SQLType: "NUMERIC(18,6)"},
			{Name: "prev", SQLType: "VARCHAR(32)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "timeline", FromList: []string{"order-placed", "order-shipped", "order-cancelled"}, Reduce: "window",
			PartitionBy: []string{"$.order_id"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"source()"},
			Emit: []sql.StageEmit{
				{Field: "order_id", From: "$.order_id"},
				{Field: "secs", Expr: "extract(epoch from ($.at::timestamptz - lag($.at::timestamptz)))"},
				{Field: "prev", Expr: "lag(source())"},
			},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "timeline", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "order_id", From: "$.order_id"}, {Column: "secs", From: "$.secs"}, {Column: "prev", From: "$.prev"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"order-placed", "order-shipped", "order-cancelled"},
		`id TEXT PRIMARY KEY, order_id TEXT, at TIMESTAMPTZ`, []string{"id", "order_id", "at"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT kind, event_id, order_id, secs::numeric(18,6)::text, prev FROM %q ORDER BY kind, event_id`
	want := `SELECT src, id, order_id, (extract(epoch from (at - lag(at) OVER w)))::numeric(18,6)::text, lag(src) OVER w
		FROM (SELECT 'order-placed' AS src, * FROM {order-placed} UNION ALL SELECT 'order-shipped', * FROM {order-shipped}
		      UNION ALL SELECT 'order-cancelled', * FROM {order-cancelled}) u
		WINDOW w AS (PARTITION BY order_id ORDER BY at, src)
		ORDER BY src, id`

	ev := func(id, order, at string) map[string]any {
		return map[string]any{"id": id, "order_id": order, "at": at}
	}
	o.upsert("order-placed", ev("e1", "o1", "2026-01-01T10:00:00Z"))
	o.upsert("order-shipped", ev("e1", "o1", "2026-01-02T12:30:00Z")) // placed's key e1 too
	o.upsert("order-placed", ev("e2", "o2", "2026-01-01T11:00:00Z"))
	o.assertMatches("in-order events across topics", got, want)

	o.upsert("order-cancelled", ev("e1", "o2", "2026-01-01T11:00:00Z"))
	o.assertMatches("a tie on at across topics, broken by source()", got, want)

	o.upsert("order-shipped", ev("e3", "o1", "2026-01-01T18:00:00Z"))
	o.assertMatches("a late event re-splits the timeline", got, want)

	o.del("order-placed", "e1")
	o.assertMatches("a delete in one source", got, want)
}
