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

// GROUP BY <expression> against its oracle: a projection rolls payments up
// per customer per month — the month a computed stage key landing in a
// TIMESTAMPTZ primary-key column — while the same writes go to a raw
// Postgres table. After every step (inserts across offsets, a rekey into
// another month, a delete that empties a month) the projection must equal
// PostgreSQL's own GROUP BY customer, date_trunc('month', …) over the raw
// rows.
func TestPostgreSQLIntegration_ProjGroupByExpression(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	raw := table + "_raw"
	defer dropTable(t, table)
	defer dropTable(t, raw)
	_, err = db.DB.Exec(fmt.Sprintf(`CREATE TABLE %q (id TEXT PRIMARY KEY, customer TEXT, paid_at TEXT, amount NUMERIC)`, raw))
	require.NoError(t, err)

	payType := &cluster.Type{ID: "payments", Name: "Payment"}
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"customer", "month"},
		Columns: []sql.ProjectionColumn{
			{Name: "customer", SQLType: "VARCHAR(64)"},
			{Name: "month", SQLType: "TIMESTAMPTZ"},
			{Name: "total", SQLType: "DECIMAL(12,2)"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "monthly", From: "payments",
			KeyPath: []string{"$.customer", "date_trunc('month', $.paid_at::timestamptz)"},
			Reduce:  "aggregate",
			Emit:    []sql.StageEmit{{Field: "total", Sum: "$.amount"}, {Field: "n", Count: true}},
		}},
		Sources: []sql.ProjectionSource{{
			FromStage: "monthly",
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
				{Column: "total", From: "$.total"},
				{Column: "n", From: "$.n"},
			}}},
		}},
	}
	p := sql.NewProjection(db, cfg, nil, table)
	p.SetStoreDir(t.TempDir())
	require.NoError(t, p.Init())
	defer func() { _ = p.Close() }()

	var idx uint64
	upsert := func(id, customer, paidAt, amount string) {
		t.Helper()
		idx++
		payload := fmt.Sprintf(`{"id":%q,"customer":%q,"paid_at":%q,"amount":%s}`, id, customer, paidAt, amount)
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(payType, []byte(id), []byte(payload)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`INSERT INTO %q VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET customer = EXCLUDED.customer, paid_at = EXCLUDED.paid_at, amount = EXCLUDED.amount`, raw),
			id, customer, paidAt, amount)
		require.NoError(t, err)
	}
	del := func(id string) {
		t.Helper()
		idx++
		_, err := p.Sync(context.Background(), &cluster.Actual{Index: idx, Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(payType, []byte(id)),
		}})
		require.NoError(t, err)
		_, err = db.DB.Exec(fmt.Sprintf(`DELETE FROM %q WHERE id = $1`, raw), id)
		require.NoError(t, err)
	}
	// One connection pinned to UTC renders both sides' timestamps alike.
	conn, err := db.DB.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(context.Background(), `SET timezone = 'UTC'`)
	require.NoError(t, err)
	dump := func(query string) string {
		t.Helper()
		rows, err := conn.QueryContext(context.Background(), query)
		require.NoError(t, err)
		defer rows.Close()
		var lines []string
		for rows.Next() {
			var c, m, total, n string
			require.NoError(t, rows.Scan(&c, &m, &total, &n))
			lines = append(lines, strings.Join([]string{c, m, total, n}, " | "))
		}
		require.NoError(t, rows.Err())
		return strings.Join(lines, "\n")
	}
	assertMatchesOracle := func(step string) {
		t.Helper()
		got := dump(fmt.Sprintf(`SELECT customer, month::text, total::text, n::text FROM %q ORDER BY 1, 2`, table))
		want := dump(fmt.Sprintf(`SELECT customer, date_trunc('month', paid_at::timestamptz)::text, sum(amount)::numeric(12,2)::text, count(*)::text FROM %q GROUP BY 1, 2 ORDER BY 1, 2`, raw))
		require.NotEmpty(t, want, "%s: the oracle must have rows to compare", step)
		require.Equal(t, want, got, step)
	}

	upsert("p1", "c1", "2024-01-05T10:00:00Z", "10")
	upsert("p2", "c1", "2024-01-31 20:00:00-05:00", "5") // Feb 1 UTC
	upsert("p3", "c1", "2024-01-20", "2.5")
	upsert("p4", "c2", "2024-02-29 23:59:59.9999995+00", "1.25")
	upsert("p5", "c2", "2024-03-01T00:00:00.1234567Z", "4")
	assertMatchesOracle("initial inserts across offsets and renderings")

	upsert("p3", "c1", "2024-02-02", "2.5")
	assertMatchesOracle("p3 rekeys from January to February")

	del("p1")
	assertMatchesOracle("deleting January's last payment retracts its row")

	upsert("p2", "c2", "2024-03-15T08:00:00Z", "5")
	assertMatchesOracle("p2 moves customer and month at once")
}
