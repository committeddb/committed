//go:build docker || integration

package dialects_test

import (
	gosql "database/sql"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// The expression language's CASE against its oracle: each expression runs
// through committed's evaluator and through PostgreSQL over the same rows,
// and the results must agree row for row (numbers compared as exact
// values, so PG's carried scale — 2.50 vs 2.5 — is not a difference).
func TestPostgreSQLIntegration_ExprCaseParity(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %q (id INT PRIMARY KEY, hours NUMERIC, status TEXT, a NUMERIC, n NUMERIC)`, table))
	require.NoError(t, err)

	// Rows cover each branch, null operands, and the zero divisor.
	rows := []string{
		`{"id":1,"hours":2,"status":"done","a":10,"n":4}`,
		`{"id":2,"hours":8,"status":"open","a":10,"n":0}`,
		`{"id":3,"hours":39.5,"status":"held","a":1,"n":3}`,
		`{"id":4,"hours":40,"a":-7,"n":2}`,
		`{"id":5,"status":"done"}`,
	}
	for _, r := range rows {
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %q SELECT * FROM json_populate_record(null::%q, $1::json)`, table, table), r)
		require.NoError(t, err)
	}

	cases := []struct{ ours, pg string }{
		{
			"case when $.hours < 8 then 'small' when $.hours < 40 then 'medium' else 'large' end",
			"case when hours < 8 then 'small' when hours < 40 then 'medium' else 'large' end",
		},
		{
			"case when $.status = 'done' then $.hours end",
			"case when status = 'done' then hours end",
		},
		{
			"case $.status when 'done' then 1 when 'open' then 2 else 0 end",
			"case status when 'done' then 1 when 'open' then 2 else 0 end",
		},
		{
			"case when $.n = 0 then null else round($.a / $.n, 2) end",
			"case when n = 0 then null else round(a / n, 2) end",
		},
		{
			"case when $.hours >= 8 then case when $.a < 0 then 'neg' else 'pos' end end",
			"case when hours >= 8 then case when a < 0 then 'neg' else 'pos' end end",
		},
		{
			"coalesce(case when $.status in ('done', 'held') then $.a * 2 end, 0) + 1",
			"coalesce(case when status in ('done', 'held') then a * 2 end, 0) + 1",
		},
		// IS [NOT] DISTINCT FROM: null-safe, never null (row 5's null
		// hours against its null n are NOT distinct).
		{
			"case when $.status is distinct from 'done' then 'd' else 's' end",
			"case when status is distinct from 'done' then 'd' else 's' end",
		},
		{
			"case when $.hours is not distinct from $.n then 'same' else 'diff' end",
			"case when hours is not distinct from n then 'same' else 'diff' end",
		},
	}

	for _, c := range cases {
		n, err := stages.Compile(c.ours)
		require.NoError(t, err, c.ours)
		for i, r := range rows {
			payload, err := stages.DecodeObject([]byte(r))
			require.NoError(t, err)
			got, err := stages.Eval(n, payload, nil)
			require.NoError(t, err, "%s on %s", c.ours, r)

			var want gosql.NullString
			require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT (%s)::text FROM %q WHERE id = $1`, c.pg, table), i+1).Scan(&want))

			msg := fmt.Sprintf("%s on %s", c.ours, r)
			if !want.Valid {
				require.Nil(t, got, msg)
				continue
			}
			if rat, ok := got.(*big.Rat); ok {
				wantRat, ok := new(big.Rat).SetString(want.String)
				require.True(t, ok, "PG returned a non-number for %s: %q", msg, want.String)
				require.Zero(t, rat.Cmp(wantRat), "%s: ours %s, PG %s", msg, rat.RatString(), want.String)
				continue
			}
			require.Equal(t, want.String, got, msg)
		}
	}
}
