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

// The string functions against their oracle: each expression runs through
// committed's evaluator and through PostgreSQL over the same rows. The
// corpus avoids the listed divergences (non-ASCII case mapping, numbers
// with trailing zeros, booleans in concat), which the stages tests pin
// on our side instead.
func TestPostgreSQLIntegration_ExprStringParity(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %q (id INT PRIMARY KEY, first TEXT, last TEXT, code TEXT, sku TEXT, n NUMERIC)`, table))
	require.NoError(t, err)

	rows := []string{
		`{"id":1,"first":"Ada","last":"Lovelace","code":"  ab-1  ","sku":"SVC-100","n":3}`,
		`{"id":2,"first":"Grace","code":"xxCODExx","sku":"svc_9","n":-2}`,
		`{"id":3,"last":"Hopper","code":"héllo wörld","sku":"PRT%1","n":0}`,
		`{"id":4,"first":"","last":"","code":"","sku":"","n":12}`,
		`{"id":5}`,
	}
	for _, r := range rows {
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %q SELECT * FROM json_populate_record(null::%q, $1::json)`, table, table), r)
		require.NoError(t, err)
	}

	cases := []struct{ ours, pg string }{
		{"$.first || ' ' || $.last", "first || ' ' || last"},
		{"concat($.first, ' ', $.last)", "concat(first, ' ', last)"},
		{"'#' || $.n + 1", "'#' || n + 1"},
		{"concat('n=', $.n)", "concat('n=', n)"},
		{"upper(trim($.code))", "upper(trim(code))"},
		{"lower($.first)", "lower(first)"},
		{"ltrim($.code)", "ltrim(code)"},
		{"rtrim($.code)", "rtrim(code)"},
		{"trim($.code, 'x')", "trim(code, 'x')"},
		{"length($.code)", "length(code)"},
		{"char_length($.code)", "char_length(code)"},
		{"substring($.code, 2, 4)", "substring(code, 2, 4)"},
		{"substring($.code, 0, 3)", "substring(code, 0, 3)"},
		{"substring($.code, -1)", "substring(code, -1)"},
		{"substring($.code, 3)", "substring(code, 3)"},
		{"replace($.code, 'x', 'Y')", "replace(code, 'x', 'Y')"},
		{"replace($.code, '', 'Y')", "replace(code, '', 'Y')"},
		{"starts_with($.sku, 'SVC')", "starts_with(sku, 'SVC')"},
		{"$.sku like 'SVC-%'", "sku like 'SVC-%'"},
		{"$.sku like 'svc\\_%'", "sku like 'svc\\_%'"},
		{"$.sku like '%\\%%'", "sku like '%\\%%'"},
		{"$.sku not like '%1%'", "sku not like '%1%'"},
		{"$.code like '%_ö%'", "code like '%_ö%'"},
		{"$.first not in ('Ada', 'Grace')", "first not in ('Ada', 'Grace')"},
		{"$.first || 'x' = 'Adax'", "first || 'x' = 'Adax'"},
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
			switch g := got.(type) {
			case nil:
				require.False(t, want.Valid, "%s: ours null, PG %q", msg, want.String)
			case *big.Rat:
				require.True(t, want.Valid, "%s: ours %s, PG null", msg, g.RatString())
				wantRat, ok := new(big.Rat).SetString(want.String)
				require.True(t, ok, "PG returned a non-number for %s: %q", msg, want.String)
				require.Zero(t, g.Cmp(wantRat), "%s: ours %s, PG %s", msg, g.RatString(), want.String)
			case bool:
				require.True(t, want.Valid, "%s: ours %v, PG null", msg, g)
				require.Equal(t, map[bool]string{true: "true", false: "false"}[g], want.String, msg)
			default:
				require.True(t, want.Valid, "%s: ours %v, PG null", msg, g)
				require.Equal(t, want.String, g, msg)
			}
		}
	}
}
