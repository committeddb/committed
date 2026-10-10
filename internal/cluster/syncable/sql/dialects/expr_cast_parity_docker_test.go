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

// Casts, math, and between against their oracle. try_cast is committed's
// addition; its PG equivalent is pg_input_is_valid guarding the cast, so
// every input — valid or not — compares row for row. NaN/Infinity (PG
// accepts them, exact arithmetic cannot) and numbers rendered to text
// (canonical digits vs PG's carried scale) are listed divergences, pinned
// by the stages tests rather than here.
func TestPostgreSQLIntegration_ExprCastParity(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %q (id INT PRIMARY KEY, s TEXT, n NUMERIC, m NUMERIC, b BOOLEAN)`, table))
	require.NoError(t, err)

	inputs := []string{
		"1e3", " 5 ", "0x1F", "0o17", "-0b101", "1_000.5", "-0.50", "2.5", "+.5", "5.", "1e1_0",
		"7", "-2147483648", "2147483648", "9223372036854775808", "0xFFFFFFFF",
		"1__0", "_1", "1_", "1._5", "0x1.8", "1e", "", "x", "1.2.3",
		"t", "TrUe", "tr", "yes", "on", "of", "off", "o", "n", "0", "1", "10",
	}
	nums := []string{"2.5", "-2.5", "7.49", "-7", "0", "1234567.125", "null"}
	rows := make([]string, 0, len(inputs))
	for i, s := range inputs {
		n, m := nums[i%len(nums)], nums[(i+3)%len(nums)]
		b := []string{"true", "false", "null"}[i%3]
		rows = append(rows, fmt.Sprintf(`{"id":%d,"s":%q,"n":%s,"m":%s,"b":%s}`, i+1, s, n, m, b))
	}
	for _, r := range rows {
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %q SELECT * FROM json_populate_record(null::%q, $1::json)`, table, table), r)
		require.NoError(t, err)
	}

	guarded := func(typ string) string {
		return fmt.Sprintf("case when pg_input_is_valid(s, '%s') then s::%s end", typ, typ)
	}
	cases := []struct{ ours, pg string }{
		{"try_cast($.s as numeric)", guarded("numeric")},
		{"try_cast($.s as integer)", guarded("integer")},
		{"try_cast($.s as bigint)", guarded("bigint")},
		{"try_cast($.s as boolean)", guarded("boolean")},
		{"cast($.n as integer)", "n::integer"},
		{"$.n::bigint", "n::bigint"},
		{"cast($.b as integer)", "b::integer"},
		{"cast($.b as text)", "b::text"},
		{"abs($.n)", "abs(n)"},
		{"ceil($.n)", "ceil(n)"},
		{"floor($.n)", "floor(n)"},
		{"mod($.n, 3)", "mod(n, 3)"},
		{"mod($.n, -0.75)", "mod(n, -0.75)"},
		{"greatest($.n, 0, $.m)", "greatest(n, 0, m)"},
		{"least($.n, $.m)", "least(n, m)"},
		{"$.n between -1 and 3", "n between -1 and 3"},
		{"$.n not between $.m and 3", "n not between m and 3"},
		{"$.s between 'a' and 'p'", `s COLLATE "C" between 'a' and 'p'`},
		{"greatest(try_cast($.s as numeric), 0)", "greatest(" + guarded("numeric") + ", 0)"},
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
