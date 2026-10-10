//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// Date/time against its oracle: PostgreSQL 16 in the UTC session ingest
// pins. The inputs are every rendering the ingest survey found in real
// logs (PG, MySQL, SQL Server, producer RFC 3339) plus PG's interval
// spellings; each expression runs through committed's evaluator and
// through PG, and the results must agree row for row. One rendering
// difference is deliberate and normalized here: committed writes a
// timestamptz offset as +00:00 (MySQL rejects PG's +00).
func TestPostgreSQLIntegration_ExprTimeParity(t *testing.T) {
	ctx := context.Background()
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, `SET timezone = 'UTC'`)
	require.NoError(t, err)

	table := uniqueTable(t)
	defer dropTable(t, table)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (id INT PRIMARY KEY, s TEXT, t TEXT, t2 TEXT, iv TEXT, n NUMERIC)`, table))
	require.NoError(t, err)

	// s: candidate timestamp/date inputs (valid and not); t, t2: valid
	// timestamps; iv: interval inputs; n: numbers.
	stamps := []string{
		"2024-01-15 07:00:45.123456+00", "2024-01-15 12:30:45+00", "2024-01-15 12:30:45.5",
		"2024-01-15", "2024-01-15 12:30:45.000000", "2024-01-15T12:30:45.1234567Z",
		"2024-01-15T12:30:45.1234565Z", "2024-01-15T12:30:45.1234567+05:30", "2024-01-15T00:00:00Z",
		"0001-01-01T12:30:45.5Z", "2024-01-15T12:30:45Z", "2024-01-15T12:30:45.123Z",
		"2024-01-15T12:30:45+02:00", "2024-01-15 12:30:45 +05:30", "2024-01-15 12:30:45-0800",
		"2024-01-15 9:05", "2024-02-29 23:59:59.9999995", "2024-01-15 24:00:00", "2023-12-31 23:59:60",
		"2024-03-31T01:02:03-11", "9999-12-31 23:59:59.999999", "0001-01-01",
		"2024-02-30", "2024-13-01", "2024-01-15 25:00", "2024-01-15 12:60", "not a date", "",
		"2024-01-15T", "2024-01-15 12", "2024-1-15",
	}
	ts := []string{
		"2024-01-31 10:15:30.25+00", "2024-02-29 00:00:00+00", "2023-12-31 23:59:59.999999+00",
		"2024-01-14 10:00:00+00", "2020-06-15 12:00:00+00", "2001-01-01 00:00:00+00",
	}
	ivs := []string{
		"30 days", "1 day 02:00", "2 hours 30 mins", "1.5 years", "1.3 years", "1.5 months",
		"-1.5 days", "P1Y2M3DT4H5M6.5S", "P2W", "2 weeks ago", "@ 1 day ago", "5", "1 day 2",
		"1:30", "-1:30", "1 day -1:30", "25 hours", "0.1234565 seconds", "1 mon -2 days",
		"-1 year +2 mons", "1 week 1.5 days", "P0.5Y", "PT1.5H", "1 millennium",
		"1 decade 2 centuries", "100 microseconds", "1.0000005 seconds", "0.7 months",
		"-0.3 weeks", "1 year 2 mons 3 days 04:05:06.5", "00:00:00", "nonsense", "1 fortnight",
	}
	nums := []string{"1700000000.5", "0", "-86400", "1234567890.123456", "2.5", "-0.3", "3"}
	n := len(stamps)
	for _, l := range [][]string{ts, ivs, nums} {
		n = max(n, len(l))
	}
	for i := range n {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %q VALUES ($1, $2, $3, $4, $5, $6)`, table),
			i+1, stamps[i%len(stamps)], ts[i%len(ts)], ts[(i+2)%len(ts)], ivs[i%len(ivs)], nums[i%len(nums)])
		require.NoError(t, err)
	}
	rows := make([]map[string]any, n)
	for i := range n {
		rows[i] = map[string]any{
			"s": stamps[i%len(stamps)], "t": ts[i%len(ts)], "t2": ts[(i+2)%len(ts)],
			"iv": ivs[i%len(ivs)], "n": json.Number(nums[i%len(nums)]),
		}
	}

	guard := func(col, typ string) string {
		return fmt.Sprintf("case when pg_input_is_valid(%s, '%s') then %s::%s end", col, typ, col, typ)
	}
	type timeCase struct{ ours, pg string }
	base := []timeCase{
		{"try_cast($.s as timestamptz)", guard("s", "timestamptz")},
		{"try_cast($.s as timestamp)", guard("s", "timestamp")},
		{"try_cast($.s as date)", guard("s", "date")},
		{"try_cast($.iv as interval)", guard("iv", "interval")},
		{"$.t::timestamptz - $.t2::timestamptz", "t::timestamptz - t2::timestamptz"},
		{"$.t2::timestamp - $.t::timestamp", "t2::timestamp - t::timestamp"},
		{"$.t::timestamptz + try_cast($.iv as interval)", "t::timestamptz + " + guard("iv", "interval")},
		{"$.t::timestamptz - try_cast($.iv as interval)", "t::timestamptz - " + guard("iv", "interval")},
		{"$.t::date + try_cast($.iv as interval)", "t::date + " + guard("iv", "interval")},
		{"try_cast($.iv as interval) * 2.5", guard("iv", "interval") + " * 2.5"},
		{"try_cast($.iv as interval) * -0.3", guard("iv", "interval") + " * -0.3"},
		{"try_cast($.iv as interval) * 1.1", guard("iv", "interval") + " * 1.1"},
		{"try_cast($.iv as interval) * 3", guard("iv", "interval") + " * 3"},
		{"-try_cast($.iv as interval)", "-" + guard("iv", "interval")},
		{"try_cast($.iv as interval) + interval '1 mon 1 day 01:00'", guard("iv", "interval") + " + interval '1 mon 1 day 01:00'"},
		{"$.t::date - $.t2::date", "t::date - t2::date"},
		{"$.t::date + 40", "t::date + 40"},
		{"$.t::date - 400", "t::date - 400"},
		{"$.t::timestamptz < $.t2::timestamptz", "t::timestamptz < t2::timestamptz"},
		{"$.t::date = $.t2::timestamp", "t::date = t2::timestamp"},
		{"try_cast($.iv as interval) > interval '30 days'", guard("iv", "interval") + " > interval '30 days'"},
		{"try_cast($.iv as interval) = interval '1 mon'", guard("iv", "interval") + " = interval '1 mon'"},
		{"$.t::timestamptz + '1 day 02:00'", "t::timestamptz + '1 day 02:00'"},
		{"$.t::timestamptz - '2024-01-01'", "t::timestamptz - '2024-01-01'"},
		{"$.t::date - '2024-01-01'", "t::date - '2024-01-01'"},
		{"$.t::timestamptz >= '2024-01-15'", "t::timestamptz >= '2024-01-15'"},
		{"greatest($.t::date, '2024-02-01', $.t2::date)", "greatest(t::date, '2024-02-01', t2::date)"},
		{"to_timestamp($.n)", "to_timestamp(n)"},
		{"make_date(2024, 2, 29)", "make_date(2024, 2, 29)"},
		{"$.t::timestamptz::date", "t::timestamptz::date"},
		{"$.t::date::timestamptz", "t::date::timestamptz"},
		{"cast($.t::timestamptz as text)", "cast(t::timestamptz as text)"},
		{"'at ' || $.t::date", "'at ' || t::date"},
	}
	cases := make([]timeCase, 0, len(base)+64)
	cases = append(cases, base...)
	for _, unit := range []string{"microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter", "year", "decade", "century", "millennium"} {
		cases = append(cases,
			timeCase{fmt.Sprintf("date_trunc('%s', $.t::timestamptz)", unit), fmt.Sprintf("date_trunc('%s', t::timestamptz)", unit)},
			timeCase{fmt.Sprintf("date_trunc('%s', $.t::timestamp)", unit), fmt.Sprintf("date_trunc('%s', t::timestamp)", unit)},
		)
	}
	cases = append(cases, timeCase{"date_trunc('month', $.t::date)", "date_trunc('month', t::date)"})
	for _, field := range []string{"microseconds", "milliseconds", "second", "minute", "hour", "day", "month", "quarter", "year", "decade", "century", "millennium", "week", "isoyear", "dow", "isodow", "doy", "epoch", "timezone"} {
		cases = append(cases, timeCase{fmt.Sprintf("extract(%s from $.t::timestamptz)", field), fmt.Sprintf("extract(%s from t::timestamptz)", field)})
	}
	for _, field := range []string{"day", "month", "year", "week", "dow", "doy", "epoch", "quarter"} {
		cases = append(cases, timeCase{fmt.Sprintf("date_part('%s', $.t::date)", field), fmt.Sprintf("extract(%s from t::date)", field)})
	}
	for _, field := range []string{"epoch", "year", "month", "day", "hour", "minute", "second", "milliseconds", "microseconds"} {
		cases = append(cases, timeCase{fmt.Sprintf("extract(%s from try_cast($.iv as interval))", field), fmt.Sprintf("extract(%s from %s)", field, guard("iv", "interval"))})
	}

	for _, c := range cases {
		node, err := stages.Compile(c.ours)
		require.NoError(t, err, c.ours)
		for i, row := range rows {
			msg := fmt.Sprintf("%s on %v", c.ours, row)
			t := &softT{T: t}
			got, err := stages.Eval(node, row, nil)
			require.NoError(t, err, msg)
			got, err = stages.Materialize(got)
			require.NoError(t, err, msg)

			var want gosql.NullString
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT (%s)::text FROM %q WHERE id = $1`, c.pg, table), i+1).Scan(&want), msg)

			switch g := got.(type) {
			case nil:
				require.False(t, want.Valid, "%s: ours null, PG %q", msg, want.String)
			case json.Number:
				require.True(t, want.Valid, "%s: ours %s, PG null", msg, g)
				gr, _ := new(big.Rat).SetString(string(g))
				wr, ok := new(big.Rat).SetString(want.String)
				require.True(t, ok, "%s: PG returned a non-number %q", msg, want.String)
				require.Zero(t, gr.Cmp(wr), "%s: ours %s, PG %s", msg, g, want.String)
			case bool:
				require.True(t, want.Valid, "%s: ours %v, PG null", msg, g)
				require.Equal(t, map[bool]string{true: "true", false: "false"}[g], want.String, msg)
			case string:
				require.True(t, want.Valid, "%s: ours %q, PG null", msg, g)
				require.Equal(t, want.String, strings.Replace(g, "+00:00", "+00", 1), msg)
			default:
				t.Fatalf("%s: unexpected value %T", msg, got)
			}
		}
	}
}

// softT keeps going after a failed assertion, so one run lists every
// divergence.
type softT struct{ *testing.T }

func (s *softT) FailNow() { s.Fail() }
