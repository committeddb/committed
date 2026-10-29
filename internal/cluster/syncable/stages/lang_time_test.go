package stages

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// evalMaterialized evaluates src and renders the result the way a column
// or stage output stores it.
func evalMaterialized(t *testing.T, src, payload string) any {
	t.Helper()
	n, err := Compile(src)
	require.NoError(t, err, "compile %q", src)
	v, err := Eval(n, exprPayload(t, payload), nil)
	require.NoError(t, err, "eval %q", src)
	m, err := Materialize(v)
	require.NoError(t, err)
	if num, ok := m.(json.Number); ok {
		return string(num)
	}
	return m
}

// The canonical renderings — and the one divergence from PG's own text:
// a timestamptz offset is +00:00, which MySQL accepts (it rejects +00).
func TestTimeMaterialization(t *testing.T) {
	cases := map[string]string{
		"timestamptz '2024-01-15 12:30:45.5+02:00'":    "2024-01-15 10:30:45.5+00:00",
		"timestamptz '2024-01-15'":                     "2024-01-15 00:00:00+00:00",
		"timestamp '2024-01-15 12:30:45.120000'":       "2024-01-15 12:30:45.12",
		"timestamp '2024-01-15 12:30:45+05:30'":        "2024-01-15 12:30:45", // PG ignores the offset
		"date '2024-01-15T00:00:00Z'":                  "2024-01-15",          // SQL Server's date rendering
		"interval '1 year 2 months 3 days 04:05:06.5'": "1 year 2 mons 3 days 04:05:06.5",
		"interval '-1 day +2 hours'":                   "-1 days +02:00:00",
		"interval '0 days'":                            "00:00:00",
		"timestamptz '2024-01-15T12:30:45.1234567Z'":   "2024-01-15 12:30:45.123457+00:00", // µs, PG's rounding
		"cast(timestamptz '2024-01-15 10:00' as text)": "2024-01-15 10:00:00+00:00",
		"'due ' || date '2024-01-15'":                  "due 2024-01-15",
	}
	for src, want := range cases {
		require.Equal(t, want, evalMaterialized(t, src, `{}`), src)
	}
}

// PG inputs outside the ISO family are conversion failures: the
// nondeterministic words permanently, the rest as listed divergences.
func TestTimeRejectedInputs(t *testing.T) {
	for _, in := range []string{"now", "today", "tomorrow", "yesterday", "epoch", "infinity", "-infinity", "January 8, 2024", "2024-01-15 BC", "0000-01-01", "10000-01-01", "2024-01-15 12:00 America/Denver", "2024-01-15 12:00 UTC"} {
		payload := fmt.Sprintf(`{"s":%q}`, in)
		require.ErrorContains(t, evalErr(t, "cast($.s as timestamptz)", payload), "timestamptz", in)
		require.Nil(t, evalMaterialized(t, "try_cast($.s as timestamptz)", payload), in)
	}
	require.ErrorContains(t, evalErr(t, "cast($.s as interval)", `{"s":"1 fortnight"}`), "invalid input for interval")
}

func TestTimeRangeIsYears1To9999(t *testing.T) {
	require.ErrorContains(t, evalErr(t, "timestamptz '9999-12-31 23:00' + interval '1 day'", `{}`), "out of range")
	require.ErrorContains(t, evalErr(t, "timestamptz '2024-01-15' - interval '3 millennia'", `{}`), "out of range")
	require.ErrorContains(t, evalErr(t, "date '0001-01-01' - 1", `{}`), "out of range")
	require.ErrorContains(t, evalErr(t, "to_timestamp(-1000000000000)", `{}`), "out of range")
}

// PG's operator table, including how an untyped string beside a date/time
// value is read.
func TestTimeArithmetic(t *testing.T) {
	cases := map[string]any{
		"date '2024-01-31' + interval '1 month'":                "2024-02-29 00:00:00", // calendar month, clamped
		"timestamptz '2024-01-15 10:00' + '1 day'":              "2024-01-16 10:00:00+00:00",
		"timestamptz '2024-01-15 10:00' - '2024-01-14'":         "1 day 10:00:00",
		"date '2024-01-15' - '2024-01-01'":                      "14",
		"date '2024-01-15' + 1":                                 "2024-01-16",
		"1 + date '2024-01-15'":                                 "2024-01-16",
		"timestamp '2024-01-01' - timestamp '2024-01-02 03:00'": "-1 days -03:00:00",
		"interval '1 day' * 2.5":                                "2 days 12:00:00",
		"- interval '1 day 02:00'":                              "-1 days -02:00:00",
		"interval '1 mon' = interval '30 days'":                 true,
		"interval '1 day' > interval '23 hours'":                true,
		"cast($.d as date) >= '2026-01-01'":                     true,
		"cast($.d as date) in ('2026-02-01', '2026-02-02')":     true,
		"greatest(date '2024-01-15', '2024-02-01')":             "2024-02-01",
		"date '2024-01-15' = timestamp '2024-01-15'":            true,
		"extract(epoch from interval '1 year 1 mon')":           "34149600",
		"to_timestamp(1700000000.5)":                            "2023-11-14 22:13:20.5+00:00",
		"make_date(2024, 2, 29)":                                "2024-02-29",
	}
	for src, want := range cases {
		require.Equal(t, want, evalMaterialized(t, src, `{"d":"2026-02-01"}`), src)
	}
	for src, want := range map[string]string{
		"date '2024-01-15' + '1 day'":                         "operator is not unique",
		"timestamptz '2024-01-15' + timestamptz '2024-01-16'": "operator does not exist: timestamptz + timestamptz",
		"timestamptz '2024-01-15' * 2":                        "operator does not exist",
		"date '2024-01-15' + 1.5":                             "whole number of days",
		"extract(hour from date '2024-01-15')":                `unit "hour" not supported for type date`,
		"extract(timezone from timestamp '2024-01-15')":       `not supported for type timestamp`,
		"make_date(2023, 2, 29)":                              "out of range",
		"cast($.d as date) < 5":                               "same kind",
	} {
		require.ErrorContains(t, evalErr(t, src, `{"d":"2026-02-01"}`), want, src)
	}
}

func TestTimeUnitAdmission(t *testing.T) {
	for src, want := range map[string]string{
		"date_trunc('fortnight', $.t::timestamptz)": `unit "fortnight" is not recognized`,
		"date_trunc('dow', $.t::timestamptz)":       `unit "dow" is not recognized`,
		"date_trunc($.unit, $.t::timestamptz)":      "must be a string literal",
		"extract(fortnight from $.t::date)":         `unit "fortnight" is not recognized`,
		"extract(day $.t::date)":                    "expected from in extract",
		"to_timestamp($.n / 3)":                     "", // to_timestamp settles a division, like round
	} {
		_, err := Compile(src)
		if want == "" {
			require.NoError(t, err, src)
			continue
		}
		require.ErrorContains(t, err, want, src)
	}
	_, err := Compile("date_trunc('MONTHS', $.t::timestamptz)")
	require.NoError(t, err, "units are case-insensitive and take PG's plurals")
}

// Date/time values fold like any scalar: min/max and collect order them
// chronologically (one timeline for dates and timestamps), distinct
// dedupes by instant, and every output materializes canonically. This is
// also the monthly-rollup acceptance shape, with the month computed in a
// reshape stage until GROUP BY expressions land.
func TestTimeValuesFoldThroughStages(t *testing.T) {
	stages := []Stage{
		{
			Name: "paid", From: "payments", KeyPath: []string{"$.id"},
			Emit: []Emit{
				{Field: "customer", From: "$.customer"},
				{Field: "month", Expr: "date_trunc('month', cast($.paid_at as timestamptz))"},
				{Field: "amount", From: "$.amount"},
				{Field: "due", Expr: "cast($.paid_at as timestamptz) + interval '30 days'"},
			},
		},
		{
			Name: "by-customer", From: "paid", KeyPath: []string{"$.customer"}, Reduce: "aggregate",
			Emit: []Emit{
				{Field: "first", Min: "cast($.month as timestamptz)"},
				{Field: "last_due", Max: "cast($.due as timestamptz)"},
				{Field: "months", Collect: "cast($.month as date)", Distinct: true},
				{Field: "total", Sum: "$.amount"},
			},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	for i, p := range []string{
		`{"id":"p1","customer":"c1","paid_at":"2024-03-05T10:00:00Z","amount":10}`,
		`{"id":"p2","customer":"c1","paid_at":"2024-01-20 08:00:00+00","amount":5.5}`,
		`{"id":"p3","customer":"c1","paid_at":"2024-03-28 23:30:00-05:00","amount":1}`,
	} {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "payments", fmt.Append(nil, i), decodePayload(t, p))
		}))
	}
	out := func(stage, key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := g.Output(tx, stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	require.Equal(t, `{"amount":10,"customer":"c1","due":"2024-04-04 10:00:00+00:00","month":"2024-03-01 00:00:00+00:00"}`, out("paid", "p1"))
	require.Equal(t,
		`{"first":"2024-01-01 00:00:00+00:00","last_due":"2024-04-28 04:30:00+00:00","months":["2024-01-01","2024-03-01"],"total":16.5}`,
		out("by-customer", "c1"),
		"the -05:00 payment on Mar 28 is Mar 29 04:30 UTC; months dedupe by instant and sort chronologically")
}
