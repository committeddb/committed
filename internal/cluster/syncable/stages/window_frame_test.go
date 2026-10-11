package stages

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Frames over one partition, ordered by o (nulls last), tie by id:
//
//	id: 1   2   3   4   5   6
//	o:  1   2   2   4   7   null
//	v:  1   2   3   4   5   6
//
// Each column is PostgreSQL 16's own output for the same frame over
// ORDER BY o, id (ROWS) or ORDER BY o (RANGE).
func TestWindowFramesMatchPostgres(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want []string // by id 1..6
	}{
		{"sum($.v) over (rows between 1 preceding and current row)", []string{"1", "3", "5", "7", "9", "11"}},
		{"sum($.v) over (rows 1 preceding)", []string{"1", "3", "5", "7", "9", "11"}},
		{"sum($.v) over (rows between unbounded preceding and unbounded following)", []string{"21", "21", "21", "21", "21", "21"}},
		{"last_value($.v) over (rows between unbounded preceding and unbounded following)", []string{"6", "6", "6", "6", "6", "6"}},
		{"sum($.v) over (range between 1 preceding and current row)", []string{"1", "6", "6", "4", "5", "6"}},
		{"count(*) over (range between 2 preceding and 2 following)", []string{"3", "4", "4", "3", "1", "1"}},
		{"first_value($.v) over (rows between 2 following and 3 following)", []string{"3", "4", "5", "6", "null", "null"}},
		{"sum($.v) over (rows between unbounded preceding and 1 preceding)", []string{"null", "1", "3", "6", "10", "15"}},
		{"count(*) over (rows between unbounded preceding and 1 preceding)", []string{"0", "1", "2", "3", "4", "5"}},
		{"max($.v) over (range between unbounded preceding and 1 preceding)", []string{"null", "1", "1", "3", "4", "6"}},
		{"sum($.v) over (rows between current row and unbounded following)", []string{"21", "20", "18", "15", "11", "6"}},
		{"min($.v) over (range between current row and 3 following)", []string{"1", "2", "2", "4", "5", "6"}},
		{"last_value($.v) over (range between current row and 2 following)", []string{"3", "4", "4", "4", "5", "6"}},
		{"round(avg($.v) over (rows between 1 preceding and 1 following), 2)", []string{"1.5", "2", "3", "4", "5", "5.5"}},
		{"sum($.v) filter (where $.v > 2) over (rows between 2 preceding and current row)", []string{"null", "null", "3", "7", "12", "15"}},
	} {
		h := newWindowHarness(t, windowStage([]Emit{{Field: "x", Expr: tc.expr}}, "$.id"))
		for i, row := range []string{
			`{"p":"x","id":1,"o":1,"v":1}`, `{"p":"x","id":2,"o":2,"v":2}`, `{"p":"x","id":3,"o":2,"v":3}`,
			`{"p":"x","id":4,"o":4,"v":4}`, `{"p":"x","id":5,"o":7,"v":5}`, `{"p":"x","id":6,"o":null,"v":6}`,
		} {
			h.upsert(fmt.Sprint(i+1), row)
		}
		for i, want := range tc.want {
			require.JSONEq(t, `{"x":`+want+`}`, h.out(fmt.Sprint(i+1)), "%s, id %d", tc.expr, i+1)
		}
	}
}

// A rolling time window measures from each row's own timestamp — calendar
// months included — and desc orders reverse which side is preceding.
func TestWindowRangeIntervalFrames(t *testing.T) {
	st := windowStage([]Emit{
		{Field: "hour_n", Expr: "count(*) over (range between interval '1 hour' preceding and current row)"},
		{Field: "month_sum", Expr: "sum($.v) over (range between interval '1 month' preceding and current row)"},
	}, "$.id")
	st.WindowOrderBy = []string{"$.at::timestamptz"}
	h := newWindowHarness(t, st)
	h.upsert("a", `{"p":"x","id":1,"at":"2024-01-31T10:00:00Z","v":1}`)
	h.upsert("b", `{"p":"x","id":2,"at":"2024-01-31T10:30:00Z","v":2}`)
	h.upsert("c", `{"p":"x","id":3,"at":"2024-01-31T11:00:00Z","v":4}`)
	h.upsert("d", `{"p":"x","id":4,"at":"2024-02-29T11:00:00Z","v":8}`)
	h.upsert("e", `{"p":"x","id":5,"at":"2024-03-01T10:59:59Z","v":16}`)
	// c: [10:00, 11:00] holds a, b, c. d: 2024-02-29 minus a month is
	// 2024-01-29 (PG's day clamp), so a..d. e: minus a month is
	// 2024-02-01 10:59:59, so d and e.
	require.JSONEq(t, `{"hour_n":3,"month_sum":7}`, h.out("c"))
	require.JSONEq(t, `{"hour_n":1,"month_sum":15}`, h.out("d"))
	require.JSONEq(t, `{"hour_n":1,"month_sum":24}`, h.out("e"))

	desc := windowStage([]Emit{{Field: "n", Expr: "count(*) over (range between 1 preceding and current row)"}}, "$.id")
	desc.WindowOrderBy = []string{"$.o desc"}
	hd := newWindowHarness(t, desc)
	for i, o := range []int{5, 4, 4, 2} {
		hd.upsert(fmt.Sprint(i), fmt.Sprintf(`{"p":"x","id":%d,"o":%d}`, i, o))
	}
	// Descending, 1 preceding means values up to o+1: 4 counts 5,4,4.
	require.JSONEq(t, `{"n":1}`, hd.out("0"))
	require.JSONEq(t, `{"n":3}`, hd.out("1"))
	require.JSONEq(t, `{"n":1}`, hd.out("3"))
}

// An append in business order recomputes only the rows its frames reach.
func TestWindowFrameAppendCost(t *testing.T) {
	for _, tc := range []struct {
		expr string
		max  int64
	}{
		{"round(avg($.v) over (rows between 6 preceding and current row), 2)", 1},
		{"count(*) over (range between 5 preceding and current row)", 1},
		{"sum($.v) over (rows between current row and 2 following)", 3},
		{"sum($.v) over (range between current row and 3 following)", 4},
	} {
		h := newWindowHarness(t, windowStage([]Emit{{Field: "x", Expr: tc.expr}}, "$.id"))
		for i := 1; i <= 40; i++ {
			h.upsert(fmt.Sprintf("r%03d", i), fmt.Sprintf(`{"p":"x","o":%d,"id":%d,"v":%d}`, i, i, i))
		}
		before := h.g.FlowCounts()["w"].WindowRowsRecomputed
		h.upsert("r041", `{"p":"x","o":41,"id":41,"v":41}`)
		require.LessOrEqual(t, h.g.FlowCounts()["w"].WindowRowsRecomputed-before, tc.max, tc.expr)

		// A mid-partition edit recomputes the rows whose frames hold it,
		// not the suffix.
		before = h.g.FlowCounts()["w"].WindowRowsRecomputed
		h.upsert("r010", `{"p":"x","o":10,"id":10,"v":100}`)
		require.LessOrEqual(t, h.g.FlowCounts()["w"].WindowRowsRecomputed-before, int64(12), "%s: an edit stays local", tc.expr)
	}
}

// The incremental recompute must equal a from-scratch build after every
// batch, for every frame shape — ascending and descending, with nulls and
// peers, so each start/stop rule meets real reach.
func TestWindowFramesIncrementalEqualsRebuild(t *testing.T) {
	emits := []Emit{
		{Field: "rows_ma", Expr: "sum($.v) over (rows between 2 preceding and current row)"},
		{Field: "rows_fwd", Expr: "sum($.v) over (rows between current row and 2 following)"},
		{Field: "rows_win", Expr: "count(*) over (rows between 1 preceding and 3 following)"},
		{Field: "rows_lagged", Expr: "sum($.v) over (rows between unbounded preceding and 2 preceding)"},
		{Field: "rows_ahead", Expr: "max($.v) over (rows between 1 following and 2 following)"},
		{Field: "range_back", Expr: "sum($.v) over (range between 2 preceding and current row)"},
		{Field: "range_fwd", Expr: "count(*) over (range between current row and 2 following)"},
		{Field: "range_both", Expr: "min($.v) over (range between 1 preceding and 1 following)"},
		{Field: "range_lag", Expr: "sum($.v) over (range between unbounded preceding and 1 preceding)"},
		{Field: "range_upto", Expr: "count(*) over (range between unbounded preceding and 2 following)"},
		{Field: "first_b", Expr: "first_value($.v) over (range between 1 preceding and current row)"},
		{Field: "last_f", Expr: "last_value($.v) over (rows between current row and 1 following)"},
		{Field: "running", Expr: "sum($.v)"},
	}
	whole := []Emit{
		{Field: "total", Expr: "sum($.v) over (rows between unbounded preceding and unbounded following)"},
		{Field: "tail", Expr: "count(*) over (range between 1 following and unbounded following)"},
		{Field: "final", Expr: "last_value($.v) over (rows between unbounded preceding and unbounded following)"},
	}
	for name, tc := range map[string]struct {
		emits []Emit
		order string
	}{
		"asc":        {emits, "$.o"},
		"desc":       {emits, "$.o desc"},
		"nullsFirst": {emits, "$.o nulls first"},
		"unbounded":  {whole, "$.o"},
	} {
		t.Run(name, func(t *testing.T) {
			stage := func() Stage {
				st := windowStage(tc.emits, "$.t")
				st.WindowOrderBy = []string{tc.order}
				return st
			}
			r := rand.New(rand.NewPCG(13, 17))
			inc := newWindowHarness(t, stage())
			live := map[string]string{}
			for step := 0; step < 200; step++ {
				var ops []windowOp
				for range 1 + r.IntN(3) {
					key := fmt.Sprintf("k%02d", r.IntN(30))
					if _, ok := live[key]; ok && r.IntN(4) == 0 {
						delete(live, key)
						ops = append(ops, windowOp{key: key})
						continue
					}
					o := fmt.Sprint(r.IntN(10))
					if r.IntN(10) == 0 {
						o = "null"
					}
					p := fmt.Sprintf(`{"p":"p%d","o":%s,"t":%d,"v":%d}`, r.IntN(2), o, r.IntN(3), r.IntN(10))
					live[key] = p
					ops = append(ops, windowOp{key, p})
				}
				require.NoError(t, inc.apply(ops...))

				fresh := newWindowHarness(t, stage())
				keys := make([]string, 0, len(live))
				for k := range live {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				all := make([]windowOp, 0, len(keys))
				for _, k := range keys {
					all = append(all, windowOp{k, live[k]})
				}
				require.NoError(t, fresh.apply(all...))
				for k := range inc.keys {
					fresh.keys[k] = true
				}
				require.Equal(t, fresh.outputs("w"), inc.outputs("w"), "step %d", step)
			}
		})
	}
}

func TestWindowFrameAdmission(t *testing.T) {
	ok := func(expr string, order ...string) error {
		st := windowStage([]Emit{{Field: "x", Expr: expr}}, "$.id")
		if len(order) > 0 {
			st.WindowOrderBy = order
		}
		return validateProjectionStageShapes(stageConfig(st))
	}
	for _, src := range []string{
		"sum($.v) over (rows between 3 preceding and 1 following)",
		"count(*) over (range between interval '30 days' preceding and current row)",
		"first_value($.v) over (rows unbounded preceding)",
		"last_value($.v) over (range between current row and unbounded following)",
		"sum($.v) filter (where $.ok) over (rows 2 preceding)",
		"SUM($.v) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
	} {
		require.NoError(t, ok(src), src)
	}
	require.NoError(t, ok("count(*) over (range between interval '1 hour' preceding and current row)", "$.at::timestamptz"))
	require.NoError(t, ok("sum($.v) over (range between 2 preceding and current row)", "$.n::numeric desc"))

	for src, want := range map[string]string{
		"sum($.v) over (rows between unbounded following and current row)": "frame start cannot be unbounded following",
		"sum($.v) over (rows between current row and unbounded preceding)": "frame end cannot be unbounded preceding",
		"sum($.v) over (rows between current row and 1 preceding)":         "cannot have preceding rows",
		"sum($.v) over (rows between 1 following and current row)":         "cannot have preceding rows",
		"sum($.v) over (rows between -1 preceding and current row)":        "integer from 0",
		"sum($.v) over (rows between 1.5 preceding and current row)":       "integer from 0",
		"sum($.v) over (rows between $.n preceding and current row)":       "constant",
		"sum($.v) over (range between 'x' preceding and current row)":      "number or an interval",
		"sum($.v) over (groups 1 preceding)":                               "groups frames",
		"sum($.v) over (partition by $.p)":                                 "takes only a frame",
		"sum($.v) over (rows 1 preceding exclude current row)":             "exclude",
		"lag($.v) over (rows 1 preceding)":                                 "does not use a frame",
	} {
		require.ErrorContains(t, ok(src), want, src)
	}
	require.ErrorContains(t, ok("sum($.v) over (range 1 preceding)", "$.a", "$.b"), "exactly one orderBy key")
	require.ErrorContains(t, ok("sum($.v) over (range 1 preceding)", "$.at::timestamptz"), "is an interval")
	require.ErrorContains(t, ok("sum($.v) over (range interval '1 day' preceding)", "$.n::numeric"), "is a number")
	require.NoError(t, ok("sum($.v) over (rows 1 preceding)", "$.a", "$.b"), "rows frames take any order")

	// An undeclared order type meets its offset on data: a mismatch is a
	// data error for that batch.
	h := newWindowHarness(t, windowStage([]Emit{{Field: "x", Expr: "sum($.v) over (range interval '1 day' preceding)"}}, "$.id"))
	require.ErrorContains(t, h.apply(windowOp{"a", `{"p":"x","o":1,"id":1,"v":1}`}), "does not apply to an orderBy number")
}
