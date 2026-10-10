package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func aggStage(emits []Emit, having string) []Stage {
	return []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
		Emit: emits, Having: having,
	}}
}

func TestAggregateExpressionAdmission(t *testing.T) {
	ok := func(src string) error {
		return ValidateShapes(aggStage([]Emit{{Field: "v", Expr: src}}, ""))
	}
	for _, src := range []string{
		"round(sum($.rev) / nullif(sum($.cost), 0), 4)",
		"round(avg($.x), 2)",
		"count(*)",
		"count(distinct lower($.payer))",
		"sum($.x) filter (where $.status = 'done')",
		"COUNT(*) FILTER (WHERE $.x > 1)",
		"'constant'",
		"coalesce(max($.at::date), date '2024-01-01')",
		"case when count(*) > 10 then 'busy' else 'quiet' end",
		"sum(distinct $.x)",
	} {
		require.NoError(t, ok(src), src)
	}
	for src, want := range map[string]string{
		"$.x":                                  "outside an aggregate",
		"sum($.x) + $.y":                       "outside an aggregate",
		"avg($.x)":                             "avg(...) is an exact quotient",
		"sum(sum($.x))":                        "nested inside another aggregate",
		"sum($.x) filter (where sum($.y) > 1)": "nested inside another aggregate",
		"min(distinct $.x)":                    "min does not take distinct",
		"sum(*)":                               "unexpected",
		"sum($.x) filter ($.y > 1)":            "expected where in filter",
		"sum($.x / 3)":                         "round(...) or trunc(...)",
	} {
		require.ErrorContains(t, ok(src), want, src)
	}

	// Aggregates live only in an aggregate stage's emit expr and having.
	require.ErrorContains(t, ValidateShapes([]Stage{{Name: "r", From: "rows", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "v", Expr: "sum($.x)"}}}}), "aggregates belong in an aggregate stage")
	require.ErrorContains(t, ValidateShapes([]Stage{{Name: "r", From: "rows", KeyPath: []string{"$.k"}, When: []WhenClause{{Expr: "count(*) > 1"}}, Emit: []Emit{{Field: "v", From: "$.v"}}}}), "aggregates belong in an aggregate stage")
	require.ErrorContains(t, ValidateShapes(aggStage([]Emit{{Field: "v", Sum: "sum($.x)"}}, "")), "aggregates belong in an aggregate stage", "a fold arm's argument is per-input")
	require.ErrorContains(t, ValidateShapes(aggStage([]Emit{{Field: "v", Expr: "count(*)", Where: []WhenClause{{Path: "$.x", Equals: 1}}}}, "")), "filter each aggregate call")
	require.ErrorContains(t, ValidateShapes(aggStage([]Emit{{Field: "v", From: "$.x"}}, "")), "exactly one of sum, min, max, count, collect, or an expr")

	// having: aggregate stages only, aggregate expression rules.
	require.NoError(t, ValidateShapes(aggStage([]Emit{{Field: "n", Count: true}}, "count(*) > 5 and avg($.x) > 1.5")))
	require.ErrorContains(t, ValidateShapes(aggStage([]Emit{{Field: "n", Count: true}}, "$.x > 1")), "outside an aggregate")
	require.ErrorContains(t, ValidateShapes([]Stage{{Name: "r", From: "rows", KeyPath: []string{"$.k"}, Having: "count(*) > 1", Emit: []Emit{{Field: "v", From: "$.v"}}}}), "having filters an aggregate stage's folded keys")

	// Stage-fed: an aggregate's argument reads only what the upstream emits.
	err := ValidateShapes([]Stage{
		{Name: "a", From: "rows", KeyPath: []string{"$.id"}, Emit: []Emit{{Field: "amt", From: "$.amount"}}},
		{Name: "b", From: "a", KeyPath: []string{"'all'"}, Reduce: "aggregate", Emit: []Emit{{Field: "t", Expr: "sum($.amount)"}}},
	})
	require.ErrorContains(t, err, "amount")
	err = ValidateShapes([]Stage{
		{Name: "a", From: "rows", KeyPath: []string{"$.id"}, Emit: []Emit{{Field: "amt", From: "$.amount"}}},
		{Name: "b", From: "a", KeyPath: []string{"'all'"}, Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}}, Having: "sum($.amount) > 0"},
	})
	require.ErrorContains(t, err, "amount", "having's paths are checked too")
}

type aggHarness struct {
	t     *testing.T
	g     *Graph
	store *stagestore.Store
}

func newAggHarness(t *testing.T, stages []Stage) *aggHarness {
	t.Helper()
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	return &aggHarness{t, buildStageGraph(cfg.Stages), stageStoreForTest(t)}
}

func (h *aggHarness) upsert(key, payload string) error {
	return h.store.Update(func(tx *stagestore.Tx) error {
		dirty := Dirty{}
		if err := h.g.FoldTopicUpsert(tx, "rows", []byte(key), decodePayload(h.t, payload), 0, dirty); err != nil {
			return err
		}
		return h.g.Drain(tx, dirty)
	})
}

func (h *aggHarness) del(key string) {
	require.NoError(h.t, h.store.Update(func(tx *stagestore.Tx) error {
		dirty := Dirty{}
		if err := h.g.FoldTopicDelete(tx, "rows", []byte(key), dirty); err != nil {
			return err
		}
		return h.g.Drain(tx, dirty)
	}))
}

func (h *aggHarness) out(stage, key string) string {
	var got string
	require.NoError(h.t, h.store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut(stage, []byte(key))
		got = string(v)
		return err
	}))
	return got
}

// The select list over a GROUP BY, in one stage: a ratio of sums, an avg
// under round, FILTER, a distinct count, and a constant — alongside a
// classic fold arm.
func TestAggregateExpressionsFold(t *testing.T) {
	h := newAggHarness(t, aggStage([]Emit{
		{Field: "margin", Expr: "round((sum($.rev) - sum($.cost)) / nullif(sum($.rev), 0), 4)"},
		{Field: "avg_rate", Expr: "round(avg($.rate), 2)"},
		{Field: "done", Expr: "count(*) filter (where $.status = 'done')"},
		{Field: "done_rev", Expr: "sum($.rev) filter (where $.status = 'done')"},
		{Field: "techs", Expr: "count(distinct lower($.tech))"},
		{Field: "rated", Expr: "count($.rate)"},
		{Field: "source", Expr: "'teamup'"},
		{Field: "n", Count: true},
	}, ""))
	require.NoError(t, h.upsert("r1", `{"k":"j1","rev":100,"cost":60,"rate":10,"status":"done","tech":"Ada"}`))
	require.NoError(t, h.upsert("r2", `{"k":"j1","rev":50,"cost":45,"rate":15.555,"status":"open","tech":"ada"}`))
	require.NoError(t, h.upsert("r3", `{"k":"j1","rev":0,"cost":5,"status":"open","tech":"Bea"}`))
	require.Equal(t,
		`{"avg_rate":12.78,"done":1,"done_rev":100,"margin":0.2667,"n":3,"rated":2,"source":"teamup","techs":2}`,
		h.out("g", "j1"), "avg skips the null rate; distinct compares lower(tech); FILTER counts only done")

	// Empty-set semantics inside a non-empty key: a filter that admits
	// nothing makes sum null and count 0.
	h.del("r1")
	require.Equal(t,
		`{"avg_rate":15.56,"done":0,"done_rev":null,"margin":0,"n":2,"rated":1,"source":"teamup","techs":2}`,
		h.out("g", "j1"))
}

// HAVING: a key whose predicate is not TRUE has no output — it retracts
// downstream like an emptied key, re-enters when the predicate flips
// back, and is counted so "filtered" is told apart from "no inputs".
func TestAggregateHaving(t *testing.T) {
	stages := []Stage{
		{
			Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
			Emit:   []Emit{{Field: "total", Sum: "$.amt"}},
			Having: "sum($.amt) > 0 and count(*) >= 2",
		},
		{Name: "down", From: "g", KeyPath: []string{"'all'"}, Reduce: "aggregate", Emit: []Emit{{Field: "keys", Count: true}}},
	}
	h := newAggHarness(t, stages)
	require.NoError(t, h.upsert("r1", `{"k":"a","amt":5}`))
	require.Empty(t, h.out("g", "a"), "one input: count(*) >= 2 fails")
	require.Empty(t, h.out("down", "all"))

	require.NoError(t, h.upsert("r2", `{"k":"a","amt":3}`))
	require.Equal(t, `{"total":8}`, h.out("g", "a"))
	require.Equal(t, `{"keys":1}`, h.out("down", "all"), "the key entered downstream")

	require.NoError(t, h.upsert("r2", `{"k":"a","amt":-9}`))
	require.Empty(t, h.out("g", "a"), "sum went negative: held back")
	require.Empty(t, h.out("down", "all"), "…and retracted downstream")

	require.NoError(t, h.upsert("r2", `{"k":"a","amt":1}`))
	require.Equal(t, `{"total":6}`, h.out("g", "a"), "flipped back in")
	require.Equal(t, `{"keys":1}`, h.out("down", "all"))

	require.Positive(t, h.g.FlowCounts()["g"].HavingRejected)
}

// sum folds intervals (PG's sum(interval)); avg over intervals and a mix
// of numbers and intervals are loud data errors.
func TestAggregateOverIntervals(t *testing.T) {
	h := newAggHarness(t, aggStage([]Emit{{Field: "spent", Expr: "sum($.dur::interval)"}}, ""))
	require.NoError(t, h.upsert("r1", `{"k":"a","dur":"1 day 02:00"}`))
	require.NoError(t, h.upsert("r2", `{"k":"a","dur":"23:00"}`))
	require.Equal(t, `{"spent":"1 day 25:00:00"}`, h.out("g", "a"), "PG's interval sum: days and time stay apart")

	h2 := newAggHarness(t, aggStage([]Emit{{Field: "v", Expr: "round(avg($.dur::interval), 0)"}}, ""))
	require.ErrorContains(t, h2.upsert("r1", `{"k":"a","dur":"1 day"}`), "avg needs numbers")

	h3 := newAggHarness(t, aggStage([]Emit{{Field: "v", Expr: "sum(case when $.n then $.x::interval else $.x::numeric end)"}}, ""))
	require.NoError(t, h3.upsert("r1", `{"k":"a","n":true,"x":"1 day"}`))
	require.ErrorContains(t, h3.upsert("r2", `{"k":"a","n":false,"x":"5"}`), "mixes numbers and intervals")
}
