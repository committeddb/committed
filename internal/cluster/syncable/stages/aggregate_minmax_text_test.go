package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The endgame field finding: stage min/max folds were numeric-only —
// text values silently became NULL (the quoted_completed_date pull
// vanished through its max, and every interval text column with it).
// SQL's MIN/MAX order text (dates as ISO strings are the canonical
// case), so ours must: numbers numerically, strings lexically, bools
// false<true — the same total order collect sorts by. Sum stays
// numeric-only (SQL agrees); nulls skip everywhere.
func TestAggregateMinMaxText(t *testing.T) {
	fold, get, del := foldGet(t, []Stage{
		{
			Name: "quoted", From: "pairs", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "completed", Max: "$.visitDate"},
				{Field: "first", Min: "$.visitDate"},
				{Field: "amount", Sum: "$.price"},
			},
		},
	}, "pairs", "quoted")

	fold("p1", `{"job":"j1","visitDate":"2026-03-14T09:00:00Z","price":10}`)
	fold("p2", `{"job":"j1","visitDate":"2026-07-02T09:00:00Z","price":5}`)
	fold("p3", `{"job":"j1","price":1}`)
	require.Equal(t,
		`{"amount":16,"completed":"2026-07-02T09:00:00Z","first":"2026-03-14T09:00:00Z"}`,
		get("j1"), "min/max order text lexically (ISO dates); the null-dated input skips but still sums")

	// Retraction of the max re-promotes from the retained set.
	del("p2")
	require.Equal(t,
		`{"amount":11,"completed":"2026-03-14T09:00:00Z","first":"2026-03-14T09:00:00Z"}`,
		get("j1"))
}

// Numeric min/max stay numeric (10 > 9 — no lexical regression), and a
// pulled text value survives the fold (the field probe's exact shape:
// a named join's value through max).
func TestAggregateMinMaxNumericAndPulled(t *testing.T) {
	fold, get, _ := harness(t, []Stage{
		{
			Name: "m", From: "rows", KeyPath: []string{"$.k"},
			Reduce: "aggregate",
			Joins:  []Join{{Topic: "dims", On: []string{"$.ref"}, As: "d"}},
			Emit: []Emit{
				{Field: "big", Max: "$.n"},
				{Field: "latest", Max: "$.d.date"},
			},
		},
	}, "m")

	fold("dims", "d1", `{"date":"2026-01-05"}`)
	fold("dims", "d2", `{"date":"2026-02-01"}`)
	fold("rows", "r1", `{"k":"a","n":9,"ref":"d2"}`)
	fold("rows", "r2", `{"k":"a","n":10,"ref":"d1"}`)
	require.Equal(t, `{"big":10,"latest":"2026-02-01"}`, get("a"),
		"numbers compare numerically; a looked-up text value survives its fold")
}
