package stages

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// corpusStages cover every arm the fold state accounts for, both
// vocabularies, with filters, distinct, ordering, having and a stage gate —
// one stage per arm family, so an arm that falls back (an extreme tied
// across renderings) cannot hide another from the state path.
func corpusStages() map[string][]Stage {
	stage := func(emits ...Emit) []Stage {
		return []Stage{{
			Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
			When:   []WhenClause{{Expr: "coalesce($.w, true)"}},
			Having: "count(*) <> 3",
			Emit:   emits,
		}}
	}
	return map[string][]Stage{
		"counts and sums": stage(
			Emit{Field: "c", Expr: "count(*)"},
			Emit{Field: "cx", Expr: "count($.x)"},
			Emit{Field: "cdx", Expr: "count(distinct $.x)"},
			Emit{Field: "sn", Expr: "sum($.n)"},
			Emit{Field: "sdn", Expr: "sum(distinct $.n)"},
			Emit{Field: "an", Expr: "round(avg($.n), 3)"},
			Emit{Field: "fs", Expr: "sum($.n) filter (where $.f = 'y')"},
			Emit{Field: "lc", Count: true, Where: []WhenClause{{Path: "$.f", Equals: "y"}}},
			Emit{Field: "ls", Sum: "$.n"},
		),
		"item lists": stage(
			Emit{Field: "aa", Expr: "array_agg($.s order by $.o desc)"},
			Emit{Field: "ad", Expr: "array_agg(distinct $.s)"},
			Emit{Field: "at", Expr: "array_agg(case when $.f = 'y' then $.d::date else $.d::timestamp end order by $.o)"},
			Emit{Field: "adt", Expr: "array_agg(distinct case when $.f = 'y' then $.d::date else $.d::timestamp end)"},
			Emit{Field: "sa", Expr: "string_agg($.x, ',' order by $.o)"},
			Emit{Field: "first", Expr: "(array_agg($.x order by $.o))[1]"},
			Emit{Field: "lcol", Collect: "$.s"},
			Emit{Field: "lcold", Collect: "$.s", Distinct: true},
		),
		"joined": joinedStage(
			[]Join{
				{Topic: "dims", On: []string{"$.ref"}, As: "d", Where: []WhenClause{{Path: "$.on", Equals: true}}},
				{Topic: "dims", On: []string{"$.alt"}, As: "a", Optional: true},
			},
			Emit{Field: "c", Expr: "count(*)"},
			Emit{Field: "sr", Expr: "sum($.d.rate)"},
			Emit{Field: "sa", Expr: "sum($.a.rate) filter (where $.f = 'y')"},
			Emit{Field: "names", Expr: "array_agg($.d.name order by $.o)"},
			Emit{Field: "mx", Expr: "max($.a.name)"},
			Emit{Field: "ls", Sum: "$.n"},
		),
		"anti-joined": joinedStage(
			[]Join{{Topic: "dims", On: []string{"$.ref"}, Absent: true, Where: []WhenClause{{Path: "$.on", Equals: true}}}},
			Emit{Field: "c", Expr: "count(*)"},
			Emit{Field: "sn", Expr: "sum($.n)"},
			Emit{Field: "xs", Expr: "array_agg($.x order by $.o)"},
		),
		"extremes": stage(
			Emit{Field: "mn", Expr: "min($.n)"},
			Emit{Field: "mxn", Expr: "max($.n)"},
			Emit{Field: "mt", Expr: "min($.x)"},
			Emit{Field: "md", Expr: "min(case when $.f = 'y' then $.d::date else $.d::timestamp end)"},
			Emit{Field: "xd", Expr: "max(case when $.f = 'y' then $.d::timestamp else $.d::date end)"},
			Emit{Field: "lmin", Min: "$.n"},
			Emit{Field: "lmax", Max: "$.x"},
		),
	}
}

// joinedStage is corpusStages' stage with joins to the dims topic.
func joinedStage(joins []Join, emits ...Emit) []Stage {
	return []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
		When:  []WhenClause{{Expr: "coalesce($.w, true)"}},
		Joins: joins,
		Emit:  emits,
	}}
}

// corpusDim is a random dimension row: a gate the joins filter on, and
// values with nulls and equal-but-differently-written numbers.
func corpusDim(rng *rand.Rand) string {
	pick := func(vals ...string) string { return vals[rng.Intn(len(vals))] }
	return fmt.Sprintf(`{"on":%s,"rate":%s,"name":%s}`,
		pick(`true`, `true`, `false`, `null`),
		pick(`1`, `2.0`, `2`, `null`, `-0.5`),
		pick(`"m"`, `"n"`, `null`, `"N"`),
	)
}

// corpusPayload is a random input: its group k (a change rekeys it), the
// stage gate w, join references, and values with nulls,
// equal-but-differently-written numbers, mixed types, and order-key ties.
func corpusPayload(rng *rand.Rand) string {
	pick := func(vals ...string) string { return vals[rng.Intn(len(vals))] }
	return fmt.Sprintf(`{"k":%s,"w":%s,"ref":%s,"alt":%s,"n":%s,"x":%s,"s":%s,"o":%s,"f":%s,"d":%s}`,
		pick(`"g0"`, `"g1"`, `"g2"`, `"G1"`),
		pick(`true`, `true`, `true`, `false`, `null`),
		pick(`"d0"`, `"d1"`, `"d2"`, `"d3"`, `null`),
		pick(`"d0"`, `"d1"`, `"d4"`, `null`),
		pick(`1`, `2`, `2.0`, `2.50`, `-3`, `null`, `"x"`, `12345678901234567890.5`),
		pick(`"a"`, `"b"`, `"B"`, `""`, `null`),
		pick(`"p"`, `"q"`, `1`, `true`, `null`),
		pick(`1`, `2`, `2`, `null`, `"z"`),
		pick(`"y"`, `"n"`),
		pick(`"2026-01-02"`, `"2026-01-03"`, `"2026-01-02"`),
	)
}

// TestIncrementalAggregateMatchesFullRefold replays random appends,
// overwrites, deletes, rekeys, gate changes and epoch sweeps in random
// batches, and after every drain requires each key's stored output — from
// its fold state — to equal the plain refold over the same retained inputs.
func TestIncrementalAggregateMatchesFullRefold(t *testing.T) {
	for name, sts := range corpusStages() {
		t.Run(name, func(t *testing.T) { runIncrementalCorpus(t, sts) })
	}
}

func runIncrementalCorpus(t *testing.T, sts []Stage) {
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	st := g.byName["g"].def
	require.True(t, st.incrementalAggregate())
	store := openTestStore(t, Fingerprint(sts))

	rng := rand.New(rand.NewSource(7))
	epoch := uint64(1)
	for batch := range 600 {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			for range 1 + rng.Intn(6) {
				row := fmt.Sprintf("r%02d", rng.Intn(24))
				if len(st.Joins) > 0 && rng.Intn(4) == 0 {
					dim := fmt.Sprintf("d%d", rng.Intn(5))
					if rng.Intn(4) == 0 {
						if err := g.FoldTopicDelete(tx, "dims", []byte(dim), dirty); err != nil {
							return err
						}
					} else if err := g.FoldTopicUpsert(tx, "dims", []byte(dim), decodePayload(t, corpusDim(rng)), epoch, dirty); err != nil {
						return err
					}
					continue
				}
				switch r := rng.Intn(20); {
				case r < 14:
					if err := g.FoldTopicUpsert(tx, "rows", []byte(row), decodePayload(t, corpusPayload(rng)), epoch, dirty); err != nil {
						return err
					}
				case r < 19:
					if err := g.FoldTopicDelete(tx, "rows", []byte(row), dirty); err != nil {
						return err
					}
				default:
					epoch++
					if err := g.SweepEpochs(tx, "rows", epoch-uint64(rng.Intn(2)), dirty); err != nil {
						return err
					}
				}
			}
			return g.Drain(tx, dirty)
		}), "batch %d", batch)

		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			for _, k := range []string{"g0", "g1", "g2"} {
				got, err := tx.GetOut("g", []byte(k))
				require.NoError(t, err)
				want, live, err := g.refoldAggregate(tx, st, []byte(k))
				require.NoError(t, err)
				if !live {
					require.Nil(t, got, "batch %d key %s: output present, the refold says not live", batch, k)
					continue
				}
				require.Equal(t, string(want), string(got), "batch %d key %s", batch, k)
			}
			return nil
		}))
		requireObjectsMatchBytes(t, store, g, st, batch)
	}
	n := g.byName["g"]
	t.Logf("refolds: %d from fold state, %d in full", n.foldStateRefolds.Load(), n.fullRefolds.Load())
	// The corpus is built to hit fallbacks (ties across renderings,
	// removed extremes, three hot keys); it must still exercise the state.
	require.Greater(t, n.foldStateRefolds.Load(), int64(100), "the corpus never exercised the fold state")
}

// TestIncrementalAggregateRecipeShape: the event-store recipe's stage —
// filtered array_aggs, filtered sums, a max — over streams growing by
// appends refolds from its fold state, not in full, after a key's first.
func TestIncrementalAggregateRecipeShape(t *testing.T) {
	sts := []Stage{{
		Name: "order", From: "rows", KeyPath: []string{"$.stream"}, Reduce: "aggregate",
		Emit: []Emit{
			{Field: "customer", Expr: "(array_agg($.customer) filter (where $.type = 'placed'))[1]"},
			{Field: "status", Expr: "(array_agg($.type order by $.version desc) filter (where $.type in ('placed', 'shipped', 'cancelled')))[1]"},
			{Field: "items", Expr: "coalesce(sum($.qty) filter (where $.type = 'item'), 0)"},
			{Field: "total", Expr: "coalesce(sum($.qty * $.price) filter (where $.type = 'item'), 0)"},
			{Field: "version", Expr: "max($.version)"},
		},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	st := g.byName["order"].def
	store := openTestStore(t, Fingerprint(sts))

	const streams, events = 4, 300
	for v := 1; v <= events; v++ {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			for s := range streams {
				typ := "item"
				switch v {
				case 1:
					typ = "placed"
				case events:
					typ = "shipped"
				}
				payload := fmt.Sprintf(`{"stream":"s%d","version":%d,"type":%q,"customer":"c%d","qty":%d,"price":1.25}`, s, v, typ, s, v%7)
				if err := g.FoldTopicUpsert(tx, "rows", fmt.Appendf(nil, "s%d-%04d", s, v), decodePayload(t, payload), 0, dirty); err != nil {
					return err
				}
			}
			return g.Drain(tx, dirty)
		}))
	}
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		for s := range streams {
			key := fmt.Appendf(nil, "s%d", s)
			got, err := tx.GetOut("order", key)
			require.NoError(t, err)
			want, live, err := g.refoldAggregate(tx, st, key)
			require.NoError(t, err)
			require.True(t, live)
			require.Equal(t, string(want), string(got))
		}
		return nil
	}))
	n := g.byName["order"]
	require.Equal(t, int64(streams), n.fullRefolds.Load(), "only each stream's first event refolds in full")
	require.Equal(t, int64(streams*(events-1)), n.foldStateRefolds.Load())
}

// TestIncrementalAggregateStateMatchesRebuild: after a long random run, each
// key's stored fold state equals the state a fresh full refold builds.
func TestIncrementalAggregateStateMatchesRebuild(t *testing.T) {
	sts := corpusStages()["item lists"]
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	st := g.byName["g"].def
	store := openTestStore(t, Fingerprint(sts))

	rng := rand.New(rand.NewSource(11))
	for range 400 {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			row := fmt.Sprintf("r%02d", rng.Intn(24))
			var err error
			if rng.Intn(5) == 0 {
				err = g.FoldTopicDelete(tx, "rows", []byte(row), dirty)
			} else {
				err = g.FoldTopicUpsert(tx, "rows", []byte(row), decodePayload(t, corpusPayload(rng)), 1, dirty)
			}
			if err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		for _, k := range []string{"g0", "g1", "g2"} {
			stored, err := tx.GetAcc("g", []byte(k))
			require.NoError(t, err)
			stored = bytes.Clone(stored)
			_, _, _, err = g.refoldAggregateFull(tx, st, []byte(k))
			require.NoError(t, err)
			rebuilt, err := tx.GetAcc("g", []byte(k))
			require.NoError(t, err)
			if stored == nil || rebuilt == nil {
				continue // the key fell back (a tie the state cannot order), or has no inputs
			}
			require.Equal(t, string(rebuilt), string(stored), "key %s", k)
		}
		return nil
	}))
}

// The fallback cases refold in full and leave no stale state behind.
func TestIncrementalAggregateFallbacks(t *testing.T) {
	sts := []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "aggregate",
		Emit: []Emit{
			{Field: "m", Expr: "min(case when $.f = 'y' then $.d::date else $.d::timestamp end)"},
			{Field: "iv", Expr: "sum($.i::interval)"},
		},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := openTestStore(t, Fingerprint(sts))
	fold := func(row, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			if err := g.FoldTopicUpsert(tx, "rows", []byte(row), decodePayload(t, payload), 0, dirty); err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	fold("r1", `{"k":"a","f":"n","d":"2026-01-02","i":"1 day"}`)
	fold("r2", `{"k":"a","f":"y","d":"2026-01-02","i":"2 days"}`)
	st := g.byName["g"].def
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		got, err := tx.GetOut("g", []byte("a"))
		require.NoError(t, err)
		want, live, err := g.refoldAggregate(tx, st, []byte("a"))
		require.NoError(t, err)
		require.True(t, live)
		require.Equal(t, string(want), string(got), "the tie and the interval sum refold in full")
		require.Contains(t, string(got), `"m":"2026-01-02 00:00:00"`, "the first in input-key order wins the tie")
		state, err := tx.GetAcc("g", []byte("a"))
		require.NoError(t, err)
		require.Nil(t, state, "a key the state cannot account for keeps none")
		return nil
	}))
}

// errRollback discards a check's writes.
var errRollback = errors.New("rollback")

// requireObjectsMatchBytes: wherever the state path hands publishOutput the
// rendered object in place of its bytes, the object is exactly what decoding
// the bytes yields — so consumers see what they would have decoded.
func requireObjectsMatchBytes(t *testing.T, store *stagestore.Store, g *Graph, st *Stage, batch int) {
	t.Helper()
	err := store.Update(func(tx *stagestore.Tx) error {
		for _, k := range []string{"g0", "g1", "g2"} {
			out, obj, live, err := g.refoldAggregateFull(tx, st, []byte(k))
			require.NoError(t, err)
			if !live || obj == nil {
				continue
			}
			want, err := DecodeObject(out)
			require.NoError(t, err)
			require.Equal(t, want, obj, "batch %d key %s", batch, k)
		}
		return errRollback
	})
	require.ErrorIs(t, err, errRollback)
}
