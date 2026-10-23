package stages

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The event-store recipe's aggregate stage (docs/recipes/event-store-read-models.md):
// one key per stream, five emits.
var recipeEmits = []Emit{
	{Field: "customer", Expr: "(array_agg($.data.customer) filter (where $.type = 'order_placed'))[1]"},
	{Field: "status", Expr: "(array_agg($.type order by $.version desc) filter (where $.type in ('order_placed', 'order_shipped', 'order_cancelled')))[1]"},
	{Field: "items", Expr: "coalesce(sum($.data.qty) filter (where $.type = 'item_added'), 0)"},
	{Field: "total", Expr: "coalesce(sum($.data.qty * $.data.price) filter (where $.type = 'item_added'), 0)"},
	{Field: "version", Expr: "max($.version)"},
}

// BenchmarkAggregateLargeGroup times one input's fold into a key that already
// retains n inputs: the per-input cost of a long stream. Each op, in its own
// store transaction as a live change arrives, either appends a new event
// (append) or changes the last retained one (update). The group is loaded in
// one transaction with one drain, which costs one refold rather than n.
func BenchmarkAggregateLargeGroup(b *testing.B) {
	withoutFoldStateCheck(b)
	for _, shape := range []struct {
		name  string
		emits []Emit
	}{
		{"recipe-5-emits", recipeEmits},
		{"sum-1-emit", []Emit{{Field: "items", Expr: "coalesce(sum($.data.qty), 0)"}}},
	} {
		for _, mode := range []string{"append", "update"} {
			for _, n := range []int{1000, 5000, 10000} {
				b.Run(fmt.Sprintf("%s/%s/n=%d", shape.name, mode, n), func(b *testing.B) {
					stages := []Stage{{Name: "order", From: "events", KeyPath: []string{"$.stream_id"}, Reduce: "aggregate", Emit: shape.emits}}
					cfg := stageConfig(stages...)
					if err := validateProjectionStageShapes(cfg); err != nil {
						b.Fatal(err)
					}
					g := buildStageGraph(cfg.Stages)
					store, _, err := stagestore.Open(b.TempDir(), "p", "fp")
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = store.Close() }()

					event := func(version int, qty int) any {
						typ, data := "item_added", fmt.Sprintf(`{"sku":"pen","qty":%d,"price":1.5}`, qty)
						if version == 1 {
							typ, data = "order_placed", `{"customer":"ada"}`
						}
						v, err := decodeStageObject(fmt.Appendf(nil, `{"stream_id":"s1","version":%d,"type":%q,"data":%s}`, version, typ, data))
						if err != nil {
							b.Fatal(err)
						}
						return v
					}
					if err := store.Update(func(tx *stagestore.Tx) error {
						dirty := Dirty{}
						for v := 1; v <= n; v++ {
							if err := g.FoldTopicUpsert(tx, "events", fmt.Appendf(nil, "%d", v), event(v, 1), 0, dirty); err != nil {
								return err
							}
						}
						return g.Drain(tx, dirty)
					}); err != nil {
						b.Fatal(err)
					}

					next := n
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						next++
						key, payload := fmt.Appendf(nil, "%d", next), event(next, 1)
						if mode == "update" {
							key, payload = fmt.Appendf(nil, "%d", n), event(n, next%5+1)
						}
						if err := store.Update(func(tx *stagestore.Tx) error {
							dirty := Dirty{}
							if err := g.FoldTopicUpsert(tx, "events", key, payload, 0, dirty); err != nil {
								return err
							}
							return g.Drain(tx, dirty)
						}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkAggregateLiveBatches folds the recipe's stage the way a caught-up
// projection does under load: batches of 500 committed Actuals, one event
// each, appended to random streams among 20,000, each batch one store
// transaction and one drain. It reports CPU per event and the store's bytes
// per event on disk.
func BenchmarkAggregateLiveBatches(b *testing.B) {
	withoutFoldStateCheck(b)
	const streams, batch = 20000, 500
	stages := []Stage{{Name: "order", From: "events", KeyPath: []string{"$.stream_id"}, Reduce: "aggregate", Emit: recipeEmits}}
	cfg := stageConfig(stages...)
	if err := validateProjectionStageShapes(cfg); err != nil {
		b.Fatal(err)
	}
	g := buildStageGraph(cfg.Stages)
	dir := b.TempDir()
	store, _, err := stagestore.Open(dir, "p", "fp")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	typ := &cluster.Type{ID: "events", Name: "events"}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // G404: a reproducible workload
	versions := make([]int, streams)
	index := uint64(0)
	next := func() *cluster.Actual {
		s := rng.Intn(streams)
		versions[s]++
		v := versions[s]
		kind, data := "item_added", fmt.Sprintf(`{"sku":"pen","qty":%d,"price":1.5}`, 1+rng.Intn(5))
		if v == 1 {
			kind, data = "order_placed", `{"customer":"ada"}`
		}
		index++
		payload := fmt.Appendf(nil, `{"stream_id":"s%d","version":%d,"type":%q,"data":%s}`, s, v, kind, data)
		return &cluster.Actual{Index: index, Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(typ, fmt.Appendf(nil, "s%d-%d", s, v), payload),
		}}
	}
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		as := make([]*cluster.Actual, batch)
		for i := range as {
			as[i] = next()
		}
		b.StartTimer()
		if err := store.Update(func(tx *stagestore.Tx) error { return g.FoldActuals(tx, as) }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	events := float64(b.N * batch)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/events, "ns/event")
	if size, err := store.DiskUsage(); err == nil {
		b.ReportMetric(float64(size)/events, "store-bytes/event")
	}
}
