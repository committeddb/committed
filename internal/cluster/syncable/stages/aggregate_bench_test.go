package stages

import (
	"fmt"
	"testing"

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
