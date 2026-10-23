package stages

import (
	"fmt"
	"testing"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// BenchmarkWinnerLargeGroup times one input's fold into a latest or liveSet
// key that already retains n inputs: each op, in its own store transaction
// as a live change arrives, appends a new event that becomes the winner
// (lead) or does not (lag).
func BenchmarkWinnerLargeGroup(b *testing.B) {
	withoutFoldStateCheck(b)
	for _, shape := range []struct {
		name  string
		stage Stage
	}{
		{"latest", Stage{Reduce: "latest", OrderBy: "$.version", OrderByType: "number", TieBy: "$.id"}},
		{"liveSet", Stage{Reduce: "liveSet", DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}}}},
	} {
		for _, mode := range []string{"lead", "lag"} {
			for _, n := range []int{1000, 10000} {
				b.Run(fmt.Sprintf("%s/%s/n=%d", shape.name, mode, n), func(b *testing.B) {
					st := shape.stage
					st.Name, st.From, st.KeyPath = "order", "events", []string{"$.stream_id"}
					st.Emit = []Emit{{Field: "status", From: "$.type"}, {Field: "version", From: "$.version"}}
					stages := []Stage{st}
					if err := ValidateShapes(stages); err != nil {
						b.Fatal(err)
					}
					g := BuildGraph(stages)
					store, _, err := stagestore.Open(b.TempDir(), "p", "fp")
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = store.Close() }()
					event := func(id, version int) any {
						v, err := decodeStageObject(fmt.Appendf(nil, `{"stream_id":"s1","id":%d,"version":%d,"type":"item_added"}`, id, version))
						if err != nil {
							b.Fatal(err)
						}
						return v
					}
					if err := store.Update(func(tx *stagestore.Tx) error {
						dirty := Dirty{}
						for v := 1; v <= n; v++ {
							if err := g.FoldTopicUpsert(tx, "events", fmt.Appendf(nil, "e%09d", v), event(v, v), 0, dirty); err != nil {
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
						version, key := next, fmt.Appendf(nil, "e%09d", next)
						if mode == "lag" {
							// An older version, and (for liveSet) a smaller input key.
							version, key = -next, fmt.Appendf(nil, "a%09d", next)
						}
						if err := store.Update(func(tx *stagestore.Tx) error {
							dirty := Dirty{}
							if err := g.FoldTopicUpsert(tx, "events", key, event(next, version), 0, dirty); err != nil {
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

// withoutFoldStateCheck turns off TestMain's per-output full-refold check
// for a benchmark, which would otherwise time the check.
func withoutFoldStateCheck(b *testing.B) {
	check := checkFoldState
	checkFoldState = nil
	b.Cleanup(func() { checkFoldState = check })
}
