package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Shims keep the moved test bodies byte-stable while the engine's
// exported surface is the real API.
type dirtySet = Dirty

func validateProjectionStageShapes(c *testStageCfg) error { return ValidateShapes(c.Stages) }

func buildStageGraph(sts []Stage) *Graph { return BuildGraph(sts) }

func decodeStageObject(bs []byte) (any, error) { return DecodeObject(bs) }

func stageOutKey(parts []string) string { return OutKey(parts) }

type testStageCfg struct{ Stages []Stage }

func stageConfig(stages ...Stage) *testStageCfg { return &testStageCfg{Stages: stages} }
func TestStageShapeRules(t *testing.T) {
	valid := func() []Stage {
		return []Stage{
			{
				Name: "live", From: "txns", KeyPath: []string{"$.id"},
				Emit: []Emit{{Field: "job", From: "$.jobId"}, {Field: "amt", Expr: "round($.a / nullif($.b, 0), 2)"}},
			},
			{
				Name: "by-job", From: "live", KeyPath: []string{"$.job"},
				Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
			},
		}
	}
	require.NoError(t, validateProjectionStageShapes(stageConfig(valid()...)))

	cases := map[string]struct {
		mutate func([]Stage) []Stage
		want   string
	}{
		"duplicate name": {func(st []Stage) []Stage {
			st[1].Name = "live"
			return st
		}, `second stage named "live"`},
		"forward reference": {func(st []Stage) []Stage {
			st[0].From = "by-job"
			return st
		}, "manifest order"},
		"self reference": {func(st []Stage) []Stage {
			st[0].From = "live"
			return st
		}, "manifest order"},
		"bad reduce": {func(st []Stage) []Stage {
			st[1].Reduce = "median"
			return st
		}, `reduce "median" is invalid`},
		"aggregate emit with from": {func(st []Stage) []Stage {
			st[1].Emit[0] = Emit{Field: "total", From: "$.amt"}
			return st
		}, "exactly one of sum, min, max, or count"},
		"reshape emit with sum": {func(st []Stage) []Stage {
			st[0].Emit[0] = Emit{Field: "job", Sum: "$.x"}
			return st
		}, "exactly one of from or expr"},
		"bare division in a fold": {func(st []Stage) []Stage {
			st[1].Emit[0] = Emit{Field: "total", Sum: "$.a / $.b"}
			return st
		}, "round(...) or trunc(...)"},
		"wildcard keyPath": {func(st []Stage) []Stage {
			st[0].KeyPath = []string{"$.items[*].id"}
			return st
		}, "multi-valued"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateProjectionStageShapes(stageConfig(tc.mutate(valid())...))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestStageFingerprint(t *testing.T) {
	a := stageConfig(Stage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []Emit{{Field: "f", From: "$.x"}},
	})
	b := stageConfig(Stage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []Emit{{Field: "f", From: "$.x"}},
	})
	require.Equal(t, Fingerprint(a.Stages), Fingerprint(b.Stages))

	b.Stages[0].Emit[0].From = "$.y"
	require.NotEqual(t, Fingerprint(a.Stages), Fingerprint(b.Stages))
}

func stageStoreForTest(t *testing.T) *stagestore.Store {
	t.Helper()
	s, _, err := stagestore.Open(t.TempDir(), "p", "fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func decodePayload(t *testing.T, src string) any {
	t.Helper()
	v, err := decodeStageObject([]byte(src))
	require.NoError(t, err)
	return v
}

// The evaluator end to end over a two-stage chain: a reshape stage feeds
// an aggregate stage; upserts fold through, a rekey retracts from the old
// key, a retraction refolds the aggregate, and the last input's removal
// retracts the aggregate key entirely.
func TestStageEvaluatorChain(t *testing.T) {
	stages := []Stage{
		{
			Name: "live", From: "txns", KeyPath: []string{"$.id"},
			Emit: []Emit{{Field: "job", From: "$.jobId"}, {Field: "amt", From: "$.amount"}},
		},
		{
			Name: "by-job", From: "live", KeyPath: []string{"$.job"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg)) // compiles the emit exprs
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	out := func(stage, key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	upsert := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txns", []byte(key), decodePayload(t, payload))
		}))
	}

	upsert("t1", `{"id":"t1","jobId":"j1","amount":2.5}`)
	upsert("t2", `{"id":"t2","jobId":"j1","amount":1.25}`)
	upsert("t3", `{"id":"t3","jobId":"j2","amount":10}`)

	require.Equal(t, `{"amt":2.5,"job":"j1"}`, out("live", "t1"))
	require.Equal(t, `{"n":2,"total":3.75}`, out("by-job", "j1"), "exact decimal sum, recomputed")
	require.Equal(t, `{"n":1,"total":10}`, out("by-job", "j2"))

	// Rekey: t2 moves to job j2 — j1 refolds without it, j2 gains it.
	upsert("t2", `{"id":"t2","jobId":"j2","amount":1.25}`)
	require.Equal(t, `{"n":1,"total":2.5}`, out("by-job", "j1"))
	require.Equal(t, `{"n":2,"total":11.25}`, out("by-job", "j2"))

	// Retraction: deleting t1 empties j1 → the aggregate key retracts.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "txns", []byte("t1"))
	}))
	require.Empty(t, out("live", "t1"))
	require.Empty(t, out("by-job", "j1"), "no inputs → the key retracts entirely")
	require.Equal(t, `{"n":2,"total":11.25}`, out("by-job", "j2"), "unrelated keys untouched")
}

// Filtering is refold: an input that stops matching the stage's when
// RETRACTS from its key — the field-verified gap-5 semantics.
func TestStageWhenRetraction(t *testing.T) {
	stages := []Stage{
		{
			Name: "sold", From: "projects", KeyPath: []string{"$.id"},
			When: []WhenClause{{Path: "$.status", Equals: "sold"}},
			Emit: []Emit{{Field: "id", From: "$.id"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	up := func(payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "projects", []byte("p1"), decodePayload(t, payload))
		}))
	}
	get := func() string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("sold", []byte("p1"))
			got = string(v)
			return err
		}))
		return got
	}

	up(`{"id":"p1","status":"sold"}`)
	require.Equal(t, `{"id":"p1"}`, get())
	up(`{"id":"p1","status":"pending"}`)
	require.Empty(t, get(), "the predicate flipped off — the row leaves (gap 5)")
	up(`{"id":"p1","status":"sold"}`)
	require.Equal(t, `{"id":"p1"}`, get(), "and re-enters when it flips back")
}

func TestStageLatestArgmax(t *testing.T) {
	stages := []Stage{
		{
			Name: "vws", From: "statuses", KeyPath: []string{"$.visit"},
			When:   []WhenClause{{Path: "$.approved", Equals: true}},
			Reduce: "latest", OrderBy: "$.ts", TieBy: "$.id", TieByType: "number",
			Emit: []Emit{{Field: "status", From: "$.status"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	up := func(id, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "statuses", []byte(id), decodePayload(t, payload))
		}))
	}
	del := func(id string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, "statuses", []byte(id))
		}))
	}
	get := func() string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("vws", []byte("v1"))
			got = string(v)
			return err
		}))
		return got
	}

	// Arrival order is newest-first; the argmax still picks by $.ts.
	up("e2", `{"visit":"v1","ts":"2026-08-10","id":2,"status":"done","approved":true}`)
	up("e1", `{"visit":"v1","ts":"2026-08-03","id":1,"status":"scheduled","approved":true}`)
	require.Equal(t, `{"status":"done"}`, get(), "business field wins, not arrival order")

	// An UNAPPROVED newer event must not shadow the approved older one.
	up("e3", `{"visit":"v1","ts":"2026-08-15","id":3,"status":"cancelled","approved":false}`)
	require.Equal(t, `{"status":"done"}`, get(), "when filters BEFORE the argmax")

	// A tie on ts breaks by numeric id — 10 beats 2 numerically (and the
	// entity key "z10" scans AFTER "e2", so first-scanned-wins would keep
	// e2: the tiebreak is load-bearing here, not iteration order).
	up("z10", `{"visit":"v1","ts":"2026-08-10","id":10,"status":"revised","approved":true}`)
	require.Equal(t, `{"status":"revised"}`, get(), "ties break by tieBy, numerically")

	// Winner retraction promotes the runner-up from the retained set.
	del("z10")
	require.Equal(t, `{"status":"done"}`, get())
	del("e2")
	require.Equal(t, `{"status":"scheduled"}`, get(), "the runner-up's runner-up")
	del("e1")
	require.Empty(t, get(), "no qualifying inputs — the key retracts")
}

// latest shape rules: orderBy and tieBy are mandatory, and reserved to
// the latest reduce.
func TestStageLatestValidation(t *testing.T) {
	base := func() Stage {
		return Stage{
			Name: "s", From: "t", KeyPath: []string{"$.k"},
			Reduce: "latest", OrderBy: "$.ts", TieBy: "$.id",
			Emit: []Emit{{Field: "f", From: "$.x"}},
		}
	}
	ok := base()
	require.NoError(t, validateProjectionStageShapes(stageConfig(ok)))

	noTie := base()
	noTie.TieBy = ""
	err := validateProjectionStageShapes(stageConfig(noTie))
	require.ErrorContains(t, err, "tieBy")
	require.ErrorContains(t, err, "37 of 276,286")

	noOrder := base()
	noOrder.OrderBy = ""
	require.ErrorContains(t, validateProjectionStageShapes(stageConfig(noOrder)), "orderBy")

	strayOrder := base()
	strayOrder.Reduce = ""
	require.ErrorContains(t, validateProjectionStageShapes(stageConfig(strayOrder)), `only for reduce = "latest"`)
}

// Filtering joins (gap 6): a stage's input participates only while the
// joined dimension row exists and matches the join's where — S1's shape.
// The dimension arriving late heals; flipping its predicate off retracts
// dependents (the field probe); deleting it retracts them too.
func TestStageFilteringJoin(t *testing.T) {
	stages := []Stage{
		{
			Name: "alive", From: "workareas", KeyPath: []string{"$.id"},
			Joins: []Join{{
				Topic: "projects", On: "$.projectId",
				Where: []WhenClause{{Path: "$.status", Equals: "sold"}},
			}},
			Emit: []Emit{{Field: "id", From: "$.id"}, {Field: "price", From: "$.price"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	del := func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("alive", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// The spine arrives before its dimension: no participation yet.
	fold("workareas", "w1", `{"id":"w1","projectId":"p1","price":100}`)
	require.Empty(t, get("w1"), "no dimension row yet — participation fails closed")

	// The dimension lands, qualifying: the dependent HEALS via fan-out.
	fold("projects", "p1", `{"status":"sold"}`)
	require.Equal(t, `{"id":"w1","price":100}`, get("w1"))

	// The predicate flips off: dependents retract (the S1 field probe).
	fold("projects", "p1", `{"status":"pending"}`)
	require.Empty(t, get("w1"))

	// And back on: they re-enter.
	fold("projects", "p1", `{"status":"sold"}`)
	require.Equal(t, `{"id":"w1","price":100}`, get("w1"))

	// A second workarea on an unrelated project is untouched throughout.
	fold("projects", "p9", `{"status":"sold"}`)
	fold("workareas", "w9", `{"id":"w9","projectId":"p9","price":5}`)

	// Dimension delete retracts its dependents only.
	del("projects", "p1")
	require.Empty(t, get("w1"))
	require.Equal(t, `{"id":"w9","price":5}`, get("w9"))
}

// The epoch sweep: a re-snapshot's boundary marker retracts retained
// inputs the re-snapshot did NOT re-assert (source-deleted in the lost
// window), refolding their keys as explicit deltas. Generation 0 is
// never swept (direct writes / stage-fed retention — the >= 1 floor).
func TestStageEpochSweep(t *testing.T) {
	stages := []Stage{
		{
			Name: "by-job", From: "txns", KeyPath: []string{"$.job"},
			Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string, gen uint64) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := dirtySet{}
			if err := g.FoldTopicUpsert(tx, "txns", []byte(key), decodePayload(t, payload), gen, dirty); err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	sweep := func(marker uint64) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := dirtySet{}
			if err := g.SweepEpochs(tx, "txns", marker, dirty); err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("by-job", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// Epoch 1 snapshot: three txns, two jobs; plus one DIRECT write (gen 0).
	fold("t1", `{"job":"j1"}`, 1)
	fold("t2", `{"job":"j1"}`, 1)
	fold("t3", `{"job":"j2"}`, 1)
	fold("d1", `{"job":"j2"}`, 0)
	require.Equal(t, `{"n":2}`, get("j1"))
	require.Equal(t, `{"n":2}`, get("j2"))

	// The re-snapshot (epoch 2) re-asserts t1 only — t2 and t3 were
	// deleted at the source in the lost window.
	fold("t1", `{"job":"j1"}`, 2)
	sweep(2)

	require.Equal(t, `{"n":1}`, get("j1"), "the un-reasserted input swept, key refolds")
	require.Equal(t, `{"n":1}`, get("j2"), "gen-0 direct write survives the sweep (the >= 1 floor)")
}

// forEach as a STAGE: one event's elements fan into the stage's reduce —
// fan-then-fold in a single stage (invoiced-by-workarea's shape). A
// re-emitted parent reconciles (vanished elements retract, refolding
// their aggregates), and the parent's tombstone retracts everything.
func TestStageForEachFanThenFold(t *testing.T) {
	stages := []Stage{
		{
			Name: "by-wa", From: "txns", ForEach: "$.data.elements[*]",
			ElementKey: "$.id",
			KeyPath:    []string{"$.workareaId"}, Reduce: "aggregate",
			Emit: []Emit{{Field: "total", Sum: "$.amount"}, {Field: "n", Count: true}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txns", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("by-wa", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	fold("t1", `{"data":{"elements":[
		{"id":"e1","workareaId":"w1","amount":2.5},
		{"id":"e2","workareaId":"w1","amount":1.25},
		{"id":"e3","workareaId":"w2","amount":10}]}}`)
	require.Equal(t, `{"n":2,"total":3.75}`, get("w1"), "fan then fold, exactly")
	require.Equal(t, `{"n":1,"total":10}`, get("w2"))

	// Re-emit: one w1 element (new amount), w2's element vanished.
	fold("t1", `{"data":{"elements":[{"id":"e1","workareaId":"w1","amount":3}]}}`)
	require.Equal(t, `{"n":1,"total":3}`, get("w1"), "reconciled and refolded")
	require.Empty(t, get("w2"), "the vanished element's aggregate retracts")

	// A second parent contributes to the same workarea independently.
	fold("t2", `{"data":{"elements":[{"id":"e9","workareaId":"w1","amount":0.5}]}}`)
	require.Equal(t, `{"n":2,"total":3.5}`, get("w1"))

	// Parent tombstone retracts its elements only.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "txns", []byte("t1"))
	}))
	require.Equal(t, `{"n":1,"total":0.5}`, get("w1"))
}

// A forEach reshape stage: each element becomes its own key, with
// `$parent.` reaching the enclosing event at drain-time refold (the
// wrapper retention).
func TestStageForEachParentScope(t *testing.T) {
	stages := []Stage{
		{
			Name: "elems", From: "txns", ForEach: "$.items[*]",
			KeyPath: []string{"$.sku"},
			Emit: []Emit{
				{Field: "amt", From: "$.amount"},
				{Field: "txn", From: "$parent.id"},
			},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicUpsertNow(tx, "txns", []byte("t1"),
			decodePayload(t, `{"id":"t1","items":[{"sku":"a","amount":"2.50"}]}`))
	}))
	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut("elems", []byte("a"))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"amt":"2.50","txn":"t1"}`, got)
}

// Joins on STAGES (cross-stage correlation, the S9/S10 shape): a later
// stage joins a PRIOR stage's outputs as its dimension rows, maintained
// by the drain — the producer's refolds heal, flip, and retract the
// consumer's participation through the same fan-out machinery.
func TestJoinOnStage(t *testing.T) {
	stages := []Stage{
		{
			Name: "pay-counts", From: "payments", KeyPath: []string{"$.proposal"},
			Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}},
		},
		{
			Name: "funded", From: "proposals", KeyPath: []string{"$.id"},
			Joins: []Join{{
				From: "pay-counts", On: "$.id",
				Where: []WhenClause{{Path: "$.n", Equals: 2}},
			}},
			Emit: []Emit{{Field: "id", From: "$.id"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	del := func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	get := func() string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("funded", []byte("p1"))
			got = string(v)
			return err
		}))
		return got
	}

	// The proposal arrives before any payments: no dimension row yet.
	fold("proposals", "pr1", `{"id":"p1"}`)
	require.Empty(t, get())

	// One payment → pay-counts(p1) = {n:1} — where wants 2: still out.
	fold("payments", "m1", `{"proposal":"p1"}`)
	require.Empty(t, get())

	// The second payment refolds the producer to {n:2}: the consumer
	// HEALS through the stage-dimension fan-out.
	fold("payments", "m2", `{"proposal":"p1"}`)
	require.Equal(t, `{"id":"p1"}`, get())

	// A third payment flips it back out (n:3 ≠ 2)…
	fold("payments", "m3", `{"proposal":"p1"}`)
	require.Empty(t, get())

	// …and deleting it flips back in.
	del("payments", "m3")
	require.Equal(t, `{"id":"p1"}`, get())

	// Retracting the producer's key entirely (no payments) removes the
	// dimension row: the consumer retracts.
	del("payments", "m1")
	del("payments", "m2")
	require.Empty(t, get())
}

// reduce = "liveSet": created-minus-deleted as a set difference (S3's
// txn-live shape). A delete-shaped event retracts its key — even though
// the stage's when would exclude it, it is retained as NEGATIVE evidence
// — and retracting the delete event itself un-deletes the key.
func TestStageLiveSet(t *testing.T) {
	stages := []Stage{
		{
			Name: "txn-live", From: "txn-events", KeyPath: []string{"$.txn"},
			When:       []WhenClause{{Path: "$.type", Equals: "created"}},
			Reduce:     "liveSet",
			DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
			Emit:       []Emit{{Field: "job", From: "$.job"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txn-events", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("txn-live", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// Created → live, emitting from the create event.
	fold("e1", `{"txn":"t1","type":"created","job":"j1"}`)
	require.Equal(t, `{"job":"j1"}`, get("t1"))

	// A foreign-shaped event (matches neither when nor deleteWhen) is
	// simply not membership — the key stays as it was.
	fold("e2", `{"txn":"t1","type":"annotated"}`)
	require.Equal(t, `{"job":"j1"}`, get("t1"))

	// The delete-shaped event retracts the key — set difference, not
	// ordering: it wins regardless of arrival position.
	fold("e3", `{"txn":"t1","type":"deleted"}`)
	require.Empty(t, get("t1"))

	// A re-created event does NOT resurrect while the delete evidence
	// stands (created minus deleted is empty).
	fold("e4", `{"txn":"t1","type":"created","job":"j1"}`)
	require.Empty(t, get("t1"))

	// Retracting the delete event itself (e.g. an RTBF or correction on
	// the events topic) un-deletes: the surviving creates win again.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "txn-events", []byte("e3"))
	}))
	require.Equal(t, `{"job":"j1"}`, get("t1"))

	// An unrelated txn is untouched throughout.
	fold("e9", `{"txn":"t2","type":"created","job":"j2"}`)
	require.Equal(t, `{"job":"j2"}`, get("t2"))
}

// Composite stage keys (S5's (TimecardId, ProjectWorkareaId) grain):
// several keyPath parts form one key through the producers' composite
// encoding — same-pair inputs fold together, different pairs apart, and
// a partial key is non-membership.
func TestStageCompositeKey(t *testing.T) {
	stages := []Stage{
		{
			Name: "sums", From: "timesheets", KeyPath: []string{"$.card", "$.wa"},
			Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "timesheets", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(card, wa string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("sums", []byte(stageOutKey([]string{card, wa})))
			got = string(v)
			return err
		}))
		return got
	}

	fold("t1", `{"card":"c1","wa":"w1"}`)
	fold("t2", `{"card":"c1","wa":"w1"}`)
	fold("t3", `{"card":"c1","wa":"w2"}`)
	require.Equal(t, `{"n":2}`, get("c1", "w1"), "same pair folds together")
	require.Equal(t, `{"n":1}`, get("c1", "w2"), "the pair keys, not either column")

	// A partial key (missing wa) is non-membership: t2 retracts from its pair.
	fold("t2", `{"card":"c1"}`)
	require.Equal(t, `{"n":1}`, get("c1", "w1"))
}
