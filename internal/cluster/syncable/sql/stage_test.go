package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func stageConfig(stages ...ProjectionStage) *ProjectionConfig {
	return &ProjectionConfig{
		Table:      "t",
		PrimaryKey: []string{"id"},
		Columns:    []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "v", SQLType: "VARCHAR(64)"}},
		Sources: []ProjectionSource{{
			Topic: "x", KeyPath: []string{"$.id"}, OnDelete: "delete-row",
			Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}},
		}},
		Stages: stages,
	}
}

// The engine is wired: a valid stage config validates clean.
func TestStagesValidate(t *testing.T) {
	cfg := stageConfig(ProjectionStage{
		Name: "sums", From: "timesheets", KeyPath: []string{"$.card"},
		Reduce: "aggregate", Emit: []StageEmit{{Field: "n", Count: true}},
	})
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}

// The shape rules behind the gate, exercised directly so the gate's
// removal cannot regress them.
func TestStageShapeRules(t *testing.T) {
	valid := func() []ProjectionStage {
		return []ProjectionStage{
			{
				Name: "live", From: "txns", KeyPath: []string{"$.id"},
				Emit: []StageEmit{{Field: "job", From: "$.jobId"}, {Field: "amt", Expr: "round($.a / nullif($.b, 0), 2)"}},
			},
			{
				Name: "by-job", From: "live", KeyPath: []string{"$.job"},
				Reduce: "aggregate", Emit: []StageEmit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
			},
		}
	}
	require.NoError(t, validateProjectionStageShapes(stageConfig(valid()...)))

	cases := map[string]struct {
		mutate func([]ProjectionStage) []ProjectionStage
		want   string
	}{
		"duplicate name": {func(st []ProjectionStage) []ProjectionStage {
			st[1].Name = "live"
			return st
		}, `second stage named "live"`},
		"forward reference": {func(st []ProjectionStage) []ProjectionStage {
			st[0].From = "by-job"
			return st
		}, "manifest order"},
		"self reference": {func(st []ProjectionStage) []ProjectionStage {
			st[0].From = "live"
			return st
		}, "manifest order"},
		"bad reduce": {func(st []ProjectionStage) []ProjectionStage {
			st[1].Reduce = "median"
			return st
		}, `reduce "median" is invalid`},
		"aggregate emit with from": {func(st []ProjectionStage) []ProjectionStage {
			st[1].Emit[0] = StageEmit{Field: "total", From: "$.amt"}
			return st
		}, "exactly one of sum, min, max, or count"},
		"reshape emit with sum": {func(st []ProjectionStage) []ProjectionStage {
			st[0].Emit[0] = StageEmit{Field: "job", Sum: "$.x"}
			return st
		}, "exactly one of from or expr"},
		"bare division in a fold": {func(st []ProjectionStage) []ProjectionStage {
			st[1].Emit[0] = StageEmit{Field: "total", Sum: "$.a / $.b"}
			return st
		}, "round(...) or trunc(...)"},
		"two keyPaths": {func(st []ProjectionStage) []ProjectionStage {
			st[0].KeyPath = []string{"$.a", "$.b"}
			return st
		}, "exactly one keyPath"},
		"wildcard keyPath": {func(st []ProjectionStage) []ProjectionStage {
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

// The TOML surface decodes: names, chaining froms, reduce, emit arms.
func TestStageTOMLDecodes(t *testing.T) {
	toml := `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "v"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "live"
from    = "txns"
keyPath = "$.id"
emit    = [ { field = "job", from = "$.jobId" } ]

[[projection.stage]]
name    = "by-job"
from    = "live"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "n", count = true } ]

[[projection.source]]
topic   = "x"
keyPath = "$.id"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	require.Len(t, cfg.Stages, 2)
	require.Equal(t, "live", cfg.Stages[0].Name)
	require.Equal(t, []string{"$.id"}, cfg.Stages[0].KeyPath)
	require.Equal(t, "live", cfg.Stages[1].From, "stages chain by name")
	require.Equal(t, "aggregate", cfg.Stages[1].Reduce)
	require.True(t, cfg.Stages[1].Emit[0].Count)

	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}

// The fingerprint pins store identity to the stage definitions: same
// stages → same fingerprint; any definitional change → a different one
// (and so a store reset + re-derive).
func TestStageFingerprint(t *testing.T) {
	a := stageConfig(ProjectionStage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []StageEmit{{Field: "f", From: "$.x"}},
	})
	b := stageConfig(ProjectionStage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []StageEmit{{Field: "f", From: "$.x"}},
	})
	require.Equal(t, stageFingerprint(a), stageFingerprint(b))

	b.Stages[0].Emit[0].From = "$.y"
	require.NotEqual(t, stageFingerprint(a), stageFingerprint(b))
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
	stages := []ProjectionStage{
		{
			Name: "live", From: "txns", KeyPath: []string{"$.id"},
			Emit: []StageEmit{{Field: "job", From: "$.jobId"}, {Field: "amt", From: "$.amount"}},
		},
		{
			Name: "by-job", From: "live", KeyPath: []string{"$.job"},
			Reduce: "aggregate", Emit: []StageEmit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
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
			return g.FoldTopicUpsert(tx, "txns", []byte(key), decodePayload(t, payload))
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
		return g.FoldTopicDelete(tx, "txns", []byte("t1"))
	}))
	require.Empty(t, out("live", "t1"))
	require.Empty(t, out("by-job", "j1"), "no inputs → the key retracts entirely")
	require.Equal(t, `{"n":2,"total":11.25}`, out("by-job", "j2"), "unrelated keys untouched")
}

// Filtering is refold: an input that stops matching the stage's when
// RETRACTS from its key — the field-verified gap-5 semantics.
func TestStageWhenRetraction(t *testing.T) {
	stages := []ProjectionStage{
		{
			Name: "sold", From: "projects", KeyPath: []string{"$.id"},
			When: []WhenClause{{Path: "$.status", Equals: "sold"}},
			Emit: []StageEmit{{Field: "id", From: "$.id"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	up := func(payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsert(tx, "projects", []byte("p1"), decodePayload(t, payload))
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

// A stage-definition edit changes the projection's shape fingerprint, so
// the config-change guard demands a rebuild — pairing the store reset the
// edit causes with a replay from index 0 (a reset store folding forward
// from a head checkpoint would silently serve partial state).
func TestStageEditTripsShapeFingerprint(t *testing.T) {
	a := stageConfig(ProjectionStage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []StageEmit{{Field: "f", From: "$.x"}},
	})
	b := stageConfig(ProjectionStage{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Emit: []StageEmit{{Field: "f", From: "$.y"}},
	})
	require.NotEqual(t, projectionShape(a), projectionShape(b))

	// And a stage-free config's shape is byte-identical to before the
	// feature (no spurious rebuilds for existing projections).
	c := stageConfig()
	require.NotContains(t, projectionShape(c), "stages:")
}

func projectionShape(c *ProjectionConfig) string { return c.projectionShapeFingerprint() }

// reduce = "latest": argmax by BUSINESS field, not arrival order — the
// field-measured divergence class (37/276,286 under keyset backfill).
// Ties break deterministically by tieBy; the winner's retraction promotes
// the runner-up (the payoff of retaining inputs — no O(1) winner mode);
// `when` filters BEFORE the argmax.
func TestStageLatestArgmax(t *testing.T) {
	stages := []ProjectionStage{
		{
			Name: "vws", From: "statuses", KeyPath: []string{"$.visit"},
			When:   []WhenClause{{Path: "$.approved", Equals: true}},
			Reduce: "latest", OrderBy: "$.ts", TieBy: "$.id", TieByType: "number",
			Emit: []StageEmit{{Field: "status", From: "$.status"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)

	up := func(id, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsert(tx, "statuses", []byte(id), decodePayload(t, payload))
		}))
	}
	del := func(id string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDelete(tx, "statuses", []byte(id))
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
	base := func() ProjectionStage {
		return ProjectionStage{
			Name: "s", From: "t", KeyPath: []string{"$.k"},
			Reduce: "latest", OrderBy: "$.ts", TieBy: "$.id",
			Emit: []StageEmit{{Field: "f", From: "$.x"}},
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
