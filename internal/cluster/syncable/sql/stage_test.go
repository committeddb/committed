package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func stageConfig(stages ...ProjectionStage) *ProjectionConfig {
	return &ProjectionConfig{
		Table:      "t",
		PrimaryKey: []string{"id"},
		Columns:    []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}},
		Sources: []ProjectionSource{{
			Topic: "x", KeyPath: []string{"$.id"}, OnDelete: "delete-row",
			Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "id", From: "$.id"}}}},
		}},
		Stages: stages,
	}
}

// Until the stage engine lands, ANY stage block is rejected loudly at
// validation — accepted syntax, never accept-then-ignore (the forEach
// staging pattern). This assertion flips when the evaluator ships.
func TestStagesGatedUntilEngineLands(t *testing.T) {
	cfg := stageConfig(ProjectionStage{
		Name: "sums", From: "timesheets", KeyPath: []string{"$.card"},
		Reduce: "aggregate", Emit: []StageEmit{{Field: "n", Count: true}},
	})
	cfg.applyDefaults()
	err := validateProjectionConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stage engine has not landed")
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
set = [ { column = "id", from = "$.id" } ]
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

	// The full validation path carries the not-wired gate.
	cfg.applyDefaults()
	err = validateProjectionConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stage engine has not landed")
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
