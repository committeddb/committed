package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// stageConfig builds the minimal full projection config the sql-side
// stage tests need (the engine's own tests live with the engine in the
// stages package and take bare []Stage).
func stageConfig(sts ...ProjectionStage) *ProjectionConfig {
	return &ProjectionConfig{
		Table:      "t",
		PrimaryKey: []string{"id"},
		Columns:    []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "v", SQLType: "VARCHAR(64)"}},
		Sources: []ProjectionSource{{
			Topic: "x", KeyPath: []string{"$.id"}, OnDelete: "delete-row",
			Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}},
		}},
		Stages: sts,
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

// A stage join's on decodes scalar-or-list (the keyPath idiom): a single
// path stays one part, a composite key is a list matching the joined
// stage's key arity.
func TestStageJoinOnTOMLDecodes(t *testing.T) {
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
name    = "pairs"
from    = "billing"
keyPath = [ "$.job", "$.wa" ]
emit    = [ { field = "job", from = "$.job" } ]

[[projection.stage]]
name    = "unbilled"
from    = "candidates"
keyPath = "$.id"
emit    = [ { field = "id", from = "$.id" } ]
[[projection.stage.join]]
topic = "wgs"
on    = "$.wgsId"
[[projection.stage.join]]
from   = "pairs"
on     = [ "$.jobId", "$.waId" ]
absent = true

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
	joins := cfg.Stages[1].Joins
	require.Equal(t, []string{"$.wgsId"}, joins[0].On, "scalar on decodes as one part")
	require.Equal(t, []string{"$.jobId", "$.waId"}, joins[1].On, "list on decodes positionally")
	require.True(t, joins[1].Absent)
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}
