package sql

import (
	"strings"
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

// normalize = "lower" decodes on all three key-bearing declarations
// (source keyPath, stage keyPath, join on) and rejects unsupported modes.
func TestNormalizeTOMLDecodes(t *testing.T) {
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

[[projection.columns]]
name = "w"
type = "VARCHAR(64)"

[[projection.stage]]
name      = "st"
from      = "tp"
keyPath   = "$.guid"
normalize = "lower"
emit      = [ { field = "v", from = "$.v" } ]
[[projection.stage.join]]
topic     = "billed"
on        = "$.pairId"
absent    = true
normalize = "lower"

[[projection.source]]
topic     = "x"
keyPath   = "$.id"
normalize = "lower"
rowOwner  = true
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]

[[projection.source]]
from = "st"
[[projection.source.rules]]
set = [ { column = "w", from = "$.v" } ]
`
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	require.Equal(t, "lower", cfg.Stages[0].Normalize)
	require.Equal(t, "lower", cfg.Stages[0].Joins[0].Normalize)
	require.Equal(t, "lower", cfg.Sources[0].Normalize)
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))

	// An unsupported mode is rejected at admission, naming the one value.
	cfg.Sources[0].Normalize = "upper"
	err = validateProjectionConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `normalize "upper" is not supported (want "lower")`)
}

// The pilot's lsprobe: reduce = "liveSet" was unreachable from TOML —
// the parser lowercased it to "liveset", the engine compares camelCase,
// and the deleteWhen guard fired a self-contradictory error quoting the
// exact spelling the user wrote. The documented spelling must admit (in
// any case), and a genuinely unknown reduce must name liveSet among the
// valid values.
func TestLiveSetTOMLAdmits(t *testing.T) {
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
name       = "txn-live"
from       = "txn-events"
keyPath    = "$.TransactionId"
reduce     = "liveSet"
deleteWhen = [ { path = "$.EventType", equals = "delete" } ]
emit       = [ { field = "id", from = "$.TransactionId" } ]

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
	require.Equal(t, "liveSet", cfg.Stages[0].Reduce, "the documented spelling reaches the engine's canonical form")
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg), "liveSet must be admissible from config")

	// Case-tolerant like every other config spelling.
	loud := strings.Replace(toml, `reduce     = "liveSet"`, `reduce     = "LIVESET"`, 1)
	v, err = cluster.ParseConfigBytes("toml", []byte(loud))
	require.NoError(t, err)
	cfg, err = parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	require.Equal(t, "liveSet", cfg.Stages[0].Reduce)

	// A genuinely unknown reduce names ALL the valid values.
	bad := strings.Replace(toml, `reduce     = "liveSet"`, `reduce     = "liveliest"`, 1)
	v, err = cluster.ParseConfigBytes("toml", []byte(bad))
	require.NoError(t, err)
	cfg, err = parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	cfg.applyDefaults()
	err = validateProjectionConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `"liveSet"`)
}

// keyType / onType decode scalar-or-list, case-tolerant, and validate.
func TestKeyTypeTOMLDecodes(t *testing.T) {
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
name    = "by-wa"
from    = "billing"
keyPath = [ "$.job", "$.wa" ]
keyType = [ "text", "NUMBER" ]
emit    = [ { field = "job", from = "$.job" } ]

[[projection.stage]]
name    = "gate"
from    = "pairs"
keyPath = "$.id"
emit    = [ { field = "id", from = "$.id" } ]
[[projection.stage.join]]
topic  = "was"
on     = "$.waRef"
onType = "number"

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
	require.Equal(t, []string{"text", "number"}, cfg.Stages[0].KeyType, "case-tolerant, positional")
	require.Equal(t, []string{"number"}, cfg.Stages[1].Joins[0].OnType, "scalar decodes as one entry")
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}
