package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// rowOwnerConfig builds the canonical ownership shape: a topic source
// owning row existence and a stage-fed decorator owning one column.
func rowOwnerConfig() *ProjectionConfig {
	return &ProjectionConfig{
		Table:      "t",
		PrimaryKey: []string{"id"},
		Columns: []ProjectionColumn{
			{Name: "id", SQLType: "VARCHAR(64)"},
			{Name: "w", SQLType: "VARCHAR(64)"},
			{Name: "v", SQLType: "VARCHAR(64)"},
		},
		Stages: []ProjectionStage{{
			Name: "st", From: "tp", KeyPath: []string{"$.id"},
			Emit: []StageEmit{{Field: "v", From: "$.v"}},
		}},
		Sources: []ProjectionSource{
			{
				Topic: "x", KeyPath: []string{"$.id"}, RowOwner: true,
				Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "w", From: "$.w"}}}},
			},
			{
				FromStage: "st",
				Rules:     []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}},
			},
		},
	}
}

func validated(t *testing.T, c *ProjectionConfig) error {
	t.Helper()
	c.applyDefaults()
	return validateProjectionConfig(c)
}

// The canonical shape validates, and the decorator's retraction defaults
// to clearing its own columns (never removing the owner's row).
func TestRowOwnerValidatesAndDecoratorDefaultsToClear(t *testing.T) {
	c := rowOwnerConfig()
	require.NoError(t, validated(t, c))
	require.Equal(t, onDeleteClear, c.Sources[1].OnDelete)
	require.Equal(t, onDeleteRow, c.Sources[0].OnDelete, "the row owner keeps row-delete semantics")
}

// Several sources folding one row with a stage among them MUST declare an
// owner: a stage-fed source's deltas are byte-suppressed against its
// store, so another source's row delete would silently strip its columns
// forever — admission demands the declaration instead of trapping.
func TestRowOwnerRequiredForMultiSourceWithStageFed(t *testing.T) {
	c := rowOwnerConfig()
	c.Sources[0].RowOwner = false
	err := validated(t, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "none declares rowOwner = true")
}

// A decorator's retraction may clear or ignore — only the row owner
// removes rows.
func TestRowOwnerDecoratorRejectsDeleteRow(t *testing.T) {
	c := rowOwnerConfig()
	c.Sources[1].OnDelete = onDeleteRow
	err := validated(t, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-owner")
	require.Contains(t, err.Error(), "only the row owner removes rows")
}

// A topic source cannot decorate an owned table: it has no retention, so
// a value arriving before the owner admits its row would be silently lost.
func TestRowOwnerTopicDecoratorRejected(t *testing.T) {
	c := rowOwnerConfig()
	c.Sources = append(c.Sources, ProjectionSource{
		Topic: "y", KeyPath: []string{"$.id"},
		Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "w", Value: "z"}}}},
	})
	err := validated(t, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be stage-fed")
}

// rowOwner declares row admission — only a plain rules source can hold
// it.
func TestRowOwnerOnAggregateRejected(t *testing.T) {
	c := rowOwnerConfig()
	c.Sources[0].Rules = nil
	c.Sources[0].Aggregate = &ProjectionAggregate{
		Column: "w", ElementKey: "$.id", Element: []ProjectionElementField{{Field: "a", From: "$.a"}},
	}
	err := validated(t, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only a plain rules source")
}

// A decorator cannot use lookup enrichment — its statements are bare
// UPDATEs; enrich from the row-owning source's rules.
func TestRowOwnerDecoratorRejectsEnrichment(t *testing.T) {
	c := rowOwnerConfig()
	c.Sources[1].Rules = []ProjectionRule{{Set: []ProjectionSet{
		{Column: "w", From: "$.f"},
		{Column: "v", Lookup: "dim", On: "w", Select: "name"},
	}}}
	err := validated(t, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use lookup enrichment")
}

// rowOwner = true decodes through the TOML surface.
func TestRowOwnerTOMLDecodes(t *testing.T) {
	toml := `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "w"
type = "VARCHAR(64)"

[[projection.columns]]
name = "v"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "st"
from    = "tp"
keyPath = "$.id"
emit    = [ { field = "v", from = "$.v" } ]

[[projection.source]]
topic    = "x"
keyPath  = "$.id"
rowOwner = true
[[projection.source.rules]]
set = [ { column = "w", from = "$.w" } ]

[[projection.source]]
from = "st"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	require.True(t, cfg.Sources[0].RowOwner)
	require.False(t, cfg.Sources[1].RowOwner)
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
	require.Equal(t, onDeleteClear, cfg.Sources[1].OnDelete)
}

// Flipping row ownership changes which writes create and delete rows —
// it must trip the rebuild-required shape gate. Spine-free configs keep
// a byte-identical shape (no spurious rebuilds).
func TestRowOwnerEditTripsShapeFingerprint(t *testing.T) {
	a := rowOwnerConfig()
	b := rowOwnerConfig()
	b.Sources[0].RowOwner = false
	b.Sources[1].RowOwner = true
	require.NotEqual(t, a.projectionShapeFingerprint(), b.projectionShapeFingerprint())
	require.NotContains(t, stageConfig().projectionShapeFingerprint(), "owners:")
}
