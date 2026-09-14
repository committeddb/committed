package sql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// A 0.7.10 projection config PARKS on 0.8.0 — the arrays of tables were
// renamed to plural nouns. The refusal must name the replacement, not read as
// a typo: api-compatibility.md promises "a spelling the vocabulary refuses on
// purpose is refused naming its replacement", and upgrade.md sends operators
// here. The type-level ledger only covers documents still saying
// type = "sql-projection"; a config that already said type = "projection" —
// the common 0.7.10 shape — lands on the projection parser instead.
func TestProjection_RemovedSpellingsNameTheirReplacement(t *testing.T) {
	p := &sql.ProjectionSyncableParser{}

	for _, tc := range []struct {
		name, doc, wantField, wantMentions string
	}{
		{
			name:         "singular source",
			doc:          "[projection]\ndb = \"bff\"\ntable = \"t\"\n\n[[projection.source]]\ntopic = \"a\"\n",
			wantField:    "projection.source",
			wantMentions: "sources",
		},
		{
			name:         "singular stage",
			doc:          "[projection]\ndb = \"bff\"\ntable = \"t\"\n\n[[projection.stage]]\nname = \"s\"\n",
			wantField:    "projection.stage",
			wantMentions: "stages",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := cluster.ParseConfigBytes("text/toml", []byte(tc.doc))
			require.NoError(t, err)
			_, err = p.ParseConfig(v, nil)
			require.Error(t, err)

			var fe *cluster.FieldError
			require.ErrorAs(t, err, &fe)
			require.Equal(t, tc.wantField, fe.Field)
			require.Contains(t, fe.Issue, tc.wantMentions,
				"the refusal must name the new spelling, not just call the old one unknown")
			require.NotContains(t, fe.Issue, "check the spelling against the docs",
				"a deliberate rename must not fall through to the generic typo message")
		})
	}
}

// The nested tables were renamed too, and an operator meets them right after
// renaming the enclosing table. The strict decoder names the offending path
// but not what it became, so the rename is annotated onto its error — pinned
// here because the annotation matches the decoder's message format, which a
// decoder change could silently break.
func TestProjection_RemovedNestedSpellingsNameTheirReplacement(t *testing.T) {
	p := &sql.ProjectionSyncableParser{}

	for _, tc := range []struct{ name, doc, wantMentions string }{
		{
			name:         "aggregate element",
			doc:          "[projection]\ndb = \"b\"\ntable = \"t\"\n\n[[projection.sources]]\ntopic = \"a\"\nkeyPath = \"$.k\"\n[projection.sources.aggregate]\ncolumn = \"c\"\n[[projection.sources.aggregate.element]]\njsonPath = \"$.x\"\nname = \"x\"\n",
			wantMentions: `renamed "element" to "fields"`,
		},
		{
			name:         "stage join",
			doc:          "[projection]\ndb = \"b\"\ntable = \"t\"\n\n[[projection.stages]]\nname = \"s\"\nfrom = \"a\"\n[[projection.stages.join]]\ntopic = \"b\"\n",
			wantMentions: `renamed "join" to "joins"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := cluster.ParseConfigBytes("text/toml", []byte(tc.doc))
			require.NoError(t, err)
			_, err = p.ParseConfig(v, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMentions,
				"the decode failure must carry the rename, not just name the key")
		})
	}
}

// TestProjection_HonorsCheckpointPolicy pins that the [syncable] envelope's
// checkpoint cadence reaches a projection. It was accepted and inert before —
// the README told operators to raise it on a projection, and nothing read it.
func TestProjection_HonorsCheckpointPolicy(t *testing.T) {
	p := &sql.ProjectionSyncableParser{}
	v, err := cluster.ParseConfigBytes("text/toml", []byte(projectionCheckpointTOML))
	require.NoError(t, err)

	storage := &TestDatabaseStorage{dbs: map[string]cluster.Database{"bff": &TestDatabase{}}}
	cfg, err := p.ParseConfig(v, storage)
	require.NoError(t, err)
	require.Equal(t, 7, cfg.Checkpoint.Every)
	require.Equal(t, 250*time.Millisecond, cfg.Checkpoint.MaxAge)
}

// TestProjection_CheckpointPolicyResolvesThroughWrappers proves the cadence
// actually reaches the worker, not just the config: the worker resolves it
// with cluster.SyncableAs over the unwrap chain, and a projection is wrapped
// (interpretation, and migration in always-current mode) before it gets
// there. Implementing the method is not enough on its own — this is what
// makes the config key take effect.
func TestProjection_CheckpointPolicyResolvesThroughWrappers(t *testing.T) {
	p := &sql.ProjectionSyncableParser{}
	v, err := cluster.ParseConfigBytes("text/toml", []byte(projectionCheckpointTOML))
	require.NoError(t, err)
	cfg, err := p.ParseConfig(v, &TestDatabaseStorage{dbs: map[string]cluster.Database{"bff": &TestDatabase{}}})
	require.NoError(t, err)

	proj := sql.NewProjection(&sql.DB{}, cfg, nil, "p")
	cc, ok := cluster.SyncableAs[cluster.CheckpointConfigurable](cluster.Syncable(proj))
	require.True(t, ok, "the worker must find CheckpointConfigurable on a projection")
	require.Equal(t, 7, cc.CheckpointPolicy().Every)
}

// projectionCheckpointTOML is a minimal complete projection that declares a
// checkpoint cadence in the [syncable] envelope.
const projectionCheckpointTOML = `[syncable]
name = "p"
type = "projection"
checkpointEvery = 7
checkpointMaxAge = "250ms"

[projection]
db = "bff"
table = "t"
primaryKey = "id"
topic = "a"
keyPath = "$.id"

[[projection.columns]]
name = "id"
type = "TEXT"

[[projection.columns]]
name = "title"
type = "TEXT"

[[projection.rules]]
set = [{ column = "title", from = "$.title" }]
`
