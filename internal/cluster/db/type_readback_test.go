package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/version"
)

func typeConfig(id, doc string) *cluster.Configuration {
	return &cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(doc)}
}

// newWalDBAtFeatureLevel is newWalDBAnnounced once the announce has landed
// — retaining a type's document is gated on the cluster feature level.
func newWalDBAtFeatureLevel(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	d, s := newWalDBAnnounced(t)
	require.Eventually(t, func() bool { return d.FeatureEnabled(version.FeatureLevel) },
		10*time.Second, 10*time.Millisecond, "feature level never announced")
	return d, s
}

// TestTypeReadBack_ReturnsTheSubmittedDocument pins the read-back
// contract: the type listing and the version endpoints return the document
// the operator submitted — comments, formatting, and the validation-only
// keys included — one document per version.
func TestTypeReadBack_ReturnsTheSubmittedDocument(t *testing.T) {
	d, s := newWalDBAtFeatureLevel(t)

	v1 := "# the person contract\n[type]\nname = \"Person\"   # display name\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n"
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", v1)))

	cfgs, err := s.Types()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, v1, string(cfgs[0].Data))
	require.Equal(t, "text/toml", cfgs[0].MimeType)

	v2 := "[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\",\"required\":[\"email\"]}'\n\n[migration]\ntransform = '. + {email: \"unknown\"}'\nvalidateAgainst = '{\"name\":\"x\"}'\n"
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", v2)))

	cur, err := s.TypeVersion("person", 2)
	require.NoError(t, err)
	require.Equal(t, v2, string(cur.Data), "the current version reads back as the document that made it")
	old, err := s.TypeVersion("person", 1)
	require.NoError(t, err)
	require.Equal(t, v1, string(old.Data), "an earlier version keeps its own document")
	cfgs, err = s.Types()
	require.NoError(t, err)
	require.Equal(t, v2, string(cfgs[0].Data))
}

// TestTypeReadBack_SynthesizesForATypeWithoutADocument pins the fallback
// for a type written before the document was retained: its read-back is a
// document in the current vocabulary — validate as a word, never the
// 0.7.x integer — that ParseType reads back to the stored fields.
func TestTypeReadBack_SynthesizesForATypeWithoutADocument(t *testing.T) {
	d, s := newWalDB(t)

	stored := &cluster.Type{
		ID: "cap", Name: "Cap", Version: 1, SchemaType: "JSONSchema",
		Schema:            []byte(`{"type":"object","properties":{"it's":{"type":"string"}}}`),
		Validate:          cluster.ValidateAnnounce,
		SchemaChangeTopic: "schema-changes",
		Migration:         []byte(".a |= 1\n| .b"),
		EntityKind:        cluster.EntityKindEvent,
		Discriminator:     "$.kind",
	}
	e, err := cluster.NewUpsertTypeEntity(stored) // no Document: the pre-0.8.0 entry shape
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	cfgs, err := s.Types()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	doc := string(cfgs[0].Data)
	require.Contains(t, doc, "validate = 'announce'")
	require.NotContains(t, doc, "validate = 2")
	require.Equal(t, "text/toml", cfgs[0].MimeType)

	_, parsed, err := db.ParseType(cfgs[0], nil)
	require.NoError(t, err, "the synthesized document must be a valid type document:\n%s", doc)
	// ParseType leaves the version to ProposeType, notes the [migration]
	// section it saw, and carries the document it parsed; the fields the
	// engine acts on read back exactly.
	parsed.Version = stored.Version
	parsed.MigrationExplicit = false
	parsed.Document, parsed.DocumentMimeType = nil, ""
	require.Equal(t, stored, parsed)

	v, err := s.TypeVersion("cap", 1)
	require.NoError(t, err)
	require.Equal(t, cfgs[0].Data, v.Data, "the version read-back synthesizes the same document")
}

// TestProposeType_RepointsTheSchemaChangeTopicInPlace: re-pointing an
// announce-typed type's destination is admitted as an in-place edit (mutable
// routing, like the discriminator) and must apply as one — the apply path
// used to take it for a replay and drop it silently.
func TestProposeType_RepointsTheSchemaChangeTopicInPlace(t *testing.T) {
	d, s := newWalDBAtFeatureLevel(t)

	for _, id := range []string{"events-a", "events-b"} {
		require.NoError(t, d.ProposeType(testCtx(t), typeConfig(id, "[type]\nname = \""+id+"\"\nentityKind = \"standalone\"")))
	}
	capDoc := func(dest string) string {
		return fmt.Sprintf("[type]\nname = \"Cap\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\nvalidate = \"announce\"\nschemaChangeTopic = %q\n", dest)
	}
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("cap", capDoc("events-a"))))
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("cap", capDoc("events-b"))))

	got, err := s.ResolveType(cluster.LatestTypeRef("cap"))
	require.NoError(t, err)
	require.Equal(t, "events-b", got.SchemaChangeTopic)
	require.Equal(t, 1, got.Version, "re-pointing the destination is not a schema change")

	versions, err := s.TypeVersions("cap")
	require.NoError(t, err)
	require.Len(t, versions, 1)
	cur, err := s.TypeVersion("cap", 1)
	require.NoError(t, err)
	require.Equal(t, capDoc("events-b"), string(cur.Data), "the retained document follows the in-place edit")
}

// TestTypeReadBack_AdoptsTheDocumentOnRePost walks a type through the
// roll: POSTed while a member is still below the level, its document is
// not retained (a pre-level-2 member would drop the field on apply and
// members would disagree on the read-back) and it reads back synthesized;
// once every member announces the level, the next document submitted for
// it is adopted in place and once; a further re-POST that changes nothing
// stays a no-op and keeps the document that last changed the type.
func TestTypeReadBack_AdoptsTheDocumentOnRePost(t *testing.T) {
	d, s := newWalDB(t) // un-announced: the cluster minimum is 0

	mine := "# the person contract\n[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n"
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", mine)))
	cur, err := s.TypeVersion("person", 1)
	require.NoError(t, err)
	require.NotEqual(t, mine, string(cur.Data), "below the level the document is not retained")
	require.Contains(t, string(cur.Data), "name = 'Person'", "…and the type reads back synthesized")

	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(version.FeatureLevel) },
		10*time.Second, 10*time.Millisecond)
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", mine)))
	cur, err = s.TypeVersion("person", 1)
	require.NoError(t, err)
	require.Equal(t, mine, string(cur.Data), "the first document submitted after the roll is adopted in place")
	versions, err := s.TypeVersions("person")
	require.NoError(t, err)
	require.Len(t, versions, 1, "adopting the document is not a version bump")

	reformatted := "[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n"
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", reformatted)))
	cur, err = s.TypeVersion("person", 1)
	require.NoError(t, err)
	require.Equal(t, mine, string(cur.Data), "a field-level no-op keeps the document that last changed the type")
}
