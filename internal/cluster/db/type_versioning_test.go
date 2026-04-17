package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"
)

// newWalDB brings up a single-node DB backed by a real wal.Storage so
// the apply path actually persists types into bbolt. Tests in this file
// need that — the in-memory MemoryStorage doesn't track configuration
// state, which is what version-bump and no-op short-circuit logic reads
// from when deciding what to propose.
func newWalDB(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync(), wal.WithInMemoryTimeSeries())
	require.NoError(t, err)

	id := uint64(1)
	peers := db.Peers{id: ""}
	d := db.New(id, peers, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() {
		_ = d.Close()
	})
	return d, s
}

// proposeTypeTOML creates or bumps a type. For the first version of a
// type (no existing version in storage), no migration section is needed.
// For schema bumps, pass migrationSection as e.g.
// "\n\n[migration]\nnone = true" or "\n\n[migration]\ntransform = '.'".
func proposeTypeTOML(t *testing.T, d *db.DB, id, name, schema, migrationSection string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var data string
	if schema == "" {
		data = fmt.Sprintf("[type]\nname = \"%s\"%s", name, migrationSection)
	} else {
		data = fmt.Sprintf("[type]\nname = \"%s\"\nschemaType = \"JSONSchema\"\nschema = '%s'%s", name, schema, migrationSection)
	}
	cfg := &cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(data)}
	require.NoError(t, d.ProposeType(ctx, cfg))
}

// PUTting the same type twice with byte-identical schema is a no-op:
// the second call returns nil without proposing a new entry, so the
// storage still reports Version 1.
func TestProposeType_ByteIdenticalIsNoOp(t *testing.T) {
	d, s := newWalDB(t)

	schema := `{"type":"object"}`
	proposeTypeTOML(t, d, "person", "Person", schema, "")
	proposeTypeTOML(t, d, "person", "Person", schema, "") // no-op

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, got.Version, "version should not bump on byte-identical re-PUT")

	versions, err := s.TypeVersions("person")
	require.NoError(t, err)
	require.Len(t, versions, 1, "no-op re-PUT must not write a new history entry")
}

// PUTting the same type with a changed schema bumps Type.Version by one
// and adds an entry to the version history.
func TestProposeType_BumpsVersionOnSchemaChange(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["name"]}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, v1.Version)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["email"]}`, "\n\n[migration]\nnone = true")
	v2, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, v2.Version)
	require.Equal(t, `{"type":"object","required":["email"]}`, string(v2.Schema))

	// Both versions remain readable via TypeAtVersion.
	at1, err := s.ResolveType(cluster.TypeRefAt("person", int(1)))
	require.NoError(t, err)
	require.Equal(t, `{"type":"object","required":["name"]}`, string(at1.Schema))
	require.Equal(t, 1, at1.Version)

	at2, err := s.ResolveType(cluster.TypeRefAt("person", int(2)))
	require.NoError(t, err)
	require.Equal(t, 2, at2.Version)

	versions, err := s.TypeVersions("person")
	require.NoError(t, err)
	require.Len(t, versions, 2)
}

// Cross-version replay: a proposal stamped with TypeVersion 1 must be
// read back with the v1 schema even after the type has evolved to v2.
// This is the key invariant from ticket type-schema-versioning Phase 1
// item 4.
func TestCrossVersionReplay_ProposalKeepsItsStampedSchema(t *testing.T) {
	d, s := newWalDB(t)

	// Type v1 — relaxed schema.
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	// Stamp an entity at v1. We construct the proposal directly (the
	// HTTP layer does this from the current Type, but here we mirror it
	// by reading current and stamping ourselves).
	cur, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, cur.Version)

	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type:      cur,
		Key:       []byte("k1"),
		Data:      []byte(`{"name":"alice"}`),
		Timestamp: time.Now().UnixMilli(),
	}}}
	require.NoError(t, d.Propose(testCtx(t), p))

	// Type evolves to v2 — strict schema.
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["email"]}`, "\n\n[migration]\nnone = true")
	cur2, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, cur2.Version)

	// Read all proposals back. The user-data proposal must come back
	// hydrated with v1's schema (the version it was stamped with), not
	// the latest v2 schema.
	ps, err := d.Proposals(100, "person")
	require.NoError(t, err)
	require.Len(t, ps, 1)
	require.Equal(t, 1, ps[0].Entities[0].Type.Version,
		"entity must be hydrated with the schema in force at propose time, not the latest")
	require.Equal(t, `{"type":"object"}`, string(ps[0].Entities[0].Type.Schema))
}

// Schema bump without a [migration] section is rejected.
func TestProposeType_RejectsSchemaBumpWithoutMigration(t *testing.T) {
	d, _ := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	cfg := &cluster.Configuration{
		ID:       "person",
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\",\"required\":[\"email\"]}'"),
	}
	err := d.ProposeType(testCtx(t), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "[migration] section is required")
}

// migration.none = true allows a schema bump without a transform.
func TestProposeType_AcceptsNoneExplicitMigration(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["email"]}`, "\n\n[migration]\nnone = true")

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, got.Version)
	require.Empty(t, got.Migration, "none=true means empty migration, not a transform")
}

// Re-PUTting the same version with a different migration updates the
// migration in place without bumping version.
func TestProposeType_MutableMigration(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["email"]}`, "\n\n[migration]\ntransform = '.'")

	v2, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, v2.Version)
	require.Equal(t, ".", string(v2.Migration))

	// Fix the migration in place — same schema, different transform.
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object","required":["email"]}`, "\n\n[migration]\ntransform = '. + {email: \"x\"}'")

	fixed, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, fixed.Version, "version must NOT bump on migration-only change")
	require.Equal(t, `. + {email: "x"}`, string(fixed.Migration))
}

// Bad jq syntax is rejected at ProposeType time.
func TestProposeType_RejectsBadJQ(t *testing.T) {
	d, _ := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	cfg := &cluster.Configuration{
		ID:       "person",
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\",\"required\":[\"email\"]}'\n\n[migration]\ntransform = 'this is not jq'"),
	}
	err := d.ProposeType(testCtx(t), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not valid jq")
}

// User-supplied Type.Version in the TOML is ignored — the system
// determines the version from existing storage state.
func TestProposeType_IgnoresUserSuppliedVersion(t *testing.T) {
	d, s := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "person",
		MimeType: "text/toml",
		Data:     []byte("[type]\nname = \"Person\"\nversion = 99"),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, got.Version, "system-assigned version, not user-supplied 99")
}
