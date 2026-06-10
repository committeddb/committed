package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// entityKindTOML builds a type config whose TOML carries an optional
// entityKind/discriminator declaration. kindSection is e.g.
// "\nentityKind = \"event\"" or "" for an unspecified (grandfathered)
// type.
func entityKindTOML(name, schema, kindSection, migrationSection string) []byte {
	data := fmt.Sprintf("[type]\nname = \"%s\"%s", name, kindSection)
	if schema != "" {
		data = fmt.Sprintf("[type]\nname = \"%s\"\nschemaType = \"JSONSchema\"\nschema = '%s'%s", name, schema, kindSection)
	}
	return []byte(data + migrationSection)
}

// entityKind = "event" with a discriminator round-trips: TOML →
// propose → stored type resolves with the declared kind and
// discriminator.
func TestProposeType_EntityKindRoundTrips(t *testing.T) {
	d, s := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "tenant-events",
		MimeType: "text/toml",
		Data:     entityKindTOML("TenantEvents", "", "\nentityKind = \"event\"\ndiscriminator = \"$.event_type\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))

	got, err := s.ResolveType(cluster.LatestTypeRef("tenant-events"))
	require.NoError(t, err)
	require.Equal(t, cluster.EntityKindEvent, got.EntityKind)
	require.Equal(t, "$.event_type", got.Discriminator)
	require.Equal(t, 1, got.Version)

	// The entity kind also appears in the GET /type listing's
	// synthesized TOML.
	cfgs, err := s.Types()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Contains(t, string(cfgs[0].Data), `entityKind = "event"`)
	require.Contains(t, string(cfgs[0].Data), `discriminator = "$.event_type"`)
}

// A type that declares no entity kind stores and resolves as
// unspecified — the grandfathered default that behaves exactly like
// today — and its GET /type listing is byte-identical to the pre-kind
// output.
func TestProposeType_EntityKindDefaultsToUnspecified(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, cluster.EntityKindUnspecified, got.EntityKind)
	require.Empty(t, got.Discriminator)

	cfgs, err := s.Types()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "[type]\nname = \"Person\"", string(cfgs[0].Data))
}

// entityKind = "delta" is rejected at type creation with a
// doc-pointing error: at-least-once sync delivery corrupts
// non-idempotent patches.
func TestProposeType_RejectsDeltaEntityKind(t *testing.T) {
	d, _ := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "counters",
		MimeType: "text/toml",
		Data:     entityKindTOML("Counters", "", "\nentityKind = \"delta\"", ""),
	}
	err := d.ProposeType(testCtx(t), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `entityKind "delta" is not supported`)
	require.Contains(t, err.Error(), "README")
}

// Unknown entity kind strings fail at parse time, like
// ParseSyncableMode.
func TestProposeType_RejectsUnknownEntityKind(t *testing.T) {
	d, _ := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "person",
		MimeType: "text/toml",
		Data:     entityKindTOML("Person", "", "\nentityKind = \"snapshto\"", ""),
	}
	err := d.ProposeType(testCtx(t), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), `unknown entity kind "snapshto"`)
}

// discriminator is event-kind sugar; on any other kind there are no
// variants to discriminate, so its presence is a config error.
func TestProposeType_RejectsDiscriminatorOnNonEventEntityKind(t *testing.T) {
	d, _ := newWalDB(t)

	for _, kindSection := range []string{
		"\nentityKind = \"snapshot\"\ndiscriminator = \"$.event_type\"",
		"\ndiscriminator = \"$.event_type\"", // unspecified kind
	} {
		cfg := &cluster.Configuration{
			ID:       "person",
			MimeType: "text/toml",
			Data:     entityKindTOML("Person", "", kindSection, ""),
		}
		err := d.ProposeType(testCtx(t), cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), `discriminator is only valid for entityKind = "event"`)
	}
}

// A version bump cannot change a declared entity kind — changing your
// mind means a new type/topic.
func TestProposeType_EntityKindImmutableAcrossVersionBump(t *testing.T) {
	d, s := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object"}`, "\nentityKind = \"snapshot\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))

	bump := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object","required":["id"]}`, "\nentityKind = \"event\"", "\n\n[migration]\nnone = true"),
	}
	err := d.ProposeType(testCtx(t), bump)
	require.Error(t, err)
	require.Contains(t, err.Error(), "entityKind is immutable")

	got, err := s.ResolveType(cluster.LatestTypeRef("tenant"))
	require.NoError(t, err)
	require.Equal(t, cluster.EntityKindSnapshot, got.EntityKind)
	require.Equal(t, 1, got.Version, "rejected bump must not change the stored type")
}

// Once an entity kind is declared, every subsequent version must
// restate it — omission is not a silent reset to unspecified.
func TestProposeType_EntityKindOmissionAfterDeclarationRejected(t *testing.T) {
	d, _ := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object"}`, "\nentityKind = \"snapshot\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))

	bump := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object","required":["id"]}`, "", "\n\n[migration]\nnone = true"),
	}
	err := d.ProposeType(testCtx(t), bump)
	require.Error(t, err)
	require.Contains(t, err.Error(), "restate")
}

// Declaring an entity kind on a previously-unspecified type is allowed
// (the one-way adoption path for grandfathered types) and, with the
// schema unchanged, updates in place without a version bump. Once
// adopted, the kind is immutable like any declared kind.
func TestProposeType_EntityKindAdoptionUpdatesInPlace(t *testing.T) {
	d, s := newWalDB(t)

	proposeTypeTOML(t, d, "tenant", "Tenant", `{"type":"object"}`, "")

	adopt := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object"}`, "\nentityKind = \"snapshot\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), adopt))

	got, err := s.ResolveType(cluster.LatestTypeRef("tenant"))
	require.NoError(t, err)
	require.Equal(t, cluster.EntityKindSnapshot, got.EntityKind)
	require.Equal(t, 1, got.Version, "entity-kind adoption must not bump the version")

	change := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object"}`, "\nentityKind = \"event\"", ""),
	}
	err = d.ProposeType(testCtx(t), change)
	require.Error(t, err)
	require.Contains(t, err.Error(), "entityKind is immutable")
}

// Re-PUTting a kinded type byte-identically is still a no-op.
func TestProposeType_EntityKindIdenticalRePutIsNoOp(t *testing.T) {
	d, s := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "tenant",
		MimeType: "text/toml",
		Data:     entityKindTOML("Tenant", `{"type":"object"}`, "\nentityKind = \"snapshot\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))
	require.NoError(t, d.ProposeType(testCtx(t), cfg)) // no-op

	versions, err := s.TypeVersions("tenant")
	require.NoError(t, err)
	require.Len(t, versions, 1, "identical re-PUT must not write a new history entry")
}

// The discriminator is mutable sugar: changing it with the schema
// unchanged updates in place, no version bump and no migration needed.
func TestProposeType_DiscriminatorMutableInPlace(t *testing.T) {
	d, s := newWalDB(t)

	cfg := &cluster.Configuration{
		ID:       "tenant-events",
		MimeType: "text/toml",
		Data:     entityKindTOML("TenantEvents", `{"type":"object"}`, "\nentityKind = \"event\"\ndiscriminator = \"$.event_type\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), cfg))

	update := &cluster.Configuration{
		ID:       "tenant-events",
		MimeType: "text/toml",
		Data:     entityKindTOML("TenantEvents", `{"type":"object"}`, "\nentityKind = \"event\"\ndiscriminator = \"$.type\"", ""),
	}
	require.NoError(t, d.ProposeType(testCtx(t), update))

	got, err := s.ResolveType(cluster.LatestTypeRef("tenant-events"))
	require.NoError(t, err)
	require.Equal(t, "$.type", got.Discriminator)
	require.Equal(t, 1, got.Version, "discriminator change must not bump the version")
}
