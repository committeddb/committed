package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/version"
)

// The 0.8.0 additions to the type record and the syncable checkpoint are
// gated together (db.featureLevelTypeRecord / featureLevelInterpretationPin),
// because an older member's apply path re-marshals each record from its own
// struct and DROPS a field it does not know — permanently, and only on that
// member. These tests pin both enforcements.

// TestTypeRecordGate_AnnounceTypeRefusedBelowTheLevel: an announce-typed type
// is refused while any member is below the level, rather than committed with
// a destination half the cluster would discard (a contract that can never
// announce). Once every member announces, it is admitted.
func TestTypeRecordGate_AnnounceTypeRefusedBelowTheLevel(t *testing.T) {
	d, s := newWalDB(t) // un-announced: the cluster minimum is 0

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("schema-changes",
		"[type]\nname = \"SchemaChanges\"\nentityKind = \"standalone\"")))

	announce := fmt.Sprintf("[type]\nname = \"PhotoMeta\"\nschemaType = \"JSONSchema\"\nschema = '%s'\nvalidate = \"announce\"\nschemaChangeTopic = \"schema-changes\"\n", tripwireSchema)
	err := d.ProposeType(testCtx(t), typeConfig("photo-meta", announce))
	require.Error(t, err)
	var lvl *cluster.ClusterBelowFeatureLevelError
	require.ErrorAs(t, err, &lvl)
	require.Equal(t, db.FeatureLevelTypeRecordForTest, lvl.Required)
	_, rerr := s.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.Error(t, rerr, "a refused type must not be stored")

	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(db.FeatureLevelTypeRecordForTest) },
		10*time.Second, 10*time.Millisecond)

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("photo-meta", announce)))
	got, err := s.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.NoError(t, err)
	require.Equal(t, cluster.ValidateAnnounce, got.Validate)
	require.Equal(t, "schema-changes", got.SchemaChangeTopic,
		"the destination must be stored, not dropped")
}

// TestTypeRecordGate_NonConvertibleRefusedBelowTheLevel: a declared break is
// refused below the level. Storing it as convertible would ADMIT the
// always-current syncables the break exists to refuse, on that member.
func TestTypeRecordGate_NonConvertibleRefusedBelowTheLevel(t *testing.T) {
	d, s := newWalDB(t) // un-announced

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person",
		"[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n")))

	bump := "[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\",\"required\":[\"email\"]}'\n\n[migration]\nnonConvertible = true\n"
	err := d.ProposeType(testCtx(t), typeConfig("person", bump))
	require.Error(t, err)
	var lvl *cluster.ClusterBelowFeatureLevelError
	require.ErrorAs(t, err, &lvl)

	got, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 1, got.Version, "a refused bump must not change the stored type")
	require.False(t, got.NonConvertible)

	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(db.FeatureLevelTypeRecordForTest) },
		10*time.Second, 10*time.Millisecond)

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("person", bump)))
	got, err = s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.Equal(t, 2, got.Version)
	require.True(t, got.NonConvertible, "the declared break must be stored, not dropped")
}

// TestTypeRecordGate_InterpretationPinClearedBelowTheLevel: a checkpoint
// cannot be refused — the worker must record progress — so the pin is cleared
// below the level. Every member then agrees on 0, and the first bump after
// the roll records the real coordinate.
func TestTypeRecordGate_InterpretationPinClearedBelowTheLevel(t *testing.T) {
	d, s := newWalDB(t) // un-announced
	seedSyncableConfig(t, d, "mirror")

	require.NoError(t, d.ProposeSyncableIndexWithPinForTest(testCtx(t), "mirror", 10, 7))
	require.Eventually(t, func() bool {
		ck, ok := s.SyncableCheckpoint("mirror")
		return ok && ck.Index == 10
	}, 10*time.Second, 5*time.Millisecond)
	ck, _ := s.SyncableCheckpoint("mirror")
	require.Zero(t, ck.InterpretationIndex,
		"below the level the pin is cleared, so every member agrees on 0")

	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(db.FeatureLevelInterpretationPinForTest) },
		10*time.Second, 10*time.Millisecond)

	require.NoError(t, d.ProposeSyncableIndexWithPinForTest(testCtx(t), "mirror", 11, 7))
	require.Eventually(t, func() bool {
		ck, ok := s.SyncableCheckpoint("mirror")
		return ok && ck.Index == 11 && ck.InterpretationIndex == 7
	}, 10*time.Second, 5*time.Millisecond, "the first bump after the roll records the real coordinate")
}
