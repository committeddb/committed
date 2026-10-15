package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/version"
)

// TestParseType_OnSupersede pins the parse: the two values, the per-kind
// defaults through ScrubsSuperseded, and the two pairings refused because they
// contradict their kind.
func TestParseType_OnSupersede(t *testing.T) {
	for _, tc := range []struct {
		name      string
		doc       string
		wantErr   string
		wantScrub bool
	}{
		{"event default keeps", "entityKind = \"event\"", "", false},
		{"event declares scrub", "entityKind = \"event\"\nonSupersede = \"scrub\"", "", true},
		{"event declares keep", "entityKind = \"event\"\nonSupersede = \"keep\"", "", false},
		{"unspecified declares scrub", "onSupersede = \"scrub\"", "", true},
		{"standalone declares scrub", "entityKind = \"standalone\"\nonSupersede = \"scrub\"", "", true},
		{"snapshot default scrubs", "entityKind = \"snapshot\"", "", true},
		{"snapshot restates scrub", "entityKind = \"snapshot\"\nonSupersede = \"scrub\"", "", true},
		{"snapshot keep refused", "entityKind = \"snapshot\"\nonSupersede = \"keep\"", "declare entityKind = \"revision\"", false},
		{"revision default keeps", "entityKind = \"revision\"", "", false},
		{"revision scrub refused", "entityKind = \"revision\"\nonSupersede = \"scrub\"", "prior versions are its data", false},
		{"unknown value refused", "onSupersede = \"drop\"", "unknown onSupersede \"drop\"", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, typ, err := db.ParseType(typeConfig("u", "[type]\nname = \"U\"\n"+tc.doc+"\n"), nil)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantScrub, typ.ScrubsSuperseded())
		})
	}
}

// TestOnSupersedeGate_RefusedBelowTheLevel: a declaration that changes the
// kind's default is refused while any member is below level 3 (a member that
// dropped it would keep entities the others scrub, and the logs would diverge),
// and admitted once every member announces it. A declaration that restates the
// default is admitted below the level, stored as undeclared.
func TestOnSupersedeGate_RefusedBelowTheLevel(t *testing.T) {
	d, s := newWalDB(t) // un-announced: the cluster minimum is 0

	scrub := "[type]\nname = \"Events\"\nentityKind = \"event\"\nonSupersede = \"scrub\"\n"
	err := d.ProposeType(testCtx(t), typeConfig("events", scrub))
	var lvl *cluster.ClusterBelowFeatureLevelError
	require.ErrorAs(t, err, &lvl)
	require.Equal(t, db.FeatureLevelOnSupersedeForTest, lvl.Required)
	_, rerr := s.ResolveType(cluster.LatestTypeRef("events"))
	require.Error(t, rerr, "a refused type must not be stored")

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("snapshots",
		"[type]\nname = \"Snapshots\"\nentityKind = \"snapshot\"\nonSupersede = \"scrub\"\n")))
	require.Eventually(t, func() bool {
		got, err := s.ResolveType(cluster.LatestTypeRef("snapshots"))
		return err == nil && got != nil
	}, 10*time.Second, 5*time.Millisecond)
	got, err := s.ResolveType(cluster.LatestTypeRef("snapshots"))
	require.NoError(t, err)
	require.Equal(t, cluster.OnSupersedeUnspecified, got.OnSupersede, "a restated default is stored as undeclared")
	require.True(t, got.ScrubsSuperseded())

	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(db.FeatureLevelOnSupersedeForTest) },
		10*time.Second, 10*time.Millisecond)

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("events", scrub)))
	require.Eventually(t, func() bool {
		got, err := s.ResolveType(cluster.LatestTypeRef("events"))
		return err == nil && got != nil && got.OnSupersede == cluster.OnSupersedeScrub
	}, 10*time.Second, 5*time.Millisecond, "the declaration must be stored, not dropped")
}

// TestOnSupersede_MutableInPlace: switching keep to scrub and back updates the
// current version in place (no version bump, no [migration] required), and a
// re-POST of the same document is a no-op.
func TestOnSupersede_MutableInPlace(t *testing.T) {
	d, s := newWalDB(t)
	announceFeatureLevel(t, d, version.FeatureLevel)
	require.Eventually(t, func() bool { return d.FeatureEnabled(db.FeatureLevelOnSupersedeForTest) },
		10*time.Second, 10*time.Millisecond)

	resolve := func() *cluster.Type {
		got, err := s.ResolveType(cluster.LatestTypeRef("events"))
		require.NoError(t, err)
		return got
	}
	waitFor := func(on cluster.OnSupersede) {
		require.Eventually(t, func() bool {
			got, err := s.ResolveType(cluster.LatestTypeRef("events"))
			return err == nil && got != nil && got.OnSupersede == on
		}, 10*time.Second, 5*time.Millisecond)
	}

	base := "[type]\nname = \"Events\"\nentityKind = \"event\"\n"
	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("events", base)))
	waitFor(cluster.OnSupersedeUnspecified)

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("events", base+"onSupersede = \"scrub\"\n")))
	waitFor(cluster.OnSupersedeScrub)
	require.Equal(t, 1, resolve().Version, "a switch is an in-place edit, not a version bump")

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("events", base+"onSupersede = \"scrub\"\n")))
	require.Equal(t, cluster.OnSupersedeScrub, resolve().OnSupersede)

	require.NoError(t, d.ProposeType(testCtx(t), typeConfig("events", base+"onSupersede = \"keep\"\n")))
	waitFor(cluster.OnSupersedeKeep)
	require.Equal(t, 1, resolve().Version)
}

// TestOnSupersede_RoundTrips: the field survives the wire format, and a type
// stored without its submitted document synthesizes a read-back that parses
// back to the same declaration.
func TestOnSupersede_RoundTrips(t *testing.T) {
	in := &cluster.Type{ID: "events", Name: "Events", Version: 1, EntityKind: cluster.EntityKindEvent, OnSupersede: cluster.OnSupersedeScrub}
	bs, err := in.Marshal()
	require.NoError(t, err)
	var out cluster.Type
	require.NoError(t, out.Unmarshal(bs))
	require.Equal(t, cluster.OnSupersedeScrub, out.OnSupersede)

	cfg, err := out.Configuration()
	require.NoError(t, err)
	require.Contains(t, string(cfg.Data), `onSupersede = 'scrub'`)
	_, parsed, err := db.ParseType(cfg, nil)
	require.NoError(t, err)
	require.Equal(t, cluster.OnSupersedeScrub, parsed.OnSupersede)
}
