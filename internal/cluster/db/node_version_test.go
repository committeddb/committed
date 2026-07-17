package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/version"
)

func members(ids ...uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

// TestMinFeatureLevel is the heart of the version-skew gate: the cluster-agreed
// minimum is the lowest announced level across CURRENT members, and any member
// that hasn't announced (or predates the mechanism) counts as level 0 — the
// conservative floor that holds emission.
func TestMinFeatureLevel(t *testing.T) {
	cases := []struct {
		name     string
		members  map[uint64]struct{}
		versions map[uint64]uint64
		want     uint64
	}{
		{
			name:     "all announced at 1 → min 1 (refresh-boundary A gate open)",
			members:  members(1, 2, 3),
			versions: map[uint64]uint64{1: 1, 2: 1, 3: 1},
			want:     1,
		},
		{
			name:     "one member unannounced → 0 (A gate held during a rolling upgrade)",
			members:  members(1, 2, 3),
			versions: map[uint64]uint64{1: 1, 2: 1}, // node 3 hasn't announced
			want:     0,
		},
		{
			name:     "mixed levels → the lowest wins (B gate: a level-2 feature is held)",
			members:  members(1, 2, 3),
			versions: map[uint64]uint64{1: 2, 2: 2, 3: 1}, // node 3 still on the old binary
			want:     1,
		},
		{
			name:     "all at 2 → min 2 (B gate opens once the whole cluster is upgraded)",
			members:  members(1, 2, 3),
			versions: map[uint64]uint64{1: 2, 2: 2, 3: 2},
			want:     2,
		},
		{
			name:     "a removed node's lingering announcement is ignored (not a member)",
			members:  members(1, 2),
			versions: map[uint64]uint64{1: 2, 2: 2, 3: 0}, // 3 removed but stale entry present
			want:     2,
		},
		{
			name:     "no members → 0",
			members:  members(),
			versions: map[uint64]uint64{1: 5},
			want:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, minFeatureLevel(tc.members, tc.versions))
		})
	}
}

// TestFeatureLevelBaseline pins the invariant the A gate rests on: the
// refresh-boundary requirement is exactly the baseline level this binary
// supports, so an all-current-binary cluster has the gate OPEN (min == required)
// while any pre-mechanism node (level 0) holds it. If FeatureLevel is bumped for
// a future feature, refresh-boundary stays at the baseline it shipped with.
func TestFeatureLevelBaseline(t *testing.T) {
	require.GreaterOrEqual(t, version.FeatureLevel, uint64(1))
	require.Equal(t, uint64(1), featureLevelRefreshBoundary)
	require.GreaterOrEqual(t, version.FeatureLevel, featureLevelRefreshBoundary,
		"this binary must support the features it may emit")
}

func TestContainsRefreshBoundary(t *testing.T) {
	tipe := &cluster.Type{ID: "11111111-1111-1111-1111-111111111111"}

	marker := &cluster.Proposal{Entities: []*cluster.Entity{cluster.NewRefreshBoundaryEntity(tipe, 7)}}
	require.True(t, containsRefreshBoundary(marker))

	data := &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tipe, []byte("k"), []byte(`{"a":1}`)),
	}}
	require.False(t, containsRefreshBoundary(data), "plain row data is not a marker")

	require.False(t, containsRefreshBoundary(&cluster.Proposal{}), "empty proposal is not a marker")
}
