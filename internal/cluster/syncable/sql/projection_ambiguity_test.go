package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestResolveRowKeys_RunFlipsToConfigShaped pins the keyPath site's
// ambiguity wiring: a keyPath that misses a run of consecutive distinct
// matched rows flips from Permanent to config-shaped (transient) at the
// evidence threshold, and a success of the SAME path resets the run.
func TestResolveRowKeys_RunFlipsToConfigShaped(t *testing.T) {
	p := &Projection{config: &ProjectionConfig{
		PrimaryKey: []string{"id"},
		Columns:    []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}},
	}}
	src := &projectionSource{
		keyPaths:    []string{"$.id"},
		keyTrackers: cluster.NewAmbiguityTrackers(1),
	}
	missing := map[string]any{"other": "x"}

	for i := 1; i < cluster.AmbiguityEvidenceThreshold; i++ {
		p.syncIndex = uint64(i)
		_, err := p.resolveRowKeys(src, missing, nil)
		require.ErrorIs(t, err, cluster.ErrPermanent, "miss %d may still be entry-specific", i)
		require.NotErrorIs(t, err, cluster.ErrConfigShaped)
	}
	p.syncIndex = uint64(cluster.AmbiguityEvidenceThreshold)
	_, err := p.resolveRowKeys(src, missing, nil)
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "the threshold-th distinct matched row establishes config-shape")
	require.NotErrorIs(t, err, cluster.ErrPermanent)

	// A row that resolves resets the run — the next miss is entry-specific
	// again.
	p.syncIndex++
	_, err = p.resolveRowKeys(src, map[string]any{"id": "k1"}, nil)
	require.NoError(t, err)
	p.syncIndex++
	_, err = p.resolveRowKeys(src, missing, nil)
	require.ErrorIs(t, err, cluster.ErrPermanent, "a success resets the site's evidence")
	require.NotErrorIs(t, err, cluster.ErrConfigShaped)
}
