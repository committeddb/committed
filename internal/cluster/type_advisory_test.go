package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationEditAdvisory(t *testing.T) {
	base := &Type{ID: "t", Version: 2, Schema: []byte("schema"), Migration: []byte("jq-old")}

	// Migration-only in-place edit (same version + schema, changed migration):
	// the one case that leaves synced history silently stale → advisory.
	changed := &Type{ID: "t", Version: 2, Schema: []byte("schema"), Migration: []byte("jq-new")}
	require.NotEmpty(t, MigrationEditAdvisory(base, changed))
	require.Contains(t, MigrationEditAdvisory(base, changed), "read-models.md")

	// Byte-identical no-op (migration unchanged) → no advisory.
	require.Empty(t, MigrationEditAdvisory(base,
		&Type{ID: "t", Version: 2, Schema: []byte("schema"), Migration: []byte("jq-old")}))

	// Schema + version bump → the new version owns its own migration → no advisory.
	require.Empty(t, MigrationEditAdvisory(base,
		&Type{ID: "t", Version: 3, Schema: []byte("schema2"), Migration: []byte("jq-new")}))

	// Migration changed but version also bumped (defensive) → no advisory.
	require.Empty(t, MigrationEditAdvisory(base,
		&Type{ID: "t", Version: 3, Schema: []byte("schema"), Migration: []byte("jq-new")}))

	// Brand-new type (no before) and a nil after are both no-advisory.
	require.Empty(t, MigrationEditAdvisory(nil, changed))
	require.Empty(t, MigrationEditAdvisory(base, nil))
}
