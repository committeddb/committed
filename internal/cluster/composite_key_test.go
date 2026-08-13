package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The composite-key encoding is the producer↔consumer contract of the entity
// model (ingest cursors AND syncable delete tombstones decode it), pinned at
// its owning package. The encoding is frozen: changing it re-keys every
// existing composite entity.
func TestCompositeKeyContract(t *testing.T) {
	t.Run("single column is the bare value", func(t *testing.T) {
		key := cluster.CompositeKey(map[string]any{"id": "42"}, []string{"id"})
		require.Equal(t, "42", key, "single keys must stay byte-stable (existing entities)")
		vals, err := cluster.DecodeCompositeKey(key, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"42"}, vals)
	})

	t.Run("composite is a JSON array in COLUMN ORDER", func(t *testing.T) {
		m := map[string]any{"tenantid": "7", "projectid": "42"}
		key := cluster.CompositeKey(m, []string{"TenantId", "ProjectId"})
		require.Equal(t, `["7","42"]`, key, "values marshal in the configured column order, case-folded lookup")

		vals, err := cluster.DecodeCompositeKey(key, 2)
		require.NoError(t, err)
		require.Equal(t, []string{"7", "42"}, vals, "decode returns producer column order — the ordering contract")

		// The ordering hazard, pinned: a consumer configured in the OTHER order
		// gets the same values in the same positions — nothing can detect the
		// swap. This is why both config docs state order as a contract.
		swapped := cluster.CompositeKey(m, []string{"ProjectId", "TenantId"})
		require.Equal(t, `["42","7"]`, swapped)
	})

	t.Run("arity mismatch is loud", func(t *testing.T) {
		_, err := cluster.DecodeCompositeKey(`["a","b"]`, 3)
		require.Error(t, err)
		require.NotContains(t, err.Error(), `"a"`, "key values are PII and must not appear in errors")
	})

	t.Run("non-UTF-8 values round-trip via the b64 form", func(t *testing.T) {
		m := map[string]any{"bin": string([]byte{0xff, 0xfe}), "id": "x"}
		key := cluster.CompositeKey(m, []string{"bin", "id"})
		vals, err := cluster.DecodeCompositeKey(key, 2)
		require.NoError(t, err)
		require.Equal(t, []string{string([]byte{0xff, 0xfe}), "x"}, vals)
	})
}
