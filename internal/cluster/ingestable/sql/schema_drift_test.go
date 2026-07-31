package sql_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestReconcileSchema covers the runtime schema-drift classifier: it splits a
// config's column contract (primaryKey ∪ mapped) against the observed (live decode)
// schema into the two response tiers — a missing primaryKey column is corruption
// (park, via ParkError), a missing mapped non-key column is divergence (warn).
func TestReconcileSchema(t *testing.T) {
	observed := func(cols ...string) map[string]bool {
		s := make(map[string]bool, len(cols))
		for _, c := range cols {
			s[c] = true
		}
		return s
	}
	cfg := func(pk []string, mapped ...string) *sql.TopicSpec {
		ms := make([]sql.Mapping, 0, len(mapped))
		for _, c := range mapped {
			ms = append(ms, sql.Mapping{JsonName: c, SQLColumn: c})
		}
		return &sql.TopicSpec{PrimaryKey: pk, Mappings: ms}
	}

	t.Run("whole contract present is clean", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"id"}, "id", "name"), observed("id", "name"))
		require.Empty(t, d.MissingKey)
		require.Empty(t, d.MissingMapped)
		require.NoError(t, d.ParkError())
	})

	t.Run("case-insensitive match is clean", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"ID"}, "Name"), observed("id", "name"))
		require.Empty(t, d.MissingKey)
		require.Empty(t, d.MissingMapped)
	})

	t.Run("missing primaryKey is corruption (park)", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"id"}, "id", "name"), observed("id2", "name"))
		require.Equal(t, []string{"id"}, d.MissingKey)
		require.Empty(t, d.MissingMapped, "the renamed key is corruption, not also divergence")
		err := d.ParkError()
		require.ErrorIs(t, err, sql.ErrPrimaryKeyColumnMissing)
		require.Contains(t, err.Error(), `[id]`, "the park error must name the missing key column")
	})

	t.Run("missing mapped non-key is divergence (no park)", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"id"}, "id", "name", "extra"), observed("id", "name"))
		require.Empty(t, d.MissingKey)
		require.Equal(t, []string{"extra"}, d.MissingMapped)
		require.NoError(t, d.ParkError(), "a divergence must not park the worker")
	})

	t.Run("both tiers at once", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"id"}, "id", "name", "extra"), observed("name"))
		require.Equal(t, []string{"id"}, d.MissingKey)
		require.Equal(t, []string{"extra"}, d.MissingMapped)
		require.ErrorIs(t, d.ParkError(), sql.ErrPrimaryKeyColumnMissing)
	})

	t.Run("a keyed-and-mapped column counts only as corruption", func(t *testing.T) {
		// "id" is both the primary key and an explicit mapping; when it vanishes it
		// must be reported once (park), not also as a divergence warn.
		d := sql.ReconcileSchema(cfg([]string{"id"}, "id"), observed("other"))
		require.Equal(t, []string{"id"}, d.MissingKey)
		require.Empty(t, d.MissingMapped)
	})

	t.Run("composite key names the missing member", func(t *testing.T) {
		d := sql.ReconcileSchema(cfg([]string{"tenant", "id"}, "tenant", "id"), observed("tenant", "name"))
		require.Equal(t, []string{"id"}, d.MissingKey)
		require.ErrorIs(t, d.ParkError(), sql.ErrPrimaryKeyColumnMissing)
	})

	t.Run("empty entries and empty contract are no-ops", func(t *testing.T) {
		require.NoError(t, sql.ReconcileSchema(cfg(nil), observed("id")).ParkError())
		require.NoError(t, sql.ReconcileSchema(cfg([]string{""}), observed("id")).ParkError())
	})

	t.Run("wrapped park sentinel is matchable", func(t *testing.T) {
		err := sql.ReconcileSchema(cfg([]string{"id"}), observed("other")).ParkError()
		require.True(t, errors.Is(err, sql.ErrPrimaryKeyColumnMissing))
	})
}
