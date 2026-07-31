package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRejectGeneratedColumnRefs: an explicit mapping or PK of a generated column
// is refused at POST — committed can't replicate it (present on the snapshot, null
// on every CDC change) — while a normal column, or a config that touches no
// generated column, passes.
func TestRejectGeneratedColumnRefs(t *testing.T) {
	gen := map[string][]string{"orders": {"total"}}

	t.Run("explicit mapping of a generated column is rejected", func(t *testing.T) {
		err := rejectGeneratedColumnRefs(&TopicSpec{Mappings: []Mapping{{JsonName: "total", SQLColumn: "total"}}}, gen)
		require.Error(t, err)
		require.Contains(t, err.Error(), "generated")
	})
	t.Run("case-insensitive", func(t *testing.T) {
		require.Error(t, rejectGeneratedColumnRefs(&TopicSpec{Mappings: []Mapping{{JsonName: "t", SQLColumn: "TOTAL"}}}, gen))
	})
	t.Run("generated primaryKey is rejected", func(t *testing.T) {
		err := rejectGeneratedColumnRefs(&TopicSpec{PrimaryKey: []string{"total"}}, gen)
		require.Error(t, err)
		require.Contains(t, err.Error(), "primaryKey")
	})
	t.Run("normal mapping/PK passes", func(t *testing.T) {
		cfg := &TopicSpec{Mappings: []Mapping{{JsonName: "price", SQLColumn: "price"}}, PrimaryKey: []string{"id"}}
		require.NoError(t, rejectGeneratedColumnRefs(cfg, gen))
	})
	t.Run("no generated columns is a no-op", func(t *testing.T) {
		require.NoError(t, rejectGeneratedColumnRefs(&TopicSpec{Mappings: []Mapping{{JsonName: "total", SQLColumn: "total"}}}, nil))
	})
}

// TestExcludeGeneratedFromMapAll: generated columns are dropped from the map-all
// column set so they aren't auto-mirrored; normal columns and order are kept.
func TestExcludeGeneratedFromMapAll(t *testing.T) {
	cols := map[string][]string{"orders": {"id", "price", "qty", "total"}}
	gen := map[string][]string{"orders": {"total"}}
	require.Equal(t, []string{"id", "price", "qty"}, excludeGeneratedFromMapAll(cols, gen)["orders"])

	// No generated columns → unchanged set.
	require.Equal(t, []string{"id", "price", "qty", "total"}, excludeGeneratedFromMapAll(cols, nil)["orders"])
}
