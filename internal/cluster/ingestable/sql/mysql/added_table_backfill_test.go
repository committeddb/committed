package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// addedTables drives the added-table backfill: configured-but-never-
// snapshotted tables, in config order (deterministic scan order).
func TestAddedTables(t *testing.T) {
	cases := []struct {
		name        string
		configured  []string
		snapshotted []string
		want        []string
	}{
		{"nothing added", []string{"a", "b"}, []string{"a", "b"}, nil},
		{"one added", []string{"a", "b"}, []string{"a"}, []string{"b"}},
		{"added preserves config order", []string{"c", "a", "b"}, []string{"a"}, []string{"c", "b"}},
		{"all new (fresh registry, no position) ", []string{"a"}, nil, []string{"a"}},
		{"registry superset tolerated", []string{"a"}, []string{"a", "gone"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, addedTables(tc.configured, tc.snapshotted))
		})
	}
}
