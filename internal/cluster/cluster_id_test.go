package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterIDRoundTrip(t *testing.T) {
	c, err := NewClusterID()
	require.NoError(t, err)
	require.False(t, c.IsZero())
	s := c.String()
	require.Len(t, s, 36)
	got, err := ParseClusterID(s)
	require.NoError(t, err)
	require.Equal(t, c, got)

	for _, bad := range []string{"", "xyz", "00000000-0000-0000-0000-000000000000", s[:35]} {
		_, err := ParseClusterID(bad)
		require.Error(t, err, bad)
	}
}

// Every node derives the same id from the same canonical peer set, whatever
// order it was listed in; a different set, URL, or cluster name derives a
// different one.
func TestDeriveClusterID(t *testing.T) {
	a := map[uint64]string{1: "http://n1:9022", 2: "http://n2:9022", 3: "http://n3:9022"}
	b := map[uint64]string{3: "http://n3:9022", 1: "http://n1:9022", 2: "http://n2:9022"}
	require.Equal(t, DeriveClusterID(a, ""), DeriveClusterID(b, ""))
	require.False(t, DeriveClusterID(a, "").IsZero())

	require.NotEqual(t, DeriveClusterID(a, ""), DeriveClusterID(a, "staging"), "the cluster name separates identical configs")
	moved := map[uint64]string{1: "http://n1:9022", 2: "http://n2:9022", 3: "http://n4:9022"}
	require.NotEqual(t, DeriveClusterID(a, ""), DeriveClusterID(moved, ""))
	renumbered := map[uint64]string{1: "http://n1:9022", 2: "http://n2:9022", 4: "http://n3:9022"}
	require.NotEqual(t, DeriveClusterID(a, ""), DeriveClusterID(renumbered, ""))
}
