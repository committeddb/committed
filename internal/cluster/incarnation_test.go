package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An incarnation round-trips through its string form and its entity; zero
// and malformed ones are refused.
func TestIncarnation_RoundTrips(t *testing.T) {
	inc, err := NewIncarnation()
	require.NoError(t, err)
	parsed, err := ParseIncarnation(inc.String())
	require.NoError(t, err)
	require.Equal(t, inc, parsed)

	e, err := NewNodeIncarnationEntity(7, inc)
	require.NoError(t, err)
	require.True(t, IsNodeIncarnation(e.Type.ID))
	node, got, err := NodeIncarnationFromEntity(e)
	require.NoError(t, err)
	require.Equal(t, uint64(7), node)
	require.Equal(t, inc, got)

	for _, bad := range []string{"", "abc", Incarnation{}.String(), "zz" + inc.String()[2:]} {
		_, err := ParseIncarnation(bad)
		require.Error(t, err, "%q", bad)
	}
	_, err = NewNodeIncarnationEntity(7, Incarnation{})
	require.Error(t, err)
}
