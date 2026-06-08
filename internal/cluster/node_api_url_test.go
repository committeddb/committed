package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeAPIURL_RoundTrip(t *testing.T) {
	n := &NodeAPIURL{NodeID: 7, APIURL: "http://n7:8080"}
	bs, err := n.Marshal()
	require.NoError(t, err)

	got := &NodeAPIURL{}
	require.NoError(t, got.Unmarshal(bs))
	require.Equal(t, n.NodeID, got.NodeID)
	require.Equal(t, n.APIURL, got.APIURL)
}

func TestNewNodeAPIURLEntity(t *testing.T) {
	e, err := NewNodeAPIURLEntity(7, "http://n7:8080")
	require.NoError(t, err)

	require.True(t, IsNodeAPIURL(e.Type.ID))
	require.Equal(t, NodeAPIURLKey(7), e.Key)

	got := &NodeAPIURL{}
	require.NoError(t, got.Unmarshal(e.Data))
	require.Equal(t, uint64(7), got.NodeID)
	require.Equal(t, "http://n7:8080", got.APIURL)
}

// The node-api-url type is hidden from the default Proposals() listing like
// every other system type, so an operator never sees the cluster's internal
// address bookkeeping among their own data.
func TestNodeAPIURL_IsSystemType(t *testing.T) {
	require.True(t, IsSystem(nodeAPIURLType.ID))
	require.False(t, IsNodeAPIURL("not-the-type"))
	require.False(t, IsNodeAPIURL(""))
}
