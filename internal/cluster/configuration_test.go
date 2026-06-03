package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigurationRoundTrip verifies Marshal/Unmarshal preserves every
// field — including Name, which is persisted in the LogConfiguration proto
// and surfaced by the config list/get endpoints.
func TestConfigurationRoundTrip(t *testing.T) {
	original := &Configuration{
		ID:       "db-1",
		Name:     "primary-postgres",
		MimeType: "text/toml",
		Data:     []byte("[database]\nname = \"primary-postgres\"\ntype = \"sql\""),
	}

	bs, err := original.Marshal()
	require.Nil(t, err)

	var got Configuration
	require.Nil(t, got.Unmarshal(bs))

	require.Equal(t, original.ID, got.ID)
	require.Equal(t, original.Name, got.Name)
	require.Equal(t, original.MimeType, got.MimeType)
	require.Equal(t, original.Data, got.Data)
}
