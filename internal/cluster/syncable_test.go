package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSyncableMode(t *testing.T) {
	tests := map[string]struct {
		in      string
		want    SyncableMode
		wantErr bool
	}{
		"empty defaults to as-stored": {"", ModeAsStored, false},
		"as-stored":                   {"as-stored", ModeAsStored, false},
		"always-current":              {"always-current", ModeAlwaysCurrent, false},
		"unknown":                     {"something-weird", 0, true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseSyncableMode(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
