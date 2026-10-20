package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	twal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// A new data directory gets the segmented event log; a tidwall one is refused
// with the conversion named, and nothing in it changes.
func TestNodeRunsTheSegmentedEventLog(t *testing.T) {
	t.Setenv("COMMITTED_EVENT_CACHE_RECENT_BYTES", "0")
	t.Setenv("COMMITTED_EVENT_CACHE_HISTORICAL_BYTES", "1024")
	options, err := loadEventLogOptions()
	require.NoError(t, err)

	base := t.TempDir()
	for range 2 {
		storage, err := wal.Open(base, nil, nil, nil, options...)
		require.NoError(t, err)
		require.NoError(t, storage.Close())
		segmented, err := segmentlog.RecognizeDirectory(datadir.EventsDir(base))
		require.NoError(t, err)
		require.True(t, segmented)
	}

	// A tidwall event log, as a node before 0.8.3 left it.
	legacy := t.TempDir()
	events, err := twal.Open(datadir.EventsDir(legacy), nil)
	require.NoError(t, err)
	require.NoError(t, events.Write(1, []byte("event")))
	require.NoError(t, events.Close())
	_, err = wal.Open(legacy, nil, nil, nil, options...)
	require.ErrorIs(t, err, wal.ErrTidwallEventLog)
	require.ErrorContains(t, err, "committed wal convert --data "+legacy)
	segmented, err := segmentlog.RecognizeDirectory(datadir.EventsDir(legacy))
	require.NoError(t, err)
	require.False(t, segmented)
}

func TestEventCacheBudgetValidation(t *testing.T) {
	for _, name := range []string{"COMMITTED_EVENT_CACHE_RECENT_BYTES", "COMMITTED_EVENT_CACHE_HISTORICAL_BYTES"} {
		t.Run(name, func(t *testing.T) {
			for _, invalid := range []string{"-1", "1.5", "160MiB", "18446744073709551616"} {
				t.Setenv(name, invalid)
				_, err := loadEventLogOptions()
				require.ErrorContains(t, err, name)
			}
		})
	}
}

func TestEventCacheByteBudgets(t *testing.T) {
	const name = "COMMITTED_EVENT_CACHE_RECENT_BYTES"
	for _, tc := range []struct {
		raw  string
		want uint64
	}{
		{"", 160 << 20}, {"  ", 160 << 20}, {"0", 0}, {" 1048576 ", 1 << 20}, {"8589934592", 8 << 30},
	} {
		t.Setenv(name, tc.raw)
		got, err := eventCacheBytesEnv(name)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}
