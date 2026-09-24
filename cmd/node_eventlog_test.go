package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogEnvironmentSelectsStorage(t *testing.T) {
	for _, backend := range []string{"", "tidwall", " segmented "} {
		t.Run("backend="+backend, func(t *testing.T) {
			t.Setenv("COMMITTED_EVENT_LOG_BACKEND", backend)
			t.Setenv("COMMITTED_EVENT_CACHE_SEGMENTS", "16")
			t.Setenv("COMMITTED_EVENT_CACHE_RECENT_BYTES", "0")
			t.Setenv("COMMITTED_EVENT_CACHE_HISTORICAL_BYTES", "1024")
			options, err := loadEventLogOptions()
			require.NoError(t, err)
			base := t.TempDir()
			for range 2 {
				storage, err := wal.Open(base, nil, nil, nil, options...)
				require.NoError(t, err)
				require.NoError(t, storage.Close())
				selected, err := segmentlog.RecognizeDirectory(datadir.EventsDir(base))
				require.NoError(t, err)
				require.Equal(t, backend == " segmented ", selected)
			}
		})
	}
}

func TestEventLogEnvironmentValidation(t *testing.T) {
	t.Setenv("COMMITTED_EVENT_CACHE_RECENT_BYTES", "")
	t.Setenv("COMMITTED_EVENT_CACHE_HISTORICAL_BYTES", "")
	for _, backend := range []string{"segmeted", "rocksdb", "SEGMENTED"} {
		t.Setenv("COMMITTED_EVENT_LOG_BACKEND", backend)
		_, err := loadEventLogOptions()
		require.ErrorContains(t, err, "COMMITTED_EVENT_LOG_BACKEND")
	}
	t.Setenv("COMMITTED_EVENT_LOG_BACKEND", "segmented")
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
