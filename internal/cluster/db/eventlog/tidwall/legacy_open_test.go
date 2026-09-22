package tidwall

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestOpenLegacyPreservesNativeFormatAndReopenOptions(t *testing.T) {
	path := t.TempDir()
	// Seed using the original library to check format compatibility.
	seed, err := native.Open(path, nil)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("event"), 16)
	require.NoError(t, seed.Write(1, payload))
	require.NoError(t, seed.Close())

	opts := LegacyOptions{SegmentCacheSize: 5, SegmentSize: 64}
	for round := 0; round < 2; round++ {
		log, err := OpenLegacy(path, opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = log.Close() })
		require.Equal(t, 5, log.SegmentCacheSize())
		for id := uint64(2 + round*4); id <= uint64(5+round*4); id++ {
			require.NoError(t, log.log.Write(id, payload))
		}
		layout, err := log.log.LayoutSnapshot()
		require.NoError(t, err)
		require.NotEmpty(t, layout.Sealed, "configured segment size must cause rollover")
		for {
			did, err := log.CompressNextSealed()
			require.NoError(t, err)
			if !did {
				break
			}
		}
		layout, err = log.log.LayoutSnapshot()
		require.NoError(t, err)
		for _, segment := range layout.Sealed {
			require.True(t, native.IsCompressedSegmentPath(segment.Path))
		}
		require.False(t, native.IsCompressedSegmentPath(layout.Tail.Path))
		for id := uint64(1); id <= layout.LastIndex; id++ {
			got, err := log.log.Read(id)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		}
		require.NoError(t, log.Close())
	}
	_, err = os.Stat(filepath.Join(path, "CURRENT"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenLegacyErrors(t *testing.T) {
	t.Run("corruption", func(t *testing.T) {
		path := t.TempDir()
		// A record claiming 100 bytes but containing only one is torn.
		require.NoError(t, os.WriteFile(filepath.Join(path, "00000000000000000001"), []byte{100, 'x'}, 0o600))
		log, err := OpenLegacy(path, LegacyOptions{})
		require.Nil(t, log)
		require.ErrorIs(t, err, eventlog.ErrCorrupt)
		require.ErrorIs(t, err, native.ErrCorrupt)
	})
	t.Run("filesystem", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		log, err := OpenLegacy(path, LegacyOptions{})
		require.Nil(t, log)
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
		require.NotErrorIs(t, err, eventlog.ErrCorrupt)
	})
}
