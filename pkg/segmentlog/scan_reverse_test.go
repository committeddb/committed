package segmentlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanReverseRangesAndReopen(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			name := "uncached"
			if cached {
				name = "cached"
			}
			if indexed {
				name += "/indexed"
			} else {
				name += "/append-files"
			}
			t.Run(name, func(t *testing.T) {
				opts := LogOptions{SegmentBytes: 48, Encoding: Options{Compression: ZstdDefault}}
				if cached {
					opts.Cache = CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}
				}
				path := t.TempDir()
				log, err := CreateLog(path, 0, opts)
				require.NoError(t, err)
				t.Cleanup(func() { _ = log.Close() })
				for id := uint64(0); id < 120; id += 10 {
					require.NoError(t, log.Append([]Record{{ID: id, Payload: []byte("original")}}))
				}
				want := []uint64{110, 100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}
				if indexed {
					_, err = log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
						return []byte("changed"), r.ID != 110 && r.ID >= 30 && r.ID != 70, nil
					})
					require.NoError(t, err)
					want = []uint64{100, 90, 80, 60, 50, 40, 30}
				}
				for round := 0; round < 2; round++ {
					for _, limit := range []int{0, 1, 3, 100} {
						var got []uint64
						n, err := log.ScanReverse(t.Context(), limit, func(r Record) (bool, error) {
							got = append(got, r.ID)
							r.Payload[0] = 'X' // Returned bytes must not modify cached storage.
							return true, nil
						})
						require.NoError(t, err)
						require.Equal(t, min(limit, len(want)), n)
						if limit > 0 {
							require.Equal(t, want[:n], got)
						}
					}
					r, err := log.Read(want[0])
					require.NoError(t, err)
					require.NotEqual(t, byte('X'), r.Payload[0])
					require.NoError(t, log.Close())
					log, err = OpenLog(path, opts.Encoding, opts.Cache)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestScanReverseStopsAndErrors(t *testing.T) {
	log := newBoltLog(t, 48)
	for id := uint64(0); id < 12; id++ {
		require.NoError(t, log.Append([]Record{{ID: id, Payload: []byte("value")}}))
	}
	catalog, err := log.catalog.Current()
	require.NoError(t, err)
	require.Greater(t, len(catalog.Segments), 1)
	// A bounded read from the head must not inspect older unrelated files.
	require.NoError(t, os.Remove(filepath.Join(log.path, catalog.Segments[0].File)))
	n, err := log.ScanReverse(t.Context(), 1, func(r Record) (bool, error) {
		require.Equal(t, uint64(11), r.ID)
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = log.ScanReverse(t.Context(), 100, func(Record) (bool, error) { return false, nil })
	require.NoError(t, err)
	require.Equal(t, 1, n)
	failure := errors.New("callback failed")
	n, err = log.ScanReverse(t.Context(), 100, func(Record) (bool, error) { return false, failure })
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, n)
	ctx, cancel := context.WithCancel(t.Context())
	n, err = log.ScanReverse(ctx, 100, func(Record) (bool, error) { cancel(); return true, nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, n)
	_, err = log.ScanReverse(t.Context(), 100, func(Record) (bool, error) { return true, nil })
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = log.ScanReverse(t.Context(), -1, func(Record) (bool, error) { return true, nil })
	require.ErrorIs(t, err, ErrInvalid)
	_, err = log.ScanReverse(t.Context(), 1, nil)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestScanReverseEmptyErasedAndClosed(t *testing.T) {
	log := newBoltLog(t, 48)
	visit := func(Record) (bool, error) { t.Fatal("unexpected record"); return false, nil }
	n, err := log.ScanReverse(t.Context(), 5, visit)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, log.Append([]Record{{ID: 0}, {ID: 100}}))
	_, err = log.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil })
	require.NoError(t, err)
	n, err = log.ScanReverse(t.Context(), 5, visit)
	require.NoError(t, err)
	require.Zero(t, n)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = log.ScanReverse(ctx, 5, visit)
	require.ErrorIs(t, err, context.Canceled)
	_, err = log.ScanReverse(nil, 5, visit) //nolint:staticcheck // SA1012: deliberately verify rejection of a nil context.
	require.ErrorIs(t, err, ErrInvalid)
	require.NoError(t, log.Close())
	_, err = log.ScanReverse(t.Context(), 5, visit)
	require.ErrorIs(t, err, ErrClosed)
}
