package eventlog_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestEventLogReverseScan(t *testing.T) {
	for _, backend := range backends() {
		t.Run(backend.name, func(t *testing.T) {
			path := t.TempDir()
			log, err := backend.create(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = log.Close() })
			records := make([]eventlog.Record, 0, 4)
			for _, id := range []uint64{0, 10, 30, 100} {
				records = append(records, eventlog.Record{ID: id, Payload: []byte("value")})
			}
			require.NoError(t, log.Append(records))
			for _, limit := range []int{0, 1, 3, 8} {
				var got []uint64
				n, err := log.ScanReverse(t.Context(), limit, func(r eventlog.Record) (bool, error) {
					got = append(got, r.ID)
					return true, nil
				})
				require.NoError(t, err)
				require.Equal(t, min(limit, 4), n)
				if n > 0 {
					require.Equal(t, []uint64{100, 30, 10, 0}[:n], got)
				}
			}
			n, err := log.ScanReverse(t.Context(), 8, func(eventlog.Record) (bool, error) { return false, nil })
			require.NoError(t, err)
			require.Equal(t, 1, n)
			failure := errors.New("stop")
			n, err = log.ScanReverse(t.Context(), 8, func(eventlog.Record) (bool, error) { return false, failure })
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, n)
			ctx, cancel := context.WithCancel(t.Context())
			n, err = log.ScanReverse(ctx, 8, func(eventlog.Record) (bool, error) { cancel(); return true, nil })
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, n)
			_, err = log.ScanReverse(t.Context(), -1, func(eventlog.Record) (bool, error) { return true, nil })
			require.ErrorIs(t, err, eventlog.ErrInvalid)
			_, err = log.ScanReverse(t.Context(), 1, nil)
			require.ErrorIs(t, err, eventlog.ErrInvalid)
			_, err = log.Rewrite(t.Context(), 1, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, r.ID == 10, nil })
			require.NoError(t, err)
			require.NoError(t, log.Close())
			log, err = backend.open(path)
			require.NoError(t, err)
			n, err = log.ScanReverse(t.Context(), 8, func(r eventlog.Record) (bool, error) {
				require.Equal(t, uint64(10), r.ID)
				return true, nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, n)
			_, err = log.Rewrite(t.Context(), 2, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			require.NoError(t, err)
			n, err = log.ScanReverse(t.Context(), 8, func(eventlog.Record) (bool, error) { t.Fatal("erased record"); return false, nil })
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
}
