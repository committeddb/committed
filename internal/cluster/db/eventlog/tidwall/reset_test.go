package tidwall

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestResetPublicationFailure(t *testing.T) {
	for _, after := range []bool{false, true} {
		phase := "before"
		if after {
			phase = "after"
		}
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			log, err := Create(path, 0, Options{SegmentBytes: 128})
			require.NoError(t, err)
			require.NoError(t, log.Append([]eventlog.Record{{ID: 10, Payload: []byte("old")}}))
			errInjected := errors.New("reset publication failed")
			log.pub = failPublisher{publisher: log.pub, after: after, err: errInjected}
			require.ErrorIs(t, log.Reset(), errInjected)
			require.ErrorIs(t, log.Append([]eventlog.Record{{ID: 20}}), eventlog.ErrPoisoned)
			require.NoError(t, log.Close())
			restored, err := Open(path)
			require.NoError(t, err)
			defer func() { _ = restored.Close() }()
			last, has, err := restored.LastAppended()
			require.NoError(t, err)
			require.Equal(t, !after, has)
			if !after {
				require.Equal(t, uint64(10), last)
			}
			require.NoError(t, restored.Reset())
			_, err = restored.Reclaim(t.Context())
			require.NoError(t, err)
			require.NoError(t, restored.Append([]eventlog.Record{{ID: 1, Payload: []byte("new")}}))
		})
	}
}
