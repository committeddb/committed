package segmentlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResetProcessCrash(t *testing.T) {
	for _, stage := range []string{"before", "inside", "after"} {
		t.Run(stage, func(t *testing.T) {
			path := t.TempDir()
			log, err := CreateLog(path, 0, LogOptions{SegmentBytes: 32})
			require.NoError(t, err)
			require.NoError(t, log.Append([]Record{{0, nil}, {10, nil}, {20, nil}, {30, nil}, {40, nil}}))
			before, err := log.InspectCatalog()
			require.NoError(t, err)
			require.NoError(t, log.Close())
			runBoltCrash(t, path, "reset", stage)
			restored, err := OpenLog(path, Options{})
			require.NoError(t, err)
			defer func() { _ = restored.Close() }()
			after, err := restored.InspectCatalog()
			require.NoError(t, err)
			last, has, err := restored.LastAppended()
			require.NoError(t, err)
			if stage == "after" {
				require.False(t, has)
				require.NotEqual(t, before.History, after.History)
				require.Empty(t, after.Segments)
				require.Equal(t, before.Generation, after.Generation)
				_, err = restored.Read(10)
				require.ErrorIs(t, err, ErrNotFound)
				result, err := restored.Reclaim(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, len(before.Segments)+1, result.RemovedFiles)
				require.NoError(t, restored.Append([]Record{{1, []byte("replacement")}}))
			} else {
				require.True(t, has)
				require.Equal(t, uint64(40), last)
				require.Equal(t, before, after)
				require.NoError(t, restored.Reset(), "unpublished reset can be retried")
			}
		})
	}
}
