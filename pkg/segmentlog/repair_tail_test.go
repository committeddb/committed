package segmentlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepairTailDurabilityBoundary(t *testing.T) {
	for _, required := range []uint64{10, 20} {
		for _, commit := range []bool{false, true} {
			t.Run(fmtTailRepairCase(required, commit), func(t *testing.T) {
				l := newLog(t, 1024)
				require.NoError(t, l.Append([]Record{{ID: 10, Payload: []byte("first")}}))
				before, err := l.tail.State()
				require.NoError(t, err)
				require.NoError(t, l.Append([]Record{{ID: 20, Payload: []byte("second")}}))
				c, err := l.InspectCatalog()
				require.NoError(t, err)
				path := filepath.Join(l.path, c.Active.File)
				require.NoError(t, l.Close())
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(path, info.Size()-1))
				files := inspectionFiles(t, l.path)
				result, err := RepairTail(t.Context(), l.path, required, commit)
				require.Equal(t, before.End, result.Tail.End)
				if required == 20 {
					require.ErrorIs(t, err, ErrInvalid)
				} else {
					require.NoError(t, err)
				}
				if required == 20 || !commit {
					require.Equal(t, files, inspectionFiles(t, l.path))
				} else {
					inspected, err := InspectDirectory(t.Context(), l.path)
					require.NoError(t, err)
					require.Equal(t, uint64(1), inspected.Records)
					resumed, err := OpenLog(l.path, l.encoding)
					require.NoError(t, err)
					defer func() { _ = resumed.Close() }()
					require.NoError(t, resumed.Append([]Record{{ID: 20, Payload: []byte("replayed")}}))
				}
			})
		}
	}
}

func fmtTailRepairCase(required uint64, commit bool) string {
	name := "retained-boundary"
	if required == 20 {
		name = "missing-boundary"
	}
	if commit {
		return name + "/commit"
	}
	return name + "/dry-run"
}

func TestRepairTailRejectsCorruptionAndCheckpointLoss(t *testing.T) {
	for _, damage := range []string{"checksum", "checkpoint"} {
		t.Run(damage, func(t *testing.T) {
			l, err := CreateLog(t.TempDir(), 1, LogOptions{SegmentBytes: 1024})
			require.NoError(t, err)
			t.Cleanup(func() { _ = l.Close() })
			require.NoError(t, l.Append([]Record{{ID: 10, Payload: []byte("first")}, {ID: 20, Payload: []byte("second")}}))
			if damage == "checkpoint" {
				_, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID == 10, nil })
				require.NoError(t, err)
			}
			c, err := l.InspectCatalog()
			require.NoError(t, err)
			require.NoError(t, l.Close())
			path := filepath.Join(l.path, c.Active.File)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			if damage == "checksum" {
				data[len(data)-1] ^= 1
			} else {
				data = data[:tailHeaderSize+1]
			}
			require.NoError(t, os.WriteFile(path, data, 0o600))
			before := inspectionFiles(t, l.path)
			_, err = RepairTail(t.Context(), l.path, 0, true)
			require.Error(t, err)
			require.Equal(t, before, inspectionFiles(t, l.path))
		})
	}
}

func TestRepairTailOwnershipAndCancellation(t *testing.T) {
	l := newLog(t, 128)
	_, err := RepairTail(t.Context(), l.path, 0, true)
	require.ErrorIs(t, err, ErrLocked)
	require.NoError(t, l.Close())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = RepairTail(ctx, l.path, 0, true)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRepairTailPreservesErasedProgress(t *testing.T) {
	l := newLog(t, 1024)
	require.NoError(t, l.Append([]Record{{ID: 20, Payload: []byte("erased")}}))
	_, err := l.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil })
	require.NoError(t, err)
	c, err := l.InspectCatalog()
	require.NoError(t, err)
	require.NoError(t, l.Close())
	path := filepath.Join(l.path, c.Active.File)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, 1), 0o600))
	result, err := RepairTail(t.Context(), l.path, 20, true)
	require.NoError(t, err)
	require.Zero(t, result.Records)
	require.True(t, result.Tail.HasRecords)
	require.Equal(t, uint64(20), result.Tail.Last)
	inspected, err := InspectDirectory(t.Context(), l.path)
	require.NoError(t, err)
	require.Equal(t, result.Tail, inspected.Tail)
}
