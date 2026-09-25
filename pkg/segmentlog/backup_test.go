package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupPreservesSelectionDuringRollover(t *testing.T) {
	for _, eraseAll := range []bool{false, true} {
		name := "sparse"
		if eraseAll {
			name = "erased"
		}
		t.Run(name, func(t *testing.T) {
			source := t.TempDir()
			log, err := CreateLog(source, 1, LogOptions{SegmentBytes: 128, Encoding: Options{Compression: ZstdDefault}})
			require.NoError(t, err)
			defer func() { _ = log.Close() }()
			for id := uint64(1); id <= 20; id++ {
				require.NoError(t, log.Append([]Record{{ID: id, Payload: bytes.Repeat([]byte{byte(id)}, 80)}}))
			}
			_, err = log.Rewrite(context.Background(), 1, func(r Record) ([]byte, bool, error) {
				return r.Payload, !eraseAll && r.ID%2 == 0 && r.ID < 20, nil
			})
			require.NoError(t, err)
			target := t.TempDir()
			var captured []string
			err = log.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
				// Deterministically force rollover after the snapshot is selected but
				// before any selected payload is streamed. This must not deadlock or
				// add these later records to the backup.
				if name == boltCatalogName {
					for id := uint64(21); id <= 30; id++ {
						require.NoError(t, log.Append([]Record{{ID: id, Payload: bytes.Repeat([]byte("x"), 80)}}))
					}
				}
				var data bytes.Buffer
				if err := write(&data); err != nil {
					return err
				}
				require.Equal(t, size, int64(data.Len()))
				captured = append(captured, name)
				return os.WriteFile(filepath.Join(target, name), data.Bytes(), 0o600)
			})
			require.NoError(t, err)
			files, err := os.ReadDir(source)
			require.NoError(t, err)
			require.Less(t, len(captured), len(files), "retired and later files must be excluded")
			restored, err := OpenLog(target, Options{})
			require.NoError(t, err)
			defer func() { _ = restored.Close() }()
			id, has, err := restored.LastAppended()
			require.NoError(t, err)
			require.True(t, has)
			require.Equal(t, uint64(20), id, "erased append progress survives")
			generation, err := restored.Generation()
			require.NoError(t, err)
			require.Equal(t, uint64(1), generation)
			for id := uint64(1); id <= 30; id++ {
				r, err := restored.Read(id)
				if !eraseAll && id%2 == 0 && id < 20 {
					require.NoError(t, err)
					require.Equal(t, bytes.Repeat([]byte{byte(id)}, 80), r.Payload)
				} else {
					require.ErrorIs(t, err, ErrNotFound)
				}
			}
			require.NoError(t, restored.Append([]Record{{ID: 21, Payload: []byte("new")}}))
			_, err = restored.Reclaim(context.Background())
			require.NoError(t, err, "captured retirement metadata tolerates absent retired files")
		})
	}
}

func TestBackupVisitorFailureReleasesMaintenance(t *testing.T) {
	log, err := CreateLog(t.TempDir(), 0, LogOptions{})
	require.NoError(t, err)
	errVisitor := errors.New("visitor failed")
	require.ErrorIs(t, log.CaptureBackup(func(string, int64, func(io.Writer) error) error { return errVisitor }), errVisitor)
	_, err = log.Reclaim(context.Background())
	require.NoError(t, err)
	require.NoError(t, log.Close())
}
