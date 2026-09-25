package eventlog_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestBackupRoundTripAfterErasure(t *testing.T) {
	for _, backend := range backendsWithSegmentBytes(128) {
		for _, mode := range []string{"empty", "sparse", "erased"} {
			t.Run(backend.name+"/"+mode, func(t *testing.T) {
				log, err := backend.create(t.TempDir())
				require.NoError(t, err)
				defer func() { _ = log.Close() }()
				if mode != "empty" {
					for id := uint64(0); id < 20; id++ {
						require.NoError(t, log.Append([]eventlog.Record{{ID: id, Payload: bytes.Repeat([]byte{byte(id)}, 80)}}))
					}
					_, err := log.Rewrite(context.Background(), 7, func(r eventlog.Record) ([]byte, bool, error) {
						return r.Payload, mode == "sparse" && r.ID%2 == 0, nil
					})
					require.NoError(t, err)
				}
				target := t.TempDir()
				require.NoError(t, log.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
					var data bytes.Buffer
					if err := write(&data); err != nil {
						return err
					}
					require.Equal(t, size, int64(data.Len()))
					path := filepath.Join(target, filepath.FromSlash(name))
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						return err
					}
					return os.WriteFile(path, data.Bytes(), 0o600)
				}))
				restored, err := backend.open(target)
				require.NoError(t, err)
				defer func() { _ = restored.Close() }()
				generation, err := restored.Generation()
				require.NoError(t, err)
				last, has, err := restored.LastAppended()
				require.NoError(t, err)
				if mode == "empty" {
					require.False(t, has)
					require.Zero(t, generation)
				} else {
					require.True(t, has)
					require.Equal(t, uint64(19), last)
					require.Equal(t, uint64(7), generation)
				}
				for id := uint64(0); id < 20; id++ {
					r, err := restored.Read(id)
					if mode == "sparse" && id%2 == 0 {
						require.NoError(t, err)
						require.Equal(t, bytes.Repeat([]byte{byte(id)}, 80), r.Payload)
					} else {
						require.ErrorIs(t, err, eventlog.ErrNotFound)
					}
				}
				require.NoError(t, restored.Append([]eventlog.Record{{ID: 20, Payload: []byte("next")}}))
			})
		}
	}
}
