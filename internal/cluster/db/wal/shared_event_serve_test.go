package wal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLogicalRecordTransferAcrossBackends(t *testing.T) {
	for sourceName, sourceOpener := range storageTestOpeners() {
		t.Run(sourceName, func(t *testing.T) {
			source, err := openStorage(t.TempDir(), nil, nil, nil, sourceOpener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })
			payloads := make([][]byte, 0, 3)
			for _, id := range []uint64{20, 40, 70} {
				payloads = append(payloads, append(experimentEntry(t, id, pb.EntryNormal), 0xa0, 0x06, 0x07))
			}
			release := source.BeginCatchUp()
			require.NoError(t, source.appendFetchedEntries(0, payloads))
			release()
			for targetName, targetOpener := range storageTestOpeners() {
				t.Run(targetName, func(t *testing.T) {
					targetPath := t.TempDir()
					target, err := openStorage(targetPath, nil, nil, nil, targetOpener, WithSafeMode())
					require.NoError(t, err)
					t.Cleanup(func() { _ = target.Close() })
					release := target.BeginCatchUp()
					after := uint64(0)
					var received [][]byte
					for calls := 0; ; calls++ {
						require.Less(t, calls, 4, "bounded batches must make progress")
						batch, err := source.fetchEntries(t.Context(), after, 100, 1, 1)
						require.NoError(t, err)
						require.Equal(t, uint64(70), batch.frontier)
						require.Equal(t, uint64(0), batch.generation)
						require.Len(t, batch.payloads, 1, "one oversized record is allowed to make progress")
						require.Greater(t, batch.after, after)
						require.NoError(t, target.appendFetchedEntries(batch.generation, batch.payloads))
						require.NoError(t, target.appendFetchedEntries(batch.generation, batch.payloads), "retry overlap is harmless")
						received = append(received, batch.payloads...)
						after = batch.after
						if batch.done {
							break
						}
					}
					require.Equal(t, payloads, received, "bytes remain valid after subsequent seeks and cursor close")
					release()
					require.NoError(t, target.Close())
					reopened, err := openStorage(targetPath, nil, nil, nil, targetOpener, WithSafeMode())
					require.NoError(t, err)
					t.Cleanup(func() { _ = reopened.Close() })
					batch, err := reopened.fetchEntries(t.Context(), 0, 100, 10, 1024)
					require.NoError(t, err)
					require.True(t, batch.done)
					require.Equal(t, payloads, batch.payloads)
					require.Equal(t, uint64(70), reopened.EventIndex())
					require.Zero(t, reopened.AppliedIndex())
				})
			}
			batch, err := source.fetchEntries(t.Context(), 20, 65, 10, 1024)
			require.NoError(t, err)
			require.Equal(t, payloads[1:2], batch.payloads)
			require.True(t, batch.done)
			require.Equal(t, uint64(65), batch.after, "coverage may end in a sparse gap")
			batch, err = source.fetchEntries(t.Context(), 0, 100, 10, len(payloads[0])+len(payloads[1]))
			require.NoError(t, err)
			require.Equal(t, payloads[:2], batch.payloads)
			require.False(t, batch.done)
			require.Equal(t, uint64(40), batch.after)
			batch, err = source.fetchEntries(t.Context(), 70, 100, 10, 1024)
			require.NoError(t, err)
			require.True(t, batch.done)
			require.Empty(t, batch.payloads)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = source.fetchEntries(ctx, 0, 100, 10, 1024)
			require.ErrorIs(t, err, context.Canceled)
			_, err = source.fetchEntries(t.Context(), 40, 20, 10, 1024)
			require.ErrorIs(t, err, eventlog.ErrInvalid)
		})
	}
}

func TestLogicalRecordBatchReportsPublishedGeneration(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			release := s.BeginCatchUp()
			require.NoError(t, s.appendFetchedEntries(0, [][]byte{experimentEntry(t, 10, pb.EntryNormal)}))
			release()
			_, err = s.eventLog.managed.Rewrite(t.Context(), 7, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			require.NoError(t, err)
			batch, err := s.fetchEntries(t.Context(), 0, 100, 10, 1024)
			require.NoError(t, err)
			require.Equal(t, uint64(7), batch.generation)
			require.Equal(t, uint64(10), batch.frontier, "append accounting survives complete erasure")
			require.Equal(t, uint64(10), batch.after)
			require.True(t, batch.done)
			require.Empty(t, batch.payloads)
		})
	}
}
