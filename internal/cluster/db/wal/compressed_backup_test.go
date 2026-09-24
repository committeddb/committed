package wal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Exercise actual node archives, including metadata and Raft state, after
// physical compression and logical erasure through the public backend choice.
func TestCompressedBackupRestoreBackends(t *testing.T) {
	const records, deleted, segmentBytes = 256, 4, 64 << 10
	entries, typ, _ := workloadEntries(t, records)
	expected := streamingExpectedRows(t, entries)
	for _, backend := range []string{"tidwall", "segmented"} {
		for _, mode := range []string{"live", "offline"} {
			for _, scrubbed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/scrubbed=%t", backend, mode, scrubbed), func(t *testing.T) {
					options := []Option{WithSafeMode(), WithEventSegmentSize(segmentBytes)}
					if backend == "segmented" {
						options = append(options, WithSegmentedEventLog(segmentlog.LogOptions{
							SegmentBytes: segmentBytes,
							Encoding:     segmentlog.Options{Compression: segmentlog.ZstdDefault},
							Cache:        segmentlog.CacheOptions{RecentBytes: 1 << 20, HistoricalBytes: 1 << 20},
						}))
					}
					source := t.TempDir()
					s, err := Open(source, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = s.Close() })
					require.NoError(t, applyWorkloadBatch(s, entries))
					// Safe mode holds background workers; explicitly drain the same
					// compression capability the production sealer uses.
					require.NotNil(t, s.eventLog.compressor)
					settle := func() int {
						t.Helper()
						count := 0
						for {
							did, err := s.eventLog.compressor.CompressNextSealed()
							require.NoError(t, err)
							if !did {
								return count
							}
							count++
						}
					}
					require.Positive(t, settle(), "fixture must contain compressed sealed segments")
					last := uint64(len(entries))
					if scrubbed {
						deletes := make([]*pb.Entry, 0, deleted)
						for i := range deleted {
							last++
							deletes = append(deletes, workloadEntityEntry(t, last, cluster.NewDeleteEntity(typ, []byte(fmt.Sprint(i)))))
						}
						require.NoError(t, applyWorkloadBatch(s, deletes))
						command, err := cluster.NewScrubEntity(last, false)
						require.NoError(t, err)
						last++
						require.NoError(t, applyWorkloadBatch(s, []*pb.Entry{workloadEntityEntry(t, last, command)}))
						require.NoError(t, s.runPendingScrub())
						settle()
					}
					verify := func(store *Storage, frontier uint64) {
						t.Helper()
						require.Equal(t, frontier, store.EventIndex())
						require.Equal(t, frontier, store.AppliedIndex())
						raftLast, err := store.LastIndex()
						require.NoError(t, err)
						require.Equal(t, frontier, raftLast)
						hs, _, err := store.InitialState()
						require.NoError(t, err)
						require.Equal(t, frontier, hs.GetCommit())
						for i := range records {
							actual, err := store.ActualAt(uint64(i + 2))
							if scrubbed && i < deleted {
								require.ErrorIs(t, err, ErrActualNotFound)
								continue
							}
							require.NoError(t, err)
							require.Equal(t, uint64(i+2), actual.Index)
							require.Len(t, actual.Entities, 1)
							require.Equal(t, expected[i+1].Key, actual.Entities[0].Key)
							require.Equal(t, expected[i+1].Data, actual.Entities[0].Data)
						}
					}
					verify(s, last)
					var archive bytes.Buffer
					if mode == "live" {
						_, err = backup.CreateLive(&archive, s, 1, time.Now())
					} else {
						require.NoError(t, s.Close())
						_, err = backup.Create(&archive, source, 1, time.Now())
					}
					require.NoError(t, err)
					target := filepath.Join(t.TempDir(), "restored")
					_, err = backup.Restore(&archive, target, time.Now())
					require.NoError(t, err)
					restored, err := Open(target, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = restored.Close() })
					verify(restored, last)

					next := new(pb.Entry)
					require.NoError(t, proto.Unmarshal(experimentEntry(t, last+1, pb.EntryNormal,
						experimentRow("after-restore", "new value")), next))
					require.NoError(t, applyWorkloadBatch(restored, []*pb.Entry{next}))
					require.NoError(t, restored.Close())
					reopened, err := Open(target, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = reopened.Close() })
					verify(reopened, last+1)
					actual, err := reopened.ActualAt(last + 1)
					require.NoError(t, err)
					require.Len(t, actual.Entities, 1)
					require.Equal(t, []byte("after-restore"), actual.Entities[0].Key)
					require.Equal(t, []byte("new value"), actual.Entities[0].Data)
				})
			}
		}
	}
}
