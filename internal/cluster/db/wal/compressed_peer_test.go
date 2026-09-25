package wal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// Every source, receiver, and reopen uses the same publicly selected backend.
// The receiver follows the storage protocol used by peer snapshot catch-up.
func TestCompressedScrubbedPeerSnapshotBackends(t *testing.T) {
	const records, deleted = 256, 4
	entries, typ, _ := workloadEntries(t, records)
	expected := streamingExpectedRows(t, entries)
	for _, backend := range []string{"tidwall", "segmented"} {
		t.Run(backend, func(t *testing.T) {
			options := compressedStorageOptions(backend)
			source, err := Open(t.TempDir(), nil, nil, nil, options...)
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })
			require.NoError(t, applyWorkloadBatch(source, entries))
			require.Positive(t, compressWorkload(t, source))
			last := uint64(len(entries))
			for i := range deleted {
				last++
				require.NoError(t, applyWorkloadBatch(source, []*pb.Entry{
					workloadEntityEntry(t, last, cluster.NewDeleteEntity(typ, []byte(fmt.Sprint(i)))),
				}))
			}
			bound := last
			command, err := cluster.NewScrubEntity(bound, false)
			require.NoError(t, err)
			last++
			require.NoError(t, applyWorkloadBatch(source, []*pb.Entry{workloadEntityEntry(t, last, command)}))
			require.NoError(t, source.runPendingScrub())
			compressWorkload(t, source)
			require.Equal(t, bound, source.EventLogGeneration())
			snapshot, err := source.CreateSnapshot(last, &pb.ConfState{Voters: []uint64{1}})
			require.NoError(t, err)
			completed, err := source.SnapshotScrubCompleted(snapshot)
			require.NoError(t, err)
			require.Equal(t, bound, completed)

			for _, interrupted := range []bool{false, true} {
				t.Run(fmt.Sprintf("saved-before-install=%t", interrupted), func(t *testing.T) {
					path := t.TempDir()
					target, err := Open(path, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = target.Close() })
					release := target.BeginCatchUp()
					defer release()
					catchUp(t, source, target, last)
					require.Equal(t, last, target.EventIndex())
					require.Zero(t, target.AppliedIndex(), "fetching event history does not apply it")
					require.NoError(t, target.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(last)}, nil, snapshot))
					if !interrupted {
						require.NoError(t, target.RestoreSnapshot(snapshot))
					}
					release()
					require.NoError(t, target.Close())

					resumed, err := Open(path, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = resumed.Close() })
					require.Equal(t, last, resumed.AppliedIndex())
					require.Equal(t, last, resumed.EventIndex())
					require.Equal(t, bound, resumed.EventLogGeneration())
					raftLast, err := resumed.LastIndex()
					require.NoError(t, err)
					require.Equal(t, last, raftLast)
					hs, _, err := resumed.InitialState()
					require.NoError(t, err)
					require.Equal(t, last, hs.GetCommit())
					requireCompressedPeerPrefix(t, source, resumed)
					for i := range deleted {
						_, err := resumed.ActualAt(uint64(i + 2))
						require.ErrorIs(t, err, ErrActualNotFound)
					}
					next := new(pb.Entry)
					require.NoError(t, proto.Unmarshal(experimentEntry(t, last+1, pb.EntryNormal,
						experimentRow("after-catchup", "new value")), next))
					require.NoError(t, applyWorkloadBatch(resumed, []*pb.Entry{next}))
					compressWorkload(t, resumed)
					require.NoError(t, resumed.Close())
					reopened, err := Open(path, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = reopened.Close() })
					require.Equal(t, last+1, reopened.AppliedIndex())
					require.Equal(t, last+1, reopened.EventIndex())
					require.Equal(t, bound, reopened.EventLogGeneration())
					requireCompressedPeerPrefix(t, source, reopened)
					actual, err := reopened.ActualAt(last + 1)
					require.NoError(t, err)
					require.Len(t, actual.Entities, 1)
					require.Equal(t, []byte("after-catchup"), actual.Entities[0].Key)
					require.Equal(t, []byte("new value"), actual.Entities[0].Data)
					for i := range deleted {
						_, err := reopened.ActualAt(uint64(i + 2))
						require.ErrorIs(t, err, ErrActualNotFound)
					}

					// Snapshot installation must leave the receiver able to scrub
					// its fetched history using the installed application metadata.
					newBound := next.GetIndex() + 1
					require.NoError(t, applyWorkloadBatch(reopened, []*pb.Entry{
						workloadEntityEntry(t, newBound, cluster.NewDeleteEntity(typ, []byte(fmt.Sprint(deleted)))),
					}))
					command, err := cluster.NewScrubEntity(newBound, false)
					require.NoError(t, err)
					require.NoError(t, applyWorkloadBatch(reopened, []*pb.Entry{workloadEntityEntry(t, newBound+1, command)}))
					require.NoError(t, reopened.runPendingScrub())
					compressWorkload(t, reopened)
					verify := func(store *Storage) {
						t.Helper()
						require.Equal(t, newBound, store.EventLogGeneration())
						require.Equal(t, newBound+1, store.EventIndex())
						require.Equal(t, newBound+1, store.AppliedIndex())
						for i := range records {
							row, err := store.ActualAt(uint64(i + 2))
							if i <= deleted {
								require.ErrorIs(t, err, ErrActualNotFound)
								continue
							}
							require.NoError(t, err)
							require.Equal(t, uint64(i+2), row.Index)
							require.Len(t, row.Entities, 1)
							require.Equal(t, expected[i+1].Key, row.Entities[0].Key)
							require.Equal(t, expected[i+1].Data, row.Entities[0].Data)
						}
						row, err := store.ActualAt(next.GetIndex())
						require.NoError(t, err)
						require.Len(t, row.Entities, 1)
						require.Equal(t, []byte("after-catchup"), row.Entities[0].Key)
						require.Equal(t, []byte("new value"), row.Entities[0].Data)
					}
					verify(reopened)
					require.NoError(t, reopened.Close())
					afterScrub, err := Open(path, nil, nil, nil, options...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = afterScrub.Close() })
					verify(afterScrub)
				})
			}
		})
	}
}

// Compare sparse IDs and exact protobuf payloads, including internal records.
// The fixture retains its scrub command at the source's original frontier.
// A receiver may have appended later records, which are checked separately.
func requireCompressedPeerPrefix(t *testing.T, source, target *Storage) {
	t.Helper()
	want, got := source.eventLog.records(), target.eventLog.records()
	defer func() { _ = want.Close() }()
	defer func() { _ = got.Close() }()
	for next := uint64(1); next <= source.EventIndex(); {
		expected, err := want.Seek(next)
		require.NoError(t, err)
		actual, err := got.Seek(next)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		next = expected.ID + 1
	}
}
