package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// Exercise the storage lifecycle used by snapshot catch-up with a real scrub
// command across native and shared sources and receivers.
func TestScrubbedPeerSnapshotBackends(t *testing.T) {
	for _, sourceName := range []string{"production-tidwall", "tidwall", "segmented", "segmented-cached"} {
		t.Run(sourceName, func(t *testing.T) {
			source, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[sourceName], WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })
			applyRaw := func(raw []byte) {
				entry := new(pb.Entry)
				require.NoError(t, proto.Unmarshal(raw, entry))
				require.NoError(t, source.Save(&pb.HardState{Term: proto.Uint64(3), Commit: entry.Index}, []*pb.Entry{entry}, &pb.Snapshot{}))
				require.NoError(t, source.ApplyCommittedBatch([]*pb.Entry{entry}))
			}
			applyEntity := func(index uint64, entity *cluster.Entity) {
				data, err := (&cluster.Proposal{Entities: []*cluster.Entity{entity}}).Marshal()
				require.NoError(t, err)
				raw, err := proto.Marshal(&pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum(), Data: data})
				require.NoError(t, err)
				applyRaw(raw)
			}
			typ := &cluster.Type{ID: "items", Name: "items", Version: 1}
			registration, err := cluster.NewUpsertTypeEntity(typ)
			require.NoError(t, err)
			applyEntity(1, registration)
			applyRaw(experimentEntry(t, 2, pb.EntryNormal, experimentRow("removed", "private")))
			applyEntity(3, cluster.NewDeleteEntity(typ, []byte("removed")))
			applyRaw(experimentEntry(t, 4, pb.EntryNormal, experimentRow("kept", "value")))
			command, err := cluster.NewScrubEntity(4, false)
			require.NoError(t, err)
			applyEntity(5, command)
			require.NoError(t, source.runPendingScrub())
			require.Equal(t, uint64(4), source.EventLogGeneration())
			_, err = source.ActualAt(2)
			require.ErrorIs(t, err, ErrActualNotFound)
			snapshot, err := source.CreateSnapshot(5, &pb.ConfState{Voters: []uint64{1}})
			require.NoError(t, err)
			completed, err := source.SnapshotScrubCompleted(snapshot)
			require.NoError(t, err)
			require.Equal(t, uint64(4), completed)

			for targetName := range storageTestOpeners() {
				for _, interrupted := range []bool{false, true} {
					phase := "installed"
					if interrupted {
						phase = "saved-before-restore"
					}
					t.Run(targetName+"/"+phase, func(t *testing.T) {
						path := t.TempDir()
						opener := storageTestOpeners()[targetName]
						target, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
						require.NoError(t, err)
						t.Cleanup(func() { _ = target.Close() })
						release := target.BeginCatchUp()
						defer release()
						result, err := source.ServeEvents(t.Context(), 0, 5, &receiverSink{recv: target})
						require.NoError(t, err)
						require.False(t, result.More)
						require.Equal(t, uint64(5), target.EventIndex(), "the retained scrub command supplies the frontier")
						require.Zero(t, target.AppliedIndex(), "fetching history does not apply it")
						require.NoError(t, target.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(5)}, nil, snapshot))
						if !interrupted {
							require.NoError(t, target.RestoreSnapshot(snapshot))
							require.Equal(t, uint64(5), target.AppliedIndex())
						}
						release()
						require.NoError(t, target.Close())
						resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
						require.NoError(t, err)
						t.Cleanup(func() { _ = resumed.Close() })
						require.Equal(t, uint64(5), resumed.AppliedIndex())
						require.Equal(t, uint64(5), resumed.EventIndex())
						require.Equal(t, uint64(4), resumed.EventLogGeneration())
						// Compare original protobuf bytes as well as sparse indexes.
						want, got := source.eventLog.records(), resumed.eventLog.records()
						defer func() { _ = want.Close() }()
						defer func() { _ = got.Close() }()
						for _, index := range []uint64{1, 3, 4, 5} {
							expected, err := want.Seek(index)
							require.NoError(t, err)
							actual, err := got.Seek(index)
							require.NoError(t, err)
							require.Equal(t, expected, actual)
						}
						_, err = got.Seek(6)
						require.ErrorIs(t, err, eventlog.ErrNotFound)
						_, err = resumed.ActualAt(2)
						require.ErrorIs(t, err, ErrActualNotFound)
						entry := new(pb.Entry)
						require.NoError(t, proto.Unmarshal(experimentEntry(t, 6, pb.EntryNormal, experimentRow("later", "value")), entry))
						require.NoError(t, resumed.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(6)}, []*pb.Entry{entry}, &pb.Snapshot{}))
						require.NoError(t, resumed.ApplyCommittedBatch([]*pb.Entry{entry}))
						require.Equal(t, uint64(6), resumed.AppliedIndex())
						actual, err := resumed.ActualAt(6)
						require.NoError(t, err)
						require.Equal(t, uint64(6), actual.Index)
					})
				}
			}
		})
	}
}
