package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestResetAndRefetchBackends(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			release := s.BeginCatchUp()
			require.NoError(t, s.SetEventLogGeneration(3))
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
			require.NoError(t, s.saveAppliedIndex(10))
			s.appliedIndex.Store(10)
			frozen := s.FreezeEventLayout()
			require.ErrorIs(t, s.ResetEventLog(), ErrLayoutFrozen)
			frozen()
			require.Equal(t, uint64(10), s.EventIndex())
			require.NoError(t, s.ResetEventLog())
			require.Zero(t, s.EventIndex())
			if s.eventLog.managed != nil {
				reclaimed, err := s.eventLog.managed.Reclaim(t.Context())
				require.NoError(t, err)
				require.Zero(t, reclaimed.RemovedFiles, "reset already removed retired payloads")
			}
			require.Zero(t, s.firstEventIndex.Load())
			require.Equal(t, uint64(3), s.EventLogGeneration())
			require.Equal(t, uint64(10), s.AppliedIndex())
			release()
			require.NoError(t, s.Close())
			// Reopen between reset and adopting the replacement source generation.
			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = resumed.Close() }()
			require.Zero(t, resumed.EventIndex())
			require.Equal(t, uint64(3), resumed.EventLogGeneration())
			release = resumed.BeginCatchUp()
			defer release()
			require.NoError(t, resumed.SetEventLogGeneration(7))
			replacement := experimentEntry(t, 5, pb.EntryNormal)
			require.NoError(t, resumed.AppendFetchedRecords(fetchedStream(frame(replacement))))
			require.Equal(t, uint64(5), resumed.EventIndex(), "refetch starts below the old append frontier")
			require.Equal(t, uint64(7), resumed.EventLogGeneration())
			require.Equal(t, uint64(10), resumed.AppliedIndex())
			cursor := resumed.eventLog.records()
			defer func() { _ = cursor.Close() }()
			got, err := cursor.Seek(1)
			require.NoError(t, err)
			require.Equal(t, replacement, got.Payload)
		})
	}
}

func TestRefetchReclaimsInterruptedReset(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
			// Stop after storage publication, before the application reclaims old files.
			require.NoError(t, s.eventLog.managed.Reset())
			require.NoError(t, s.Close())
			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = resumed.Close() }()
			require.Zero(t, resumed.EventIndex())
			release := resumed.BeginCatchUp()
			defer release()
			require.NoError(t, resumed.SetEventLogGeneration(7))
			reclaimed, err := resumed.eventLog.managed.Reclaim(t.Context())
			require.NoError(t, err)
			require.Zero(t, reclaimed.RemovedFiles, "generation adoption drained reset retirement")
		})
	}
}
