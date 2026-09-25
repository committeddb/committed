package wal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestFetchedGenerationAdoptionAndRestart(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			source, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })
			release := source.BeginCatchUp()
			require.NoError(t, source.SetEventLogGeneration(7))
			require.Equal(t, uint64(7), source.EventLogGeneration())
			completed, err := source.loadScrubCompleted()
			require.NoError(t, err)
			require.Equal(t, uint64(7), completed)
			ten, twenty := experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 20, pb.EntryNormal)
			require.NoError(t, source.AppendFetchedRecords(fetchedStream(frame(ten))))
			release()
			if source.eventLog.managed != nil {
				require.NoError(t, source.runPendingSharedScrub())
			}
			require.NoError(t, source.Close())
			// Reopen normally: imported completion is valid without a local pending
			// scrub command or scrub history, just as it is for native catch-up.
			resumed, err := openStorage(path, nil, nil, nil, opener)
			require.NoError(t, err)
			defer func() { _ = resumed.Close() }()
			require.Equal(t, uint64(7), resumed.EventLogGeneration())
			if resumed.eventLog.managed != nil {

				selected, err := resumed.eventLog.managed.Generation()
				require.NoError(t, err)
				require.Equal(t, uint64(7), selected)
			}
			release = resumed.BeginCatchUp()
			require.NoError(t, resumed.SetEventLogGeneration(7))
			require.NoError(t, resumed.AppendFetchedRecords(fetchedStream(frame(ten), frame(twenty))))
			release()
			require.Equal(t, uint64(20), resumed.EventIndex())
			require.Zero(t, resumed.AppliedIndex())
			cursor := resumed.eventLog.records()
			defer func() { _ = cursor.Close() }()
			for i, want := range [][]byte{ten, twenty} {
				record, err := cursor.Seek(uint64(i+1) * 10)
				require.NoError(t, err)
				require.Equal(t, want, record.Payload)
			}
		})
	}
}

func TestFetchedGenerationInterruptedPublication(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, after := range []bool{false, true} {
			phase := "before-publication"
			if after {
				phase = "after-publication"
			}
			t.Run(name+"/"+phase, func(t *testing.T) {
				path := t.TempDir()
				opener := storageTestOpeners()[name]
				s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				failure := errors.New("interrupted generation publication")
				s.eventLog.managed = failedScrubPublication{EventLog: s.eventLog.managed, after: after, failure: failure}
				release := s.BeginCatchUp()
				require.ErrorIs(t, s.SetEventLogGeneration(7), failure)
				completed, err := s.loadScrubCompleted()
				require.NoError(t, err)
				require.Equal(t, uint64(7), completed, "existing metadata is persisted before storage publication")
				release()
				require.NoError(t, s.Close())
				resumed, err := openStorage(path, nil, nil, nil, opener)
				require.NoError(t, err)
				defer func() { _ = resumed.Close() }()
				require.Equal(t, uint64(7), resumed.EventLogGeneration())
				selected, err := resumed.eventLog.managed.Generation()
				require.NoError(t, err)
				require.Equal(t, uint64(7), selected)

				release = resumed.BeginCatchUp()
				require.NoError(t, resumed.SetEventLogGeneration(7))
				require.NoError(t, resumed.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
				release()
			})
		}
	}
}

func TestFetchedGenerationDoesNotRelabelHistory(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented"} {
		for _, erased := range []bool{false, true} {
			mode := "surviving"
			if erased {
				mode = "erased"
			}
			t.Run(name+"/"+mode, func(t *testing.T) {
				path := t.TempDir()
				opener := storageTestOpeners()[name]
				s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
				if erased {
					_, err := s.eventLog.managed.Rewrite(t.Context(), 1, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
					require.NoError(t, err)
				}
				release := s.BeginCatchUp()
				require.ErrorIs(t, s.SetEventLogGeneration(7), eventlog.ErrInvalid)
				release()
				completed, err := s.loadScrubCompleted()
				require.NoError(t, err)
				require.Zero(t, completed, "ineligible adoption must not change application metadata")
				// Even if metadata claims an advanced generation, reopen must not relabel
				// existing (including completely erased) append history as a new source.
				require.NoError(t, s.putScrubCompleted(7))
				require.NoError(t, s.Close())
				_, err = openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.ErrorIs(t, err, eventlog.ErrInvalid)
			})
		}
	}
}

func TestFetchedGenerationPreservesExistingAppliedProgress(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			// An emptied event log can belong to a node that already applied entries.
			// Local applied progress is independent of the replacement log's history.
			require.NoError(t, s.saveAppliedIndex(5))
			s.appliedIndex.Store(5)
			failure := errors.New("interrupted initialization")
			s.eventLog.managed = failedScrubPublication{EventLog: s.eventLog.managed, failure: failure}
			release := s.BeginCatchUp()
			require.ErrorIs(t, s.SetEventLogGeneration(7), failure)
			release()
			require.NoError(t, s.Close())
			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = resumed.Close() }()
			require.Equal(t, uint64(5), resumed.AppliedIndex())
			require.Zero(t, resumed.EventIndex())
			require.Equal(t, uint64(7), resumed.EventLogGeneration())
			require.NoError(t, resumed.runPendingSharedScrub())
		})
	}
}

func TestSharedServeAdoptsPeerGeneration(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented"} {
		t.Run(name, func(t *testing.T) {
			source, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = source.Close() }()
			release := source.BeginCatchUp()
			require.NoError(t, source.SetEventLogGeneration(7))
			raw := experimentEntry(t, 10, pb.EntryNormal)
			require.NoError(t, source.AppendFetchedRecords(fetchedStream(frame(raw))))
			release()
			for targetName, opener := range storageTestOpeners() {
				t.Run(targetName, func(t *testing.T) {
					path := t.TempDir()
					target, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
					require.NoError(t, err)
					t.Cleanup(func() { _ = target.Close() })
					release := target.BeginCatchUp()
					defer release()
					result, err := source.ServeEvents(t.Context(), 0, 10, &receiverSink{recv: target})
					require.NoError(t, err)
					require.Equal(t, uint64(7), result.Generation)
					require.Equal(t, uint64(7), target.EventLogGeneration())
					require.Equal(t, uint64(10), target.EventIndex())
					release()
					require.NoError(t, target.Close())
					resumed, err := openStorage(path, nil, nil, nil, opener)
					require.NoError(t, err)
					defer func() { _ = resumed.Close() }()
					require.Equal(t, uint64(7), resumed.EventLogGeneration())
					require.Equal(t, uint64(10), resumed.EventIndex())
				})
			}
		})
	}
}
