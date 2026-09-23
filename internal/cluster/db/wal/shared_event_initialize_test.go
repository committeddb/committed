package wal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestInitializeFetchedGeneration(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 7), eventlog.ErrInvalid)
			release := s.BeginCatchUp()
			defer release()
			unpin := s.BeginFromZeroRead()
			require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 7), errEventRewriteDeferred)
			unpin()
			unfreeze := s.FreezeEventLayout()
			require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 7), ErrLayoutFrozen)
			unfreeze()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, s.initializeFetchedGeneration(ctx, 7), context.Canceled)
			require.NoError(t, s.initializeFetchedGeneration(t.Context(), 7))
			require.NoError(t, s.initializeFetchedGeneration(t.Context(), 7))
			require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 6), eventlog.ErrInvalid)
			completed, err := s.loadScrubCompleted()
			require.NoError(t, err)
			require.Zero(t, completed, "source generation is not local completion")
			require.Zero(t, s.EventLogGeneration())
			require.Zero(t, s.AppliedIndex())
			ten := experimentEntry(t, 10, pb.EntryNormal)
			require.NoError(t, s.appendFetchedEntries(7, [][]byte{ten}))
			require.NoError(t, s.initializeFetchedGeneration(t.Context(), 7), "partial transfer retry")
			require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 8), eventlog.ErrInvalid)
			release()
			require.NoError(t, s.Close())

			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = resumed.Close() })
			release = resumed.BeginCatchUp()
			defer release()
			require.NoError(t, resumed.initializeFetchedGeneration(t.Context(), 7))
			twenty := experimentEntry(t, 20, pb.EntryNormal)
			require.NoError(t, resumed.appendFetchedEntries(7, [][]byte{ten, twenty}))
			batch, err := resumed.fetchEntries(t.Context(), 0, 100, 10, 1024)
			require.NoError(t, err)
			require.Equal(t, uint64(7), batch.generation)
			require.Equal(t, [][]byte{ten, twenty}, batch.payloads)
			require.Zero(t, resumed.AppliedIndex())
			release()
			// Erasing every survivor does not erase append history or make it safe
			// to change this receiver to a different source generation.
			_, err = resumed.eventLog.managed.Rewrite(t.Context(), 8, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			require.NoError(t, err)
			release = resumed.BeginCatchUp()
			defer release()
			require.ErrorIs(t, resumed.initializeFetchedGeneration(t.Context(), 9), eventlog.ErrInvalid)
		})
	}
}

func TestInitializeFetchedGenerationUncertainPublication(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, after := range []bool{false, true} {
			phase := "before-publication"
			if after {
				phase = "lost-acknowledgment"
			}
			t.Run(name+"/"+phase, func(t *testing.T) {
				path := t.TempDir()
				opener := storageTestOpeners()[name]
				s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				failure := errors.New("generation publication interrupted")
				s.eventLog.managed = failedScrubPublication{EventLog: s.eventLog.managed, after: after, failure: failure}
				release := s.BeginCatchUp()
				require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 7), failure)
				release()
				require.NoError(t, s.Close())
				resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = resumed.Close() })
				observed := &observedScrubLog{EventLog: resumed.eventLog.managed}
				resumed.eventLog.managed = observed
				selected, err := observed.Generation()
				require.NoError(t, err)
				if after {
					require.Equal(t, uint64(7), selected)
				} else {
					require.Zero(t, selected)
				}
				release = resumed.BeginCatchUp()
				defer release()
				require.NoError(t, resumed.initializeFetchedGeneration(t.Context(), 7))
				if after {
					require.Empty(t, observed.generations)
				} else {
					require.Equal(t, []uint64{7}, observed.generations)
				}
				_, hasHistory, err := resumed.eventLog.entries.LastAppended()
				require.NoError(t, err)
				require.False(t, hasHistory)
				completed, err := resumed.loadScrubCompleted()
				require.NoError(t, err)
				require.Zero(t, completed)
			})
		}
	}
}

func TestInitializeFetchedGenerationRejectsNativeFormat(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil, WithSafeMode())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	release := s.BeginCatchUp()
	defer release()
	require.ErrorIs(t, s.initializeFetchedGeneration(t.Context(), 7), eventlog.ErrUnsupported)
	require.Zero(t, s.EventLogGeneration())
	require.NoError(t, s.appendFetchedEntries(0, [][]byte{experimentEntry(t, 10, pb.EntryNormal)}))
}
