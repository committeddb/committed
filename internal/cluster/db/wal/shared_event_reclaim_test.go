package wal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type failedReclaimLog struct {
	eventlog.EventLog
	after   bool
	failure error
}

func (l failedReclaimLog) Reclaim(ctx context.Context) (eventlog.ReclaimResult, error) {
	if !l.after {
		return eventlog.ReclaimResult{}, l.failure
	}
	result, err := l.EventLog.Reclaim(ctx)
	return result, errors.Join(err, l.failure)
}

func TestSharedReclamationResumesAfterReopen(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, after := range []bool{false, true} {
			phase := "before-removal"
			if after {
				phase = "after-removal"
			}
			t.Run(name+"/"+phase, func(t *testing.T) {
				path := t.TempDir()
				opener := storageTestOpeners()[name]
				open := func() *Storage {
					s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
					require.NoError(t, err)
					t.Cleanup(func() { _ = s.Close() })
					return s
				}
				s := open()
				require.NoError(t, s.eventLog.entries.Append([]eventlog.Record{
					{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("removed", "old"))},
					{ID: 20, Payload: experimentEntry(t, 20, pb.EntryNormal, experimentRow("kept", "new"))},
				}))
				s.eventIndex.Store(20)
				s.appliedIndex.Store(20)
				plan := &scrubPlan{bound: 20, selections: map[string]uint64{string(tombstoneKey("items", []byte("removed"))): 20}}
				published, err := s.rewriteSharedPlan(t.Context(), 7, plan)
				require.NoError(t, err)
				require.True(t, published.Published)
				failure := errors.New("reclamation interrupted")
				s.eventLog.managed = failedReclaimLog{EventLog: s.eventLog.managed, after: after, failure: failure}
				_, err = s.reclaimSharedGeneration(t.Context(), 7)
				require.ErrorIs(t, err, failure)
				require.Zero(t, s.lastScrubbedBound.Load())
				completed, err := s.loadScrubCompleted()
				require.NoError(t, err)
				require.Zero(t, completed)
				require.NoError(t, s.Close())

				s = open()
				generation, err := s.eventLog.managed.Generation()
				require.NoError(t, err)
				require.Equal(t, uint64(7), generation, "reopen must select the published rewrite")
				for _, stale := range []uint64{6, 8} {
					_, err = s.reclaimSharedGeneration(t.Context(), stale)
					require.ErrorIs(t, err, eventlog.ErrInvalid)
				}
				unfreeze := s.FreezeEventLayout()
				_, err = s.reclaimSharedGeneration(t.Context(), 7)
				require.ErrorIs(t, err, ErrLayoutFrozen)
				unfreeze()
				result, err := s.reclaimSharedGeneration(t.Context(), 7)
				require.NoError(t, err)
				if !after {
					require.Positive(t, result.RemovedFiles)
				}
				result, err = s.reclaimSharedGeneration(t.Context(), 7)
				require.NoError(t, err)
				require.Zero(t, result.RemovedFiles, "repeating reclamation is harmless")
				generation, err = s.eventLog.managed.Generation()
				require.NoError(t, err)
				require.Equal(t, uint64(7), generation, "resume must not publish a second rewrite")
				_, err = s.eventLog.managed.Read(10)
				require.ErrorIs(t, err, eventlog.ErrNotFound)
				_, err = s.eventLog.managed.Read(20)
				require.NoError(t, err)
				require.Zero(t, s.lastScrubbedBound.Load(), "reclamation does not perform completion bookkeeping")
			})
		}
	}
}
