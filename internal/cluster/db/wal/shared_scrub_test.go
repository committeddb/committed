package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type observedScrubLog struct {
	eventlog.EventLog
	generations []uint64
}

func (l *observedScrubLog) RewriteWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	l.generations = append(l.generations, generation)
	return l.EventLog.RewriteWithPublicationLock(ctx, generation, transform, lock)
}

// failedScrubPublication models both outcomes hidden behind an unsuccessful
// publication call. After publication it deliberately discards Published=true:
// only reopening and reading the selected generation resolves the uncertainty.
// Backend-specific crash tests cover the physical publication protocol itself.
type failedScrubPublication struct {
	eventlog.EventLog
	after   bool
	failure error
}

func (l failedScrubPublication) RewriteWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	if l.after {
		if _, err := l.EventLog.RewriteWithPublicationLock(ctx, generation, transform, lock); err != nil {
			return eventlog.RewriteResult{}, err
		}
	}
	return eventlog.RewriteResult{}, l.failure
}

func TestSharedScrubLifecycleRecovery(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, interruption := range []string{"before-publication", "after-publication", "before-removal", "after-removal"} {
			for _, newer := range []bool{false, true} {
				for _, hash := range []bool{false, true} {
					phase := interruption
					if newer {
						phase += "/newer-pending"
					}
					if hash {
						phase += "/erase-keys"
					} else {
						phase += "/retain-keys"
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
						applyRaw := func(raw []byte) {
							entry := new(pb.Entry)
							require.NoError(t, proto.Unmarshal(raw, entry))
							require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: entry.Index}, []*pb.Entry{entry}, &pb.Snapshot{}))
							require.NoError(t, s.ApplyCommittedBatch([]*pb.Entry{entry}))
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
						command, err := cluster.NewScrubEntity(3, hash)
						require.NoError(t, err)
						applyEntity(5, command)
						release := s.BeginFromZeroRead()
						require.ErrorIs(t, s.runPendingScrub(), errEventRewriteDeferred)
						release()
						unfreeze := s.FreezeEventLayout()
						require.ErrorIs(t, s.runPendingScrub(), ErrLayoutFrozen)
						unfreeze()
						failure := errors.New("scrub interrupted")
						switch interruption {
						case "before-publication", "after-publication":
							s.eventLog.managed = failedScrubPublication{EventLog: s.eventLog.managed, after: interruption == "after-publication", failure: failure}
						default:
							s.eventLog.managed = failedReclaimLog{EventLog: s.eventLog.managed, after: interruption == "after-removal", failure: failure}
						}
						require.ErrorIs(t, s.runPendingScrub(), failure)
						require.Zero(t, s.lastScrubbedBound.Load())
						completed, err := s.loadScrubCompleted()
						require.NoError(t, err)
						require.Zero(t, completed)
						require.NoError(t, s.Close())
						s = open() // No operations on a failed backend before reopen.
						selected, err := s.eventLog.managed.Generation()
						require.NoError(t, err)
						if interruption == "before-publication" {
							require.Zero(t, selected)
							_, err := s.eventLog.managed.Read(2)
							require.NoError(t, err, "unpublished rewrite leaves the old row selected")
						} else {
							require.Equal(t, uint64(3), selected)
							_, err := s.eventLog.managed.Read(2)
							require.ErrorIs(t, err, eventlog.ErrNotFound)
						}
						require.Equal(t, 1, s.PendingDeleteKeyErasures(), "failed scrub must leave cadence bookkeeping unfinished")
						if newer {
							// Supersede the request before recovery finishes it. The old
							// hash authorization is no longer in the pending record.
							command, err = cluster.NewScrubEntity(5, false)
							require.NoError(t, err)
							applyEntity(6, command)
						}
						require.NoError(t, s.Close())
						s = open()
						observer := &observedScrubLog{EventLog: s.eventLog.managed}
						s.eventLog.managed = observer
						require.NoError(t, s.runPendingScrub())
						want := uint64(3)
						if newer {
							want = 5
							require.Equal(t, []uint64{5}, observer.generations, "finish the old publication without rewriting it")
						} else if interruption == "before-publication" {
							require.Equal(t, []uint64{3}, observer.generations, "unpublished request still needs its rewrite")
						} else {
							require.Empty(t, observer.generations, "recovery must not rerun the transform")
						}
						require.Equal(t, want, s.lastScrubbedBound.Load())
						completed, err = s.loadScrubCompleted()
						require.NoError(t, err)
						require.Equal(t, want, completed)
						_, err = s.eventLog.managed.Read(2)
						require.ErrorIs(t, err, eventlog.ErrNotFound)
						_, err = s.eventLog.managed.Read(4)
						require.NoError(t, err)
						deleted, err := s.eventLog.managed.Read(3)
						require.NoError(t, err)
						entry := new(pb.Entry)
						require.NoError(t, proto.Unmarshal(deleted.Payload, entry))
						// A newer non-erasing request replaces an unpublished
						// request, but cannot undo an already-published erasure.
						erased := hash && (!newer || interruption != "before-publication")
						require.NoError(t, cluster.ForEachProposalEntity(entry.Data, func(_ string, key, _ []byte, isDelete bool) error {
							require.True(t, isDelete)
							require.Equal(t, erased, cluster.IsErasedKey(key))
							return nil
						}))
						require.NoError(t, s.view(func(tx *bolt.Tx) error {
							var key [8]byte
							binary.BigEndian.PutUint64(key[:], 3)
							k, _ := tx.Bucket(unhashedDeleteBucket).Cursor().Seek(key[:])
							if erased {
								require.NotEqual(t, key[:], k, "erased delete must leave cadence backlog")
							} else {
								require.Equal(t, key[:], k, "retained raw delete must remain in cadence backlog")
							}
							return nil
						}))
						require.NoError(t, s.runPendingScrub(), "completed request is idempotent")
					})
				}
			}
		}
	}
}

func TestSharedScrubPublishedBeforeAppliedProgressSaved(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			stageSharedScrubCommand(t, s)
			// applyCommitted exposes the in-memory watermark before the enclosing
			// batch saves it. Publication can finish in this crash window.
			s.appliedIndex.Store(2)
			plan, err := s.prepareScrubPlan(1, false, 2)
			require.NoError(t, err)
			result, err := s.rewriteSharedPlan(t.Context(), 1, plan)
			require.NoError(t, err)
			require.True(t, result.Published)
			require.NoError(t, s.Close())

			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = resumed.Close() })
			require.Equal(t, uint64(1), resumed.AppliedIndex())
			observer := &observedScrubLog{EventLog: resumed.eventLog.managed}
			resumed.eventLog.managed = observer
			require.ErrorIs(t, resumed.runPendingScrub(), errScrubApplyPending,
				"durable scrub history ahead of the saved apply watermark is retryable")
			require.Zero(t, resumed.lastScrubbedBound.Load())
			// A missing history row is different from a saved watermark
			// lagging a durable row: never turn missing authorization into
			// permission to complete, or an endless admission retry.
			require.NoError(t, resumed.update(func(tx *bolt.Tx) error {
				var key [8]byte
				binary.BigEndian.PutUint64(key[:], 2)
				return tx.Bucket(scrubHistoryBucket).Delete(key[:])
			}))
			require.ErrorIs(t, resumed.runPendingScrub(), eventlog.ErrInvalid)
			require.Zero(t, resumed.lastScrubbedBound.Load())
			require.NoError(t, resumed.update(func(tx *bolt.Tx) error {
				return recordScrubHistory(tx, 2, 1)
			}))
			record, err := resumed.eventLog.managed.Read(2)
			require.NoError(t, err)
			command := new(pb.Entry)
			require.NoError(t, proto.Unmarshal(record.Payload, command))
			require.NoError(t, resumed.ApplyCommittedBatch([]*pb.Entry{command}))
			require.NoError(t, resumed.runPendingScrub())
			require.Equal(t, uint64(1), resumed.lastScrubbedBound.Load())
			require.Empty(t, observer.generations, "replay must finish the existing publication")
		})
	}
}
