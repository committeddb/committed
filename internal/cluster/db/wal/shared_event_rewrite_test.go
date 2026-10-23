package wal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterpb"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestStorageSharedRewritePublication(t *testing.T) {
	for name, create := range eventLogTestBackends() {
		t.Run(name, func(t *testing.T) {
			log, err := create(t.TempDir())
			require.NoError(t, err)
			defer func() { _ = log.Close() }()
			publishing := make(chan struct{})
			s := &Storage{
				eventLog:    bindEventLog(observedEventPublication{log, publishing}),
				eventLayout: layoutLock{logger: zap.NewNop()},
			}
			require.NoError(t, log.Append([]eventlog.Record{
				{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("removed", "old"))},
				{ID: 20, Payload: experimentEntry(t, 20, pb.EntryNormal, experimentRow("kept", "current"))},
				{ID: 30, Payload: experimentEntry(t, 30, pb.EntryConfChange)},
			}))
			s.eventIndex.Store(30)
			s.appliedIndex.Store(30)
			ref := cluster.TypeRef{ID: "items", Version: 1}
			typ, err := eventTestType(ref)
			require.NoError(t, err)
			s.typeCache.Store(ref, typeCacheEntry{t: typ})
			// A prepared selection removes the old subject row. Policy derivation
			// is covered by the scrub tests; this test exercises its publication.
			plan := &scrubPlan{bound: 30, selections: map[string]uint64{string(tombstoneKey("items", []byte("removed"))): 30}}
			endCatchUp := s.BeginCatchUp()
			_, err = s.rewriteSharedPlan(t.Context(), 1, plan)
			require.ErrorIs(t, err, errEventRewriteDeferred, "catch-up can start after plan preparation")
			endCatchUp()
			unpin := s.BeginFromZeroRead()
			_, err = s.rewriteSharedPlan(t.Context(), 1, plan)
			require.ErrorIs(t, err, errEventRewriteDeferred)
			unpin()
			unfreeze := s.FreezeEventLayout()
			_, err = s.rewriteSharedPlan(t.Context(), 1, plan)
			require.ErrorIs(t, err, ErrLayoutFrozen)
			unfreeze()

			r := &Reader{s: s}
			defer func() { _ = r.Close() }()
			// Retain a decoded entry as a reader does while awaiting application
			// visibility or retrying interpretation. Hold its read lifetime while
			// rewrite reaches the publication gate.
			s.eventMu.RLock()
			held := true
			defer func() {
				if held {
					s.eventMu.RUnlock()
				}
			}()
			cursor := r.eventCursorLocked()
			entry, err := cursor.Current()
			require.NoError(t, err)
			require.Equal(t, uint64(10), entry.GetIndex())
			done := make(chan error, 1)
			go func() {
				result, err := s.rewriteSharedPlan(t.Context(), 1, plan)
				if err == nil && !result.Published {
					err = eventlog.ErrCorrupt
				}
				done <- err
			}()
			select {
			case <-publishing:
			case <-time.After(5 * time.Second):
				t.Fatal("rewrite did not reach publication")
			}
			require.Zero(t, s.scrubGen.Load(), "publication must wait for active reads")
			s.eventMu.RUnlock()
			held = false
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("rewrite did not finish")
			}
			actual, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, uint64(20), actual.Index, "reader must discard its erased decoded entry")
			require.Equal(t, uint64(30), s.EventIndex())
			require.Zero(t, s.lastScrubbedBound.Load(), "publication alone is not scrub completion")
			reclaimed, err := log.Reclaim(t.Context())
			require.NoError(t, err)
			require.Positive(t, reclaimed.RemovedFiles, "publication must leave reclamation to its caller")
		})
	}
}

// A failed backend can become unusable before or during publication. A retained
// decoded entry must not bypass that failure on the next Reader.Read.
type failedPublicationLog struct {
	eventlog.EventLog
	failed      atomic.Bool
	publication bool
}

func (l *failedPublicationLog) RewriteSealedWithPublicationLock(_ context.Context, _ uint64, _ eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	if l.publication {
		lock.Lock()
		defer lock.Unlock()
	}
	l.failed.Store(true)
	return eventlog.RewriteResult{}, eventlog.ErrPoisoned
}

func (l *failedPublicationLog) NewCursor() eventlog.Cursor {
	return failedPublicationCursor{Cursor: l.EventLog.NewCursor(), failed: &l.failed}
}

type failedPublicationCursor struct {
	eventlog.Cursor
	failed *atomic.Bool
}

func (c failedPublicationCursor) Seek(id uint64) (eventlog.Record, error) {
	if c.failed.Load() {
		return eventlog.Record{}, eventlog.ErrPoisoned
	}
	return c.Cursor.Seek(id)
}

func TestStorageSharedRewriteFailureInvalidatesDecodedEntry(t *testing.T) {
	for _, publication := range []bool{false, true} {
		name := "preparation"
		if publication {
			name = "publication"
		}
		t.Run(name, func(t *testing.T) {
			log, err := eventLogTestBackends()["segmented"](t.TempDir())
			require.NoError(t, err)
			defer func() { _ = log.Close() }()
			require.NoError(t, log.Append([]eventlog.Record{{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))}}))
			backend := &failedPublicationLog{EventLog: log, publication: publication}
			s := &Storage{eventLog: bindEventLog(backend)}
			s.eventIndex.Store(10)
			s.appliedIndex.Store(10)
			r := &Reader{s: s}
			defer func() { _ = r.Close() }()
			s.eventMu.RLock()
			_, err = r.eventCursorLocked().Current()
			s.eventMu.RUnlock()
			require.NoError(t, err)
			_, err = s.rewriteSharedPlan(t.Context(), 1, &scrubPlan{bound: 10})
			require.ErrorIs(t, err, eventlog.ErrPoisoned)
			_, err = r.Read()
			require.ErrorIs(t, err, eventlog.ErrPoisoned)
			require.Zero(t, r.Position())
			require.Zero(t, s.lastScrubbedBound.Load())
		})
	}
}

func experimentPBEntry(t *testing.T, index uint64, entities ...*clusterpb.LogEntity) *pb.Entry {
	t.Helper()
	var entry pb.Entry
	require.NoError(t, proto.Unmarshal(experimentEntry(t, index, pb.EntryNormal, entities...), &entry))
	return &entry
}

// gatedRewriteLog pauses a scrub's rewrite at its first examined record.
type gatedRewriteLog struct {
	*segmented.Log
	entered, release chan struct{}
}

func (l gatedRewriteLog) gate(transform eventlog.Transform) eventlog.Transform {
	first := true
	return func(r eventlog.Record) ([]byte, bool, error) {
		if first {
			first = false
			close(l.entered)
			<-l.release
		}
		return transform(r)
	}
}

func (l gatedRewriteLog) RewriteSealedWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	return l.Log.RewriteSealedWithPublicationLock(ctx, generation, l.gate(transform), lock)
}

// A scrub's rewrite runs off the apply path: an event entry applied while the
// rewrite prepares is appended before it publishes, and survives it. Records
// at or below the bound that were still in the active tail are erased too.
func TestStorageSharedRewriteAllowsAppends(t *testing.T) {
	dir := t.TempDir()
	opts := segmentlog.LogOptions{SegmentBytes: 1 << 20, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}}
	log, err := segmented.Create(dir, 1, opts)
	require.NoError(t, err)
	defer func() { _ = log.Close() }()
	gated := gatedRewriteLog{Log: log, entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(gated.release) })
	defer release()
	s := &Storage{
		eventLog:    bindEventLog(gated),
		eventLayout: layoutLock{logger: zap.NewNop()},
	}
	require.NoError(t, s.appendEvents([]*pb.Entry{
		experimentPBEntry(t, 10, experimentRow("removed", "old")),
		experimentPBEntry(t, 20, experimentRow("kept", "current")),
	}))
	s.appliedIndex.Store(20)
	ref := cluster.TypeRef{ID: "items", Version: 1}
	typ, err := eventTestType(ref)
	require.NoError(t, err)
	s.typeCache.Store(ref, typeCacheEntry{t: typ})
	plan := &scrubPlan{bound: 20, selections: map[string]uint64{string(tombstoneKey("items", []byte("removed"))): 20}}

	done := make(chan error, 1)
	go func() {
		result, err := s.rewriteSharedPlan(t.Context(), 20, plan)
		if err == nil && !result.Published {
			err = eventlog.ErrCorrupt
		}
		done <- err
	}()
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rewrite did not start preparing")
	}
	appended := make(chan error, 1)
	go func() {
		appended <- s.appendEvents([]*pb.Entry{experimentPBEntry(t, 30, experimentRow("removed", "new"))})
	}()
	select {
	case err := <-appended:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		release()
		<-done
		t.Fatal("an applied event waited for the scrub's rewrite")
	}
	release()
	require.NoError(t, <-done)
	require.Equal(t, uint64(30), s.EventIndex())

	ids := func() []uint64 {
		var ids []uint64
		require.NoError(t, log.Scan(t.Context(), eventlog.Coverage{End: ^uint64(0)}, func(r eventlog.Record) error {
			ids = append(ids, r.ID)
			return nil
		}))
		return ids
	}
	require.Equal(t, []uint64{20, 30}, ids(), "the tail's erased record is gone; the later write survives")
	generation, err := log.Generation()
	require.NoError(t, err)
	require.Equal(t, uint64(20), generation)
	require.NoError(t, log.Close())
	log, err = segmented.Open(dir, opts.Encoding)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 30}, ids())
	require.NoError(t, log.Append([]eventlog.Record{{ID: 40, Payload: experimentEntry(t, 40, pb.EntryNormal, experimentRow("later", "x"))}}))
}
