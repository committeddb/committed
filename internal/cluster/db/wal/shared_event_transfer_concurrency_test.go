package wal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

type pausedTransferCursor struct {
	eventlog.Cursor
	calls int
	pause func()
}

func (c *pausedTransferCursor) Seek(index uint64) (eventlog.Record, error) {
	c.calls++
	if c.calls == 2 {
		c.pause()
	}
	return c.Cursor.Seek(index)
}

// Pause after the first record is collected, without holding a backend lock.
func pauseTransferBatch(s *Storage) (<-chan struct{}, func()) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var pause, release sync.Once
	factory := s.eventLog.records
	s.eventLog.records = func() eventlog.Cursor {
		return &pausedTransferCursor{Cursor: factory(), pause: func() {
			pause.Do(func() { close(entered); <-resume })
		}}
	}
	return entered, func() { release.Do(func() { close(resume) }) }
}

func TestLogicalTransferCapturesAppendFrontier(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			payloads := [][]byte{experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 20, pb.EntryNormal)}
			unpin := s.BeginCatchUp()
			defer unpin()
			require.NoError(t, s.appendFetchedEntries(0, payloads))
			entered, resume := pauseTransferBatch(s)
			defer resume()
			type outcome struct {
				batch fetchedEntryBatch
				err   error
			}
			done := make(chan outcome, 1)
			go func() { batch, err := s.fetchEntries(t.Context(), 0, 100, 10, 1024); done <- outcome{batch, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not reach pause")
			}
			appended := make(chan error, 1)
			thirty := experimentEntry(t, 30, pb.EntryNormal)
			go func() { appended <- s.appendFetchedEntries(0, [][]byte{thirty}) }()
			select {
			case err := <-appended:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("batch unnecessarily blocked append")
			}
			resume()
			select {
			case result := <-done:
				require.NoError(t, result.err)
				require.Equal(t, uint64(20), result.batch.frontier)
				require.Equal(t, uint64(20), result.batch.after)
				require.True(t, result.batch.done)
				require.Equal(t, payloads, result.batch.payloads)
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not finish")
			}
			next, err := s.fetchEntries(t.Context(), 20, 100, 10, 1024)
			require.NoError(t, err)
			require.Equal(t, uint64(30), next.frontier)
			require.Equal(t, [][]byte{thirty}, next.payloads)
		})
	}
}

func TestLogicalTransferExcludesRewritePublication(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			payloads := [][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("removed", "old")), experimentEntry(t, 20, pb.EntryNormal, experimentRow("kept", "new"))}
			release := s.BeginCatchUp()
			require.NoError(t, s.appendFetchedEntries(0, payloads))
			release()
			s.appliedIndex.Store(20)
			publishing := make(chan struct{})
			s.eventLog.managed = observedEventPublication{EventLog: s.eventLog.managed, entered: publishing}
			entered, resume := pauseTransferBatch(s)
			defer resume()
			type outcome struct {
				batch fetchedEntryBatch
				err   error
			}
			fetched := make(chan outcome, 1)
			go func() { batch, err := s.fetchEntries(t.Context(), 0, 100, 10, 1024); fetched <- outcome{batch, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not reach pause")
			}
			rewritten := make(chan error, 1)
			go func() {
				_, err := s.rewriteSharedPlan(t.Context(), 7, &scrubPlan{bound: 20, selections: map[string]uint64{string(tombstoneKey("items", []byte("removed"))): 20}})
				rewritten <- err
			}()
			select {
			case <-publishing:
			case <-time.After(5 * time.Second):
				t.Fatal("rewrite did not reach publication")
			}
			require.Zero(t, s.scrubGen.Load(), "publication must wait for the entire batch")
			resume()
			var batch fetchedEntryBatch
			select {
			case result := <-fetched:
				require.NoError(t, result.err)
				batch = result.batch
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not finish")
			}
			select {
			case err := <-rewritten:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("publication did not resume")
			}
			_, err = s.reclaimSharedGeneration(t.Context(), 7)
			require.NoError(t, err)
			require.Equal(t, uint64(0), batch.generation)
			require.Equal(t, payloads, batch.payloads, "returned bytes survive publication and retirement")
			next, err := s.fetchEntries(t.Context(), 0, 100, 10, 1024)
			require.NoError(t, err)
			require.Equal(t, uint64(7), next.generation)
			require.Equal(t, payloads[1:], next.payloads)
		})
	}
}

func TestLogicalReceiveSerializesConcurrentRetries(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			s, err := openStorage(t.TempDir(), nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			release := s.BeginCatchUp()
			defer release()
			observer := &productionAppendObserver{Appender: s.eventLog.entries}
			s.eventLog.entries = &observedEntryStore{entryStore: s.eventLog.entries, appender: observer}
			payloads := [][]byte{experimentEntry(t, 10, pb.EntryNormal), experimentEntry(t, 30, pb.EntryNormal)}
			const attempts = 16
			start := make(chan struct{})
			done := make(chan error, attempts)
			for range attempts {
				go func() { <-start; done <- s.appendFetchedEntries(0, payloads) }()
			}
			close(start)
			for range attempts {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent receive did not finish")
				}
			}
			require.Equal(t, 1, observer.calls)
			require.Len(t, observer.records, 2)
			require.Equal(t, uint64(30), s.EventIndex())
			require.Equal(t, uint64(10), s.firstEventIndex.Load())
			require.Equal(t, int64(1), s.eventLogWriteOps.Load())
		})
	}
}
