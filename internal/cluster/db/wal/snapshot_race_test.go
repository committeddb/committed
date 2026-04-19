package wal_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TestSnapshot_ConcurrentCreateAndRead races CreateSnapshot (the
// snapshot writer, called from raft's serveChannels via maybeCompact)
// against Snapshot (the reader, called from etcd raft's node.run
// goroutine via raftLog.snapshot when the leader builds an
// InstallSnapshot for a lagging follower). Before snapMu was added,
// the two paths touched s.snapshot from different goroutines without
// synchronization — the race surfaced as
// `--- FAIL: TestAdversarial_SevereLagFollowerRebuild` under -race
// because that scenario is the only one that combines wal.Storage,
// aggressive compaction (writers fire often), and a lagging follower
// (reader fires often).
//
// Run with -race; the assertion is "no race detected" rather than a
// value check, so this test passes trivially without -race.
func TestSnapshot_ConcurrentCreateAndRead(t *testing.T) {
	const (
		readerCount = 16
		writerCount = 4
		runDuration = 200 * time.Millisecond
	)

	s := NewStorage(t, nil)
	defer s.Cleanup()

	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, s, 1, 1)

	var stop atomic.Bool
	var wg sync.WaitGroup

	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_, _ = s.Snapshot()
			}
		}()
	}

	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs := &pb.ConfState{Voters: []uint64{1}}
			for !stop.Load() {
				_, _ = s.CreateSnapshot(s.AppliedIndex(), cs)
			}
		}()
	}

	time.Sleep(runDuration)
	stop.Store(true)
	wg.Wait()
}

// TestSnapshot_ConcurrentConfStateAndRead races ConfState (called from
// raft's serveChannels after each EntryConfChange apply) against
// Snapshot. ConfState mutates s.snapshot.Metadata.ConfState in place;
// without snapMu the read in Snapshot could observe a torn ConfState
// during the assignment. Same -race-only assertion shape as the test
// above.
func TestSnapshot_ConcurrentConfStateAndRead(t *testing.T) {
	const (
		readerCount = 16
		runDuration = 200 * time.Millisecond
	)

	s := NewStorage(t, nil)
	defer s.Cleanup()

	var stop atomic.Bool
	var wg sync.WaitGroup

	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_, _ = s.Snapshot()
				_, _, _ = s.InitialState()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		voters := []uint64{1, 2, 3, 4, 5}
		for !stop.Load() {
			s.ConfState(&pb.ConfState{Voters: voters})
		}
	}()

	time.Sleep(runDuration)
	stop.Store(true)
	wg.Wait()
}
