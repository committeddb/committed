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

// TestRestoreSnapshot_ConcurrentReadersDoNotCrash exercises the kvMu
// serialization added alongside RestoreSnapshot. Dozens of goroutines
// hammer s.Type("events") while RestoreSnapshot swaps the bbolt
// handle underneath. Before kvMu was introduced, a read that happened
// to be mid-View when RestoreSnapshot called keyValueStorage.Close
// would hit a closed-file error and return "database not open". With
// the lock in place, in-flight reads drain before the close and new
// reads block until the reopen finishes — every Type() call must
// succeed end-to-end.
//
// The test runs N readers, calls RestoreSnapshot once in the main
// goroutine, and asserts every reader's observed errors are nil. Run
// with -race to additionally catch any torn-field access on the
// keyValueStorage pointer swap.
func TestRestoreSnapshot_ConcurrentReadersDoNotCrash(t *testing.T) {
	const readerCount = 32

	src := NewStorage(t, nil)
	defer src.Cleanup()

	// Seed a Type so readers have something to fetch.
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "events", Name: "Events", Version: 1})
	require.Nil(t, err)
	saveEntity(t, tp, src, 1, 1)

	// Capture a snapshot we can install. RestoreSnapshot requires the
	// target's EventIndex to be >= snap.Metadata.Index, so we snapshot
	// at index 1 and keep the same event-log state in dst.
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.Nil(t, err)

	dst := NewStorage(t, nil)
	defer dst.Cleanup()
	saveEntity(t, tp, dst, 1, 1)

	// stop tells readers to exit after the RestoreSnapshot completes.
	var stop atomic.Bool
	var wg sync.WaitGroup
	readErrs := make(chan error, readerCount*1024)

	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := dst.ResolveType(cluster.LatestTypeRef("events")); err != nil {
					readErrs <- err
					return
				}
			}
		}()
	}

	// Give readers a moment to ramp up before the restore kicks off.
	time.Sleep(10 * time.Millisecond)

	// Install the snapshot while readers hammer the bbolt tier. Under
	// the old no-lock implementation this is where a reader could
	// race with keyValueStorage.Close and come back with
	// "database not open".
	require.Nil(t, dst.RestoreSnapshot(snap))

	// Let readers run briefly against the reopened handle to confirm
	// reads still succeed post-restore (the reopened *bolt.DB is a
	// different pointer; a stale pointer cached in a reader would
	// show up here).
	time.Sleep(10 * time.Millisecond)

	stop.Store(true)
	wg.Wait()
	close(readErrs)

	for err := range readErrs {
		t.Fatalf("concurrent reader observed error: %v", err)
	}

	// Sanity: the type is still queryable through the normal API.
	got, err := dst.ResolveType(cluster.LatestTypeRef("events"))
	require.Nil(t, err)
	require.Equal(t, "events", got.ID)
}
