package db

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// countingCloseIngestable / countingCloseSyncable count Close() calls so the
// close-once test can prove concurrent teardown paths don't double-close.
type countingCloseIngestable struct{ closes atomic.Int32 }

func (c *countingCloseIngestable) Ingest(context.Context, cluster.Position, chan<- *cluster.Proposal, chan<- cluster.Position) error {
	return nil
}
func (c *countingCloseIngestable) Close() error { c.closes.Add(1); return nil }
func (c *countingCloseIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

type countingCloseSyncable struct{ closes atomic.Int32 }

func (c *countingCloseSyncable) Init(context.Context) error { return nil }
func (c *countingCloseSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (c *countingCloseSyncable) Close() error { c.closes.Add(1); return nil }

// TestWorkerLifecycle_CloseResourcesRunsCloseExactlyOnce pins the
// worker-handle-lifecycle-races F2 fix: the drain-then-close helpers route the
// resource Close through the handle's sync.Once, so however many teardown paths
// (delete, reconcile, db.Close, replace) reach a drained handle concurrently,
// Close runs exactly once. The Close contract is not concurrency-safe (a binlog
// syncer is unsafe to Close twice); before the Once, db.Close racing a
// listener-path teardown on one handle double-closed it.
//
// This is an internal (package db) test so it can build a handle with a
// pre-drained done channel and call the helpers directly — the race is the
// concurrent close of ONE handle, isolated from the worker machinery.
func TestWorkerLifecycle_CloseResourcesRunsCloseExactlyOnce(t *testing.T) {
	const goroutines = 16

	t.Run("ingestable", func(t *testing.T) {
		d := &DB{logger: zap.NewNop(), workerDrainTimeout: time.Second}
		done := make(chan struct{})
		close(done) // drained: the close path is eligible to run
		ing := &countingCloseIngestable{}
		h := &workerHandle{done: done, ingestable: ing}

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.closeDrainedIngestable(h, "id")
			}()
		}
		wg.Wait()

		require.Equal(t, int32(1), ing.closes.Load(),
			"ingestable Close must run exactly once across racing teardown paths")
	})

	t.Run("syncable", func(t *testing.T) {
		d := &DB{logger: zap.NewNop(), workerDrainTimeout: time.Second}
		done := make(chan struct{})
		close(done)
		syn := &countingCloseSyncable{}
		h := &workerHandle{done: done, syncable: syn}

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.closeDrainedSyncable(h, "id")
			}()
		}
		wg.Wait()

		require.Equal(t, int32(1), syn.closes.Load(),
			"syncable Close must run exactly once across racing teardown paths")
	})
}
