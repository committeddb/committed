package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestDeleteSync_BoundsWedgedWorkerDrain is the apply-loop-config-channel-send-hang
// regression (the steady-state half). listenForSyncables calls deleteSync (and
// the Sync replace path) synchronously and used to wait on the worker's done
// channel with no bound. A worker wedged in tx.Commit against an unreachable
// destination never closes done, so the single-threaded listener parked there,
// stopped draining the config channel, and the raft apply loop then blocked on
// its next config send — stalling apply of ALL further committed entries (user
// data included). The handoff must abandon a wedged worker after
// workerDrainTimeout so the listener keeps up and the apply loop never stalls
// indefinitely.
func TestDeleteSync_BoundsWedgedWorkerDrain(t *testing.T) {
	d := createDB()
	defer d.Close()

	d.SetWorkerDrainTimeoutForTest(100 * time.Millisecond)
	d.InjectWedgedSyncWorkerForTest("wedged") // its done channel never closes

	done := make(chan struct{})
	go func() {
		d.DeleteSyncForTest("wedged")
		close(done)
	}()

	select {
	case <-done:
		// Returned via the drain timeout, not the never-closing worker done.
	case <-time.After(5 * time.Second):
		t.Fatal("deleteSync blocked on a wedged worker's unbounded drain — the raft apply loop would stall")
	}
}

// blockingDestinationSyncable models a syncable whose destination has gone
// unreachable AFTER its worker drained cleanly: Sync is never called, but
// Close/Teardown — which talk to the destination — block until the destination
// recovers (never, in these tests).
type blockingDestinationSyncable struct {
	block         chan struct{} // never closed
	blockTeardown bool
	blockClose    bool
}

func (b *blockingDestinationSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}

func (b *blockingDestinationSyncable) Close() error {
	if b.blockClose {
		<-b.block
	}
	return nil
}

func (b *blockingDestinationSyncable) Teardown() error {
	if b.blockTeardown {
		<-b.block
	}
	return nil
}

// waitForLeadership blocks until the single-node db believes it is leader —
// the owner gate (isNode) a delete's teardown resolves through.
func waitForLeadership(t *testing.T, d *DB) {
	t.Helper()
	require.Eventually(t, d.IsLeaderForTest, 5*time.Second, 5*time.Millisecond,
		"single-node db never became leader")
}

// TestDeleteSync_BoundsWedgedTeardown is the sibling of the drain-bound test
// above, for the leg the drain fix left open: after a delete's (bounded) drain,
// deleteSync calls the syncable's destination Teardown on the SAME
// single-threaded listener goroutine — against the same destination that may be
// unreachable. Unbounded, a hung DROP parked the listener and the raft apply
// loop stalled on its next config-channel send. deleteSync must abandon the
// teardown after workerDrainTimeout (logging the orphaned-destination error)
// and return.
func TestDeleteSync_BoundsWedgedTeardown(t *testing.T) {
	d := createDB()
	defer d.Close()
	waitForLeadership(t, d)

	d.SetWorkerDrainTimeoutForTest(100 * time.Millisecond)
	s := &blockingDestinationSyncable{block: make(chan struct{}), blockTeardown: true}
	d.InjectDrainedSyncWorkerForTest("wedged-teardown", s)

	done := make(chan struct{})
	go func() {
		d.DeleteSyncForTest("wedged-teardown")
		close(done)
	}()

	select {
	case <-done:
		// Returned via the teardown bound, not the never-unblocking DROP.
	case <-time.After(5 * time.Second):
		t.Fatal("deleteSync blocked on an unbounded destination Teardown — the raft apply loop would stall")
	}
}

// TestDeleteSync_BoundsWedgedClose covers the third leg on the listener path:
// Close writes statement-close packets to the destination and can block on a
// dead network. closeDrainedSyncable must abandon it after workerDrainTimeout.
func TestDeleteSync_BoundsWedgedClose(t *testing.T) {
	d := createDB()
	defer d.Close()
	waitForLeadership(t, d)

	d.SetWorkerDrainTimeoutForTest(100 * time.Millisecond)
	s := &blockingDestinationSyncable{block: make(chan struct{}), blockClose: true}
	d.InjectDrainedSyncWorkerForTest("wedged-close", s)

	done := make(chan struct{})
	go func() {
		d.DeleteSyncForTest("wedged-close")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deleteSync blocked on an unbounded syncable Close — the raft apply loop would stall")
	}
}
