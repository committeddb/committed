package db_test

import (
	"testing"
	"time"
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
