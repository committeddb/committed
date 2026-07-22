package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestWorkerLifecycle_CancelCondemnsAgainstSupervisorResurrection pins the
// worker-handle-lifecycle-races F1 fix: cancelling a FROZEN ingest worker must
// condemn its handle under the first lock hold so a pending supervisor restart,
// firing inside the cancel's drain window, refuses to resurrect it. Before the
// fix the supervisor's `ingestWorkers[id] != frozen` preflight passed during
// that window (the map entry is deleted only after the relock), so it installed
// a fresh worker on the SAME Ingestable instance the cancel then Closed —
// use-after-Close on a live source plus a zombie worker for a deleted config.
//
// The race is made deterministic with three rendezvous seams (not backoff
// timing, so it holds under -race and load): the supervisor is held at a poise
// point just before its lock reacquire; the cancel's drain-window seam releases
// it only once the handle is condemned, then waits for the supervisor's attempt
// to finish before the cancel relocks and deletes. That forces the supervisor's
// preflight to land after condemn, inside the window — the exact interleave.
func TestWorkerLifecycle_CancelCondemnsAgainstSupervisorResurrection(t *testing.T) {
	const id = "condemn-race"

	// Short backoff so the supervisor promptly reaches its poise seam; the
	// rendezvous channels — not the backoff duration — sequence the race.
	d, s := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(1*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(1*time.Millisecond),
	)
	require.Eventually(t, func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond)

	proposal := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("v"),
	}}}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos")))

	require.NoError(t, d.Ingest(context.Background(), id, ing))
	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never registered a Propose waiter")

	// Rendezvous channels. poised closes when the supervisor is about to
	// reacquire workersMu; the test releases it (proceed) only once cancel is
	// parked in its drain window with the handle condemned; attemptDone closes
	// when the supervisor's restart goroutine exits.
	poised := make(chan struct{})
	proceed := make(chan struct{})
	attemptDone := make(chan struct{})
	d.SetIngestSupervisorRaceSeamsForTest(
		func() { close(poised); <-proceed },
		func() { close(attemptDone) },
	)
	d.SetBeforeCancelIngestRelockForTest(func() {
		<-poised       // supervisor has woken and is about to lock
		close(proceed) // let it run its preflight now, inside our window
		<-attemptDone  // and finish (bail) before we relock and delete
	})

	// Freeze the worker, then wait until its supervisor has actually spawned and
	// reached the poise seam (poised closed) BEFORE cancelling. This ordering is
	// load-bearing: the worker spawns a supervisor only if it returns
	// ingestExitFreeze, and that branch is gated on ctx.Err()==nil. If we
	// cancelled first, cancel's handle.cancel() would race the worker's freeze
	// decision and could flip it to ingestExitShutdown — no supervisor spawns, and
	// the rendezvous below then deadlocks forever on <-poised. Gating cancel on
	// "supervisor poised" makes the freeze strictly happen-before the cancel, so
	// the race can't occur; the bounded wait turns a missed freeze into a fast,
	// legible failure instead of a 15-minute hang.
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)
	select {
	case <-poised:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor never poised — the worker did not freeze (ingestExitShutdown?)")
	}

	// Cancel the frozen worker (the shared delete/reconcile front half).
	d.CancelIngestWorkerForTest(id)

	// The supervisor observed frozen.condemned and bailed: no restart, no zombie.
	require.Equal(t, 1, ing.IngestCalls(),
		"supervisor resurrected a condemned frozen worker — Ingest was re-invoked")
	require.False(t, d.HasIngestWorkerForTest(id),
		"a condemned worker's slot must be empty after cancel; a lingering handle means resurrection won")
	require.Equal(t, int32(1), ing.CloseCalls(),
		"the frozen ingestable must be Closed exactly once")

	s.Unblock()
	require.NoError(t, d.Close())
}
