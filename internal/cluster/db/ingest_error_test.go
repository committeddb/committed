package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
)

// SignalingIngestable runs forever (until ctx cancellation), signaling
// when Ingest is called and again when it exits.
type SignalingIngestable struct {
	started chan struct{}
	stopped chan struct{}
}

func NewSignalingIngestable() *SignalingIngestable {
	return &SignalingIngestable{
		started: make(chan struct{}, 1),
		stopped: make(chan struct{}, 1),
	}
}

func (i *SignalingIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	i.started <- struct{}{}
	defer func() { i.stopped <- struct{}{} }()
	<-ctx.Done()
	return nil
}

func (i *SignalingIngestable) Close() error {
	return nil
}

// TestIngest_CloseTerminatesWorker verifies that db.Close cancels the
// inner ingest goroutine and waits for it to exit cleanly. After PR3
// the caller-provided ctx no longer drives worker lifetime — workers
// inherit from db.ctx so they outlive per-request handlers — so the
// only legitimate way for tests to terminate a worker is db.Close (or
// a replace via a second db.Ingest call for the same ID, which
// TestIngest_RegistryReplace covers).
func TestIngest_CloseTerminatesWorker(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)

	ingestable := NewSignalingIngestable()
	err := db.Ingest(context.Background(), "ingest-cancel", ingestable)
	require.Nil(t, err)

	// Wait for the inner Ingest goroutine to start
	select {
	case <-ingestable.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest never started")
	}

	// Closing the db must cancel the worker context AND wait for the
	// goroutine to actually exit (registry drain in db.Close).
	require.Nil(t, db.Close())

	// The inner Ingest should already have exited by the time Close
	// returned, but read from the channel to assert that. (The send
	// happened-before db.Close returned thanks to the worker's
	// defer close(handle.done) → registry drain.)
	select {
	case <-ingestable.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest did not stop after db.Close")
	}
}

// TestIngest_RegistryReplace verifies that calling db.Ingest twice for
// the same ID cancels the first worker before starting the second.
// After PR3 the registry guarantees one worker per ID — duplicate
// configures replace, not stack.
func TestIngest_RegistryReplace(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	first := NewSignalingIngestable()
	require.Nil(t, db.Ingest(context.Background(), "id-1", first))

	// Wait for the first worker's inner Ingest to start so we know the
	// goroutine is actually parked on ctx.Done(), not still spinning up.
	select {
	case <-first.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first ingest never started")
	}

	// Replace it. db.Ingest must cancel and drain the first worker
	// before installing the second; by the time it returns, the first
	// inner Ingest should have observed ctx.Done() and signaled stopped.
	second := NewSignalingIngestable()
	require.Nil(t, db.Ingest(context.Background(), "id-1", second))

	select {
	case <-first.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("first ingest did not stop after replace")
	}

	// And the second worker should now be the active one.
	select {
	case <-second.started:
	case <-time.After(2 * time.Second):
		t.Fatal("second ingest never started")
	}
}

// TestIngest_ConcurrentReplaceNoOrphans is a regression test for the
// concurrent-replace race the registry loop in db.Ingest fixes. The
// pre-fix shape ("if existing { cancel; wait; install }") loses
// workers when multiple callers race on the same id: each observes
// the same original entry, each waits on it, each then unconditionally
// installs — and only the last write to the map survives, while the
// goroutines spawned by the losing callers run unreferenced until
// db.Close.
//
// The fix loops the cancel-wait check, re-validating the map slot
// after each wait. Every spawned worker is then either (a) the final
// registry winner, (b) canceled before its inner Ingest ever started
// (so SignalingIngestable.Ingest was never called), or (c) canceled
// after starting and properly drained (started AND stopped both fire).
//
// The orphan signature is "started fired but stopped didn't" — the
// inner Ingest is parked on ctx.Done() but no one canceled it. With
// the fix, exactly ONE ingestable should match this signature (the
// winner, which has not yet been canceled by anyone). Without the
// fix, two or more would match because the racing callers all install
// successfully and orphan each other. Run with -race to also catch
// any map mutation races.
func TestIngest_ConcurrentReplaceNoOrphans(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	const n = 10
	ingestables := make([]*SignalingIngestable, n)
	for i := range ingestables {
		ingestables[i] = NewSignalingIngestable()
	}

	// Fire all replace calls concurrently. Use a start barrier so they
	// race as tightly as possible — the bug only manifests when two
	// callers both observe the same "existing" snapshot before either
	// has finished its install. The WaitGroup tells us when every
	// db.Ingest call has returned (i.e., every install cycle is done).
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(n)
	for i := 0; i < n; i++ {
		ing := ingestables[i]
		go func() {
			defer done.Done()
			<-start
			require.Nil(t, db.Ingest(context.Background(), "race-id", ing))
		}()
	}
	close(start)
	done.Wait()

	// The registry winner's inner Ingest must have actually started by
	// the time we count, otherwise we'd flake when started hasn't yet
	// fired but the winner is queued on the leader-state transition.
	// Wait for any one ingestable to start (a started signal exists),
	// then poll briefly for the steady state to settle.
	deadline := time.Now().Add(2 * time.Second)
	var winnerStarted bool
	for time.Now().Before(deadline) {
		for _, ing := range ingestables {
			select {
			case <-ing.started:
				// Restore the signal so the loop below can re-observe it.
				// (started is buffered cap 1, so the send succeeds.)
				ing.started <- struct{}{}
				winnerStarted = true
			default:
			}
			if winnerStarted {
				break
			}
		}
		if winnerStarted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, winnerStarted, "no ingestable ever started — the winner's worker never reached the leader-state transition")

	// Now classify each ingestable:
	//   - never started: canceled before its inner Ingest spawned. Not an orphan.
	//   - started AND stopped: canceled after starting but properly drained. Not an orphan.
	//   - started AND NOT stopped: still running. This must be exactly 1 (the winner).
	stillRunning := 0
	for _, ing := range ingestables {
		var started bool
		select {
		case <-ing.started:
			started = true
		default:
		}
		if !started {
			continue
		}
		select {
		case <-ing.stopped:
			// Started then stopped — proper cancellation, not an orphan.
		case <-time.After(100 * time.Millisecond):
			stillRunning++
		}
	}
	require.Equal(t, 1, stillRunning,
		"expected exactly one ingestable to still be running (the registry winner); more than one indicates orphan workers from the replace race")
}

// TestIngest_CloseDrainsAllWorkers verifies that db.Close cancels and
// waits for every registered ingest worker, not just one.
func TestIngest_CloseDrainsAllWorkers(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)

	ingestables := []*SignalingIngestable{
		NewSignalingIngestable(),
		NewSignalingIngestable(),
		NewSignalingIngestable(),
	}
	for i, ing := range ingestables {
		require.Nil(t, db.Ingest(context.Background(), fmt.Sprintf("id-%d", i), ing))
	}

	// Wait for every inner Ingest to start so we're sure each worker
	// is parked on ctx.Done() before we close.
	for i, ing := range ingestables {
		select {
		case <-ing.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("ingest %d never started", i)
		}
	}

	require.Nil(t, db.Close())

	// All inner Ingests must have observed ctx.Done() before Close
	// returned (db.Close drains the worker registry before returning).
	for i, ing := range ingestables {
		select {
		case <-ing.stopped:
		case <-time.After(2 * time.Second):
			t.Fatalf("ingest %d did not stop after Close", i)
		}
	}
}

// TestIngest_LeaderChangeStopsIngestGoroutine verifies that when the node
// transitions from leader to non-leader, the inner ingest goroutine is
// cancelled (via the cancel() call at ingest.go:56).
func TestIngest_LeaderChangeStopsIngestGoroutine(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// db.New now calls EatCommitC automatically.

	// Start as leader (default node ID matches db ID)
	s.SetNode(db.ID())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingestable := NewSignalingIngestable()
	err := db.Ingest(ctx, "ingest-leader", ingestable)
	require.Nil(t, err)

	// Wait for ingest goroutine to start (because we are leader)
	select {
	case <-ingestable.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest never started while leader")
	}

	// Now transition to not-leader
	s.SetNode(uint64(0xdeadbeef))

	// Wait for ingest goroutine to stop (cancelled by ingest.go state machine)
	select {
	case <-ingestable.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("ingest did not stop after losing leadership")
	}
}
