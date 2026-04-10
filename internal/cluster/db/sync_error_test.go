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

// ErrorSyncable returns errors from Sync but tracks call counts.
// shouldSnapshot controls whether successful syncs trigger a SyncableIndex proposal.
// Setting shouldSnapshot=false avoids races during cleanup since no extra proposals
// are emitted by the sync goroutine.
//
// All mutable state is guarded by mu because Sync runs on the DB's sync
// goroutine while tests inspect count from the test goroutine.
type ErrorSyncable struct {
	syncErr        error
	maxBeforeStop  int
	cancel         func()
	shouldSnapshot cluster.ShouldSnapshot

	mu            sync.Mutex
	count         int
	receivedProps []*cluster.Proposal
}

// Count returns the number of times Sync has been called.
func (s *ErrorSyncable) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *ErrorSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	s.mu.Lock()
	s.count++
	s.receivedProps = append(s.receivedProps, p)
	count := s.count
	s.mu.Unlock()

	if count >= s.maxBeforeStop && s.cancel != nil {
		s.cancel()
	}
	if s.syncErr != nil {
		return false, s.syncErr
	}
	return s.shouldSnapshot, nil
}

func (s *ErrorSyncable) Close() error {
	return nil
}

// TestSync_TransientError_Retries verifies that when Syncable.Sync returns a
// transient (non-permanent) error, the sync loop retries the same proposal
// with backoff instead of advancing past it.
func TestSync_TransientError_Retries(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Propose 1 proposal
	ps := createProposals([][]string{{"a"}})
	proposeCtx := testCtx(t)
	require.Nil(t, db.Propose(proposeCtx, ps[0]))

	// Syncable that always returns a transient error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &ErrorSyncable{
		syncErr:       fmt.Errorf("simulated transient failure"),
		maxBeforeStop: 3,
		cancel:        cancel,
	}

	err := db.Sync(ctx, "sync-1", syncable)
	require.Nil(t, err)

	<-ctx.Done()

	// All 3 calls should have been on the same proposal (retries)
	require.Equal(t, 3, syncable.Count(), "sync loop should retry transient errors")
	syncable.mu.Lock()
	for i, p := range syncable.receivedProps {
		require.Equal(t, string(ps[0].Entities[0].Data), string(p.Entities[0].Data),
			"call %d should receive the same proposal", i)
	}
	syncable.mu.Unlock()
}

// TestSync_PermanentError_Skips verifies that when Syncable.Sync returns a
// permanent error, the sync loop skips past the bad proposal and continues
// processing the next one.
func TestSync_PermanentError_Skips(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Propose 2 proposals
	ps := createProposals([][]string{{"bad"}, {"good"}})
	proposeCtx := testCtx(t)
	for _, p := range ps {
		require.Nil(t, db.Propose(proposeCtx, p))
	}

	// Syncable that returns permanent error on first call, succeeds on second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &ScriptedSyncable{
		responses: []syncResponse{
			{err: cluster.Permanent(fmt.Errorf("bad data"))},
			{snapshot: false, err: nil},
		},
		cancel: cancel,
	}

	err := db.Sync(ctx, "sync-1", syncable)
	require.Nil(t, err)

	<-ctx.Done()

	// Both proposals should have been seen: first skipped, second processed
	syncable.mu.Lock()
	require.Equal(t, 2, len(syncable.receivedProps))
	require.Equal(t, "bad", string(syncable.receivedProps[0].Entities[0].Data))
	require.Equal(t, "good", string(syncable.receivedProps[1].Entities[0].Data))
	syncable.mu.Unlock()
}

// TestSync_RegistryReplace verifies that calling db.Sync twice for the
// same ID cancels the first worker before starting the second. The
// blockingSyncable parks inside Sync on ctx.Done() so we can observe
// the worker actually being canceled by the registry replace path.
func TestSync_RegistryReplace(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Be the leader so the sync workers' state machine enters its read
	// loop (db.sync only calls s.Sync if db.isNode(id) is true).
	s.SetNode(db.ID())

	// Propose one entry so the worker has something to read and call
	// Sync on. Without a proposal, db.sync just spins on EOF and never
	// invokes the syncable, so we can't observe Sync entering.
	require.Nil(t, db.Propose(testCtx(t), createProposals([][]string{{"x"}})[0]))

	first := newBlockingSyncable()
	require.Nil(t, db.Sync(context.Background(), "sync-replace", first))

	// Wait for the first worker to actually call Sync (i.e., it's
	// parked inside the syncable on ctx.Done()).
	select {
	case <-first.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first syncable.Sync never called")
	}

	// Replace. db.Sync's registry-aware path cancels the first
	// worker's ctx and waits for the worker (and thus the inner
	// blocked Sync) to return before installing the second.
	second := newBlockingSyncable()
	require.Nil(t, db.Sync(context.Background(), "sync-replace", second))

	// First syncable should have observed ctx.Done() and exited Sync.
	select {
	case <-first.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("first syncable.Sync did not exit after replace")
	}

	// Second worker should now be the active one. It re-reads the
	// proposal (the persisted index isn't bumped because the first
	// Sync was canceled before it could call shouldSnapshot's
	// proposeSyncableIndex). That's the documented replace semantics:
	// idempotent Sync re-applies any in-flight proposal.
	select {
	case <-second.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("second syncable.Sync never called")
	}
}

// TestSync_ConcurrentReplaceNoOrphans is the Sync-side mirror of
// TestIngest_ConcurrentReplaceNoOrphans. It exercises the same
// concurrent-replace race the registry loop in db.Sync fixes: without
// the loop, two callers can both observe the same "existing" snapshot,
// both wait on it, then both install — orphaning every loser's worker
// until db.Close.
//
// Detection signature: a syncable that "entered" Sync but hasn't
// "exited" is currently parked on ctx.Done() — still running. The
// fix guarantees that the steady-state count of such syncables is
// exactly 1 (the registry winner). Without the fix, the count is
// > 1: every loser's worker also enters Sync (its own goroutine
// runs because the orphaned handle's ctx is never canceled by any
// later caller) and stays parked until db.Close.
//
// We wait for steady state before counting. Without that wait, we
// race against the workers' state-machine spin-up: most losers DO
// briefly enter Sync (entered fires) but then exit almost immediately
// (the canceler that replaced them already canceled their ctx, so
// `<-ctx.Done()` returns instantly). We need to give the LAST
// installed worker (the winner) time to enter Sync AND for every
// loser whose worker entered Sync to also exit. 500ms is enough on
// every timing profile we've tested; it's well under the 2s test
// timeout for any of these tests.
//
// Run with -race to also catch any map mutation races.
func TestSync_ConcurrentReplaceNoOrphans(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// db.sync only invokes s.Sync if db.isNode(id) is true. Default
	// MemoryStorage state already returns true for db.ID() but be
	// explicit so the test doesn't depend on that default.
	s.SetNode(db.ID())

	// Propose one entry so every worker that reaches its read loop
	// has a proposal to invoke Sync on. Without a proposal, db.sync
	// just spins on EOF and never enters the syncable.
	require.Nil(t, db.Propose(testCtx(t), createProposals([][]string{{"x"}})[0]))

	const n = 10
	syncs := make([]*blockingSyncable, n)
	for i := range syncs {
		syncs[i] = newBlockingSyncable()
	}

	// Fire all replace calls concurrently from a start barrier so
	// they race as tightly as possible. The bug only manifests when
	// two callers both observe the same "existing" snapshot before
	// either has finished its install.
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(n)
	for i := 0; i < n; i++ {
		sy := syncs[i]
		go func() {
			defer done.Done()
			<-start
			require.Nil(t, db.Sync(context.Background(), "race-id", sy))
		}()
	}
	close(start)
	done.Wait()

	// Wait for steady state. Every spawned worker has either:
	//   - exited before entering Sync (worker canceled before reaching read), OR
	//   - entered Sync and then exited (worker canceled while parked), OR
	//   - entered Sync and is still parked (the registry winner).
	// 500ms is far longer than the worker's spin-up time on any
	// profile we've measured.
	time.Sleep(500 * time.Millisecond)

	// Count "still running" using non-destructive peeks. The fix
	// guarantees exactly 1; the bug would leave multiple orphans.
	stillRunning := 0
	for _, sy := range syncs {
		if sy.hasEntered() && !sy.hasExited() {
			stillRunning++
		}
	}
	require.Equal(t, 1, stillRunning,
		"expected exactly one syncable to still be running (the registry winner); more than one indicates orphan workers from the replace race")
}

// TestSync_CloseDrainsAllWorkers verifies db.Close cancels and waits
// for every registered sync worker before returning. Each worker is
// parked inside its syncable on ctx.Done(); after Close, every one
// should have exited.
func TestSync_CloseDrainsAllWorkers(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	s.SetNode(db.ID())

	// One proposal per ID so each worker has something to invoke Sync
	// with. We need a separate proposal per id because the sync worker
	// uses a Reader keyed by id and the MemoryStorage Reader doesn't
	// share entries across ids.
	for i := 0; i < 3; i++ {
		require.Nil(t, db.Propose(testCtx(t), createProposals([][]string{{fmt.Sprintf("p%d", i)}})[0]))
	}

	syncs := []*blockingSyncable{
		newBlockingSyncable(),
		newBlockingSyncable(),
		newBlockingSyncable(),
	}
	for i, sync := range syncs {
		require.Nil(t, db.Sync(context.Background(), fmt.Sprintf("sync-close-%d", i), sync))
	}

	// Wait for every worker to be parked inside Sync.
	for i, sync := range syncs {
		select {
		case <-sync.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("sync %d never entered Sync", i)
		}
	}

	require.Nil(t, db.Close())

	// All workers must have observed ctx.Done() before Close returned.
	for i, sync := range syncs {
		select {
		case <-sync.exited:
		case <-time.After(2 * time.Second):
			t.Fatalf("sync %d did not exit after Close", i)
		}
	}
}

// blockingSyncable parks the sync worker inside Sync on ctx.Done(),
// which lets tests observe the worker being canceled by the registry
// replace or Close drain.
//
// `entered` and `exited` are CLOSED (not sent on) to signal state, so
// tests can:
//   - block on them with `<-sy.entered` (closed channel returns immediately)
//   - check them non-destructively with a select-default peek
//     (closed channels can be selected on repeatedly without draining)
//
// Sync.Once protects the closes so a syncable that runs through Sync
// multiple times (which shouldn't happen in current tests, but is
// defensive) doesn't panic on double-close.
type blockingSyncable struct {
	enteredOnce sync.Once
	exitedOnce  sync.Once
	entered     chan struct{}
	exited      chan struct{}
}

func newBlockingSyncable() *blockingSyncable {
	return &blockingSyncable{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
}

func (s *blockingSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-ctx.Done()
	s.exitedOnce.Do(func() { close(s.exited) })
	return false, nil
}

func (s *blockingSyncable) Close() error {
	return nil
}

// hasEntered / hasExited do a non-destructive peek at the close state.
// Used by the orphan test to count "still running" workers without
// draining the signaling channels.
func (s *blockingSyncable) hasEntered() bool {
	select {
	case <-s.entered:
		return true
	default:
		return false
	}
}

func (s *blockingSyncable) hasExited() bool {
	select {
	case <-s.exited:
		return true
	default:
		return false
	}
}

// TestSync_EOF_Continues verifies that when the Reader returns io.EOF
// (no more entries to read), the sync loop continues without crashing.
// New proposals added later should still be picked up.
//
// shouldSnapshot is set to false to avoid the sync goroutine emitting a
// SyncableIndex proposal during cleanup (which would race with db.Close()).
func TestSync_EOF_Continues(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// db.New now calls EatCommitC automatically.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Syncable that does NOT request a snapshot, so no SyncableIndex proposal
	// is emitted by the sync goroutine. This prevents races during cleanup.
	syncable := &ErrorSyncable{maxBeforeStop: 1, cancel: cancel, shouldSnapshot: false}
	err := db.Sync(ctx, "sync-eof", syncable)
	require.Nil(t, err)

	// Brief moment to let sync loop spin on EOF
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 0, syncable.Count())

	// Now add a proposal - sync should pick it up despite earlier EOFs
	p := createProposals([][]string{{"after-eof"}})[0]
	require.Nil(t, db.Propose(testCtx(t), p))

	// Wait for sync to process the proposal (which fires cancel)
	<-ctx.Done()
	require.Equal(t, 1, syncable.Count(), "sync should resume after EOF when new data arrives")
}

// syncResponse is a single scripted response for ScriptedSyncable.
type syncResponse struct {
	snapshot cluster.ShouldSnapshot
	err      error
}

// ScriptedSyncable returns pre-scripted responses for each Sync call.
// After all scripted responses are exhausted, it cancels the context.
type ScriptedSyncable struct {
	responses []syncResponse
	cancel    func()

	mu            sync.Mutex
	count         int
	receivedProps []*cluster.Proposal
}

func (s *ScriptedSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	s.mu.Lock()
	idx := s.count
	s.count++
	s.receivedProps = append(s.receivedProps, p)
	s.mu.Unlock()

	if idx >= len(s.responses) {
		if s.cancel != nil {
			s.cancel()
		}
		return false, nil
	}

	resp := s.responses[idx]
	if idx == len(s.responses)-1 && s.cancel != nil {
		s.cancel()
	}
	return resp.snapshot, resp.err
}

func (s *ScriptedSyncable) Close() error {
	return nil
}
