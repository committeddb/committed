package db_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
)

// TestIngest_FreezesOnErrProposalUnknown verifies that when db.Propose
// returns ErrProposalUnknown in the ingest worker, the worker DOES NOT
// drain positionChan (which would advance the persisted
// IngestablePosition past a row whose entry may never have committed)
// and DOES exit cleanly when its ctx is canceled. This is the
// correctness guarantee that restores pre-fail-fast-ticket behavior —
// before the ticket, db.Propose hung indefinitely on a leader flap,
// which froze the whole ingest pipeline and left the persisted
// position pinned so replay-on-restart was idempotent.
//
// The test synthesizes ErrProposalUnknown by injecting it directly via
// SignalWaiterForTest once the ingest worker's waiter is registered.
// That bypasses the timing race between raft apply and the
// leader-change watcher; the path under test is the ingest worker's
// handling of the error, not the watcher itself (covered in
// propose_fail_fast_test.go).
func TestIngest_FreezesOnErrProposalUnknown(t *testing.T) {
	id := "freeze-ingest"

	d, s := newIngestFailFastDB(t)

	// Wait for the single-node leader to stabilize so any SetLeaderID
	// calls in this test don't race the Ready loop's own updates.
	// (Not strictly required for this test since we bypass the
	// watcher, but keeps the test environment in a known state.)
	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	// A blocking ingestable: emits one proposal, then records every
	// attempt to send on positionChan. If the worker freezes as
	// designed, the position send will NEVER succeed — the inner
	// Ingest goroutine stays parked on the unbuffered send until ctx
	// cancellation. On freeze, that's fine.
	proposal := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "string"},
			Key:  []byte("freeze-key"),
			Data: []byte("freeze-value"),
		}},
	}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos-1")))

	require.NoError(t, d.Ingest(context.Background(), id, ing))

	// Wait for the ingest worker to submit its proposal to db.Propose
	// (which registers a waiter in db.waiters). The exact RequestID
	// is auto-assigned; we just grab whichever waiter we see.
	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never registered a Propose waiter")

	// Inject ErrProposalUnknown directly on the ack channel. This is
	// what the watcher's grace goroutine would have done after an
	// observed leader transition; we shortcut the timing path here.
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	// Assert that positionChan is NOT drained. If the worker had
	// log-and-continued (the pre-fix regression shape), it would
	// advance on positionChan and record a position send. We give it
	// a generous window so scheduler noise can't mask a misbehaving
	// worker.
	require.Never(t,
		func() bool { return ing.PositionSendCount() > 0 },
		200*time.Millisecond, 10*time.Millisecond,
		"ingest worker advanced past ErrProposalUnknown — regression",
	)

	// Assert that the IngestablePosition entity likewise never lands
	// in the storage's log. Even if the inner Ingest had managed some
	// other path to emit a position, the fire-and-forget proposal
	// would still have to succeed. Absence of it in the log confirms
	// no persisted position advance.
	require.False(t, s.HasIngestablePositionEntity(id),
		"IngestablePosition entity appeared in log — regression",
	)

	// Unblock apply so db.Close's internal serveChannels drain can
	// complete — the raft Ready loop is parked inside
	// slowApplyStorage.ApplyCommitted waiting on `release`, and Close
	// waits on serveChannels before returning. Without this Unblock,
	// the subsequent d.Close would deadlock.
	s.Unblock()

	// Close the DB. The worker has already returned ingestExitFreeze
	// (the freeze branch no longer parks); its handle.done is closed
	// and the supervisor is effectively disabled for this test via the
	// hour-long InitialBackoff set in newIngestFailFastDB. Close
	// cancels db.ctx, which unblocks the supervisor's backoff select,
	// and then drains the registry. If the worker were leaked or
	// wedged some other way, Close would hang and the test would time
	// out.
	require.NoError(t, d.Close())
}

// newIngestFailFastDB constructs a single-node DB backed by a
// slow-apply wrapper around MemoryStorage. The wrapper stalls each
// ApplyCommitted call until the test explicitly unblocks it; this
// keeps each Propose's waiter registered in db.waiters for long
// enough that the test can poll-and-find it and inject an
// ErrProposalUnknown signal without racing apply. Without this, the
// waiter lifetime (register → Propose send to proposeC → raft apply
// → notifyApplied signal → Propose defer delete) is on the order of
// microseconds — too short for a 500μs poll cadence to reliably
// observe.
func newIngestFailFastDB(t *testing.T) (*db.DB, *slowApplyStorage) {
	t.Helper()
	// Effectively disable the supervisor restart path for the base
	// freeze test so its assertions aren't racing an automatic restart.
	// Tests that exercise the supervisor (ingest_supervisor_test.go)
	// call newIngestFailFastDBWith with tight backoffs to drive it.
	return newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(1*time.Hour),
	)
}

// newIngestFailFastDBWith is newIngestFailFastDB but lets the caller
// layer additional options (supervisor backoff, max attempts, etc.) on
// top of the common test setup. Keeps both the base freeze test and
// the supervisor tests sharing the same storage/leader plumbing.
func newIngestFailFastDBWith(t *testing.T, extra ...db.Option) (*db.DB, *slowApplyStorage) {
	t.Helper()
	id := uint64(1)
	peers := make(db.Peers)
	peers[id] = ""
	p := parser.New()
	inner := NewMemoryStorage()
	inner.SetNode(id)
	s := newSlowApplyStorage(inner)

	opts := []db.Option{
		db.WithTickInterval(testTickInterval),
		db.WithLeaderChangeGracePeriod(50 * time.Millisecond),
	}
	opts = append(opts, extra...)

	d := db.New(id, peers, s, p, nil, nil, opts...)
	// Note: not deferring Close — the test drives Close explicitly as
	// part of the assertion that the frozen worker exits cleanly.
	return d, s
}

// slowApplyStorage wraps MemoryStorage so ApplyCommitted blocks until
// blockApply is released. Lets the test pin the waiter lifetime long
// enough to inject ErrProposalUnknown without racing the raft Ready
// loop's natural apply path.
type slowApplyStorage struct {
	*MemoryStorage
	// applyBlocked is 1 while apply should park; flipped to 0 via
	// Unblock to let queued Ready iterations drain on teardown.
	applyBlocked atomic.Int32
	release      chan struct{}
}

func newSlowApplyStorage(inner *MemoryStorage) *slowApplyStorage {
	s := &slowApplyStorage{
		MemoryStorage: inner,
		release:       make(chan struct{}),
	}
	s.applyBlocked.Store(1)
	return s
}

// ApplyCommitted blocks until the test closes `release`, after which
// all subsequent ApplyCommitted calls fall through immediately.
//
// We only block EntryNormal entries with non-nil Data — i.e., actual
// user proposals. Skipping conf-change entries (which would otherwise
// prevent single-node election from ever completing — raft needs
// ApplyConfChange to land the voter set before it campaigns) and
// empty-Data entries (the leader-election no-op). The test's only
// user-data proposal is the one emitted by freezeRecordingIngestable,
// so this scopes the stall exactly to that proposal's apply path.
func (s *slowApplyStorage) ApplyCommitted(e raftpb.Entry) error {
	if s.applyBlocked.Load() == 1 && e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
		<-s.release
	}
	return s.MemoryStorage.ApplyCommitted(e)
}

// Unblock lets queued ApplyCommitted calls proceed. Idempotent — safe
// for t.Cleanup to call even if the test already unblocked inline.
func (s *slowApplyStorage) Unblock() {
	if s.applyBlocked.CompareAndSwap(1, 0) {
		close(s.release)
	}
}

// freezeRecordingIngestable is a minimal cluster.Ingestable that emits
// exactly one proposal and then records each attempted position send.
// We use a dedicated helper (rather than MemoryIngestable) so the
// behavior is sharply scoped to this test: a single proposal followed
// by one position, with explicit visibility into how many times the
// inner Ingest managed to send on positionChan.
//
// Counters are guarded by posMu — the inner Ingest goroutine runs
// concurrently with the test's assertions. ingestCalls tracks how
// often cluster.Ingestable.Ingest has been invoked; the supervisor
// tests use it to detect a restart (Ingest invoked a second time on
// the same instance).
type freezeRecordingIngestable struct {
	proposal *cluster.Proposal
	position cluster.Position

	posMu        sync.Mutex
	posSent      int
	ingestCalls  int
}

func newFreezeRecordingIngestable(p *cluster.Proposal, pos cluster.Position) *freezeRecordingIngestable {
	return &freezeRecordingIngestable{proposal: p, position: pos}
}

func (f *freezeRecordingIngestable) Init(context.Context) error { return nil }
func (f *freezeRecordingIngestable) Close() error               { return nil }

func (f *freezeRecordingIngestable) Ingest(
	ctx context.Context,
	_ cluster.Position,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	f.posMu.Lock()
	f.ingestCalls++
	f.posMu.Unlock()

	// Emit the single proposal. Blocks until the ingest worker
	// reads it (or ctx cancels). Either way, after this returns
	// control we've finished our one Propose attempt.
	select {
	case pr <- f.proposal:
	case <-ctx.Done():
		return nil
	}

	// Now try to emit the position. In the freeze path the worker
	// has parked on <-ctx.Done() and is NOT reading positionChan, so
	// this send never completes until ctx cancels. That's the
	// intended shape: every still-pending upstream event stays stuck
	// behind the unbuffered send, ensuring nothing drains past the
	// failed proposal.
	//
	// If the regression is present (worker log-and-continues), this
	// send succeeds and we bump posSent — which the test detects via
	// require.Never.
	select {
	case po <- f.position:
		f.posMu.Lock()
		f.posSent++
		f.posMu.Unlock()
	case <-ctx.Done():
		return nil
	}

	<-ctx.Done()
	return nil
}

func (f *freezeRecordingIngestable) PositionSendCount() int {
	f.posMu.Lock()
	defer f.posMu.Unlock()
	return f.posSent
}

func (f *freezeRecordingIngestable) IngestCalls() int {
	f.posMu.Lock()
	defer f.posMu.Unlock()
	return f.ingestCalls
}

// HasIngestablePositionEntity scans the storage's entry log for any
// Entity whose Type is the system IngestablePosition type. The test
// only needs "did any ingestable-position entity land in the log" —
// a stricter "did an entity for this specific id land" check would
// require decoding the protobuf payload (cluster.IngestablePosition
// is protobuf, not JSON), but since the test harness only ever runs
// one ingestable at a time that distinction doesn't matter here.
func (ms *MemoryStorage) HasIngestablePositionEntity(_ string) bool {
	fi, err := ms.FirstIndex()
	if err != nil {
		return false
	}
	li, err := ms.LastIndex()
	if err != nil {
		return false
	}
	if li+1 <= fi {
		return false
	}
	ents, err := ms.Entries(fi, li+1, 1_000_000)
	if err != nil {
		return false
	}
	for _, e := range ents {
		if e.Data == nil {
			continue
		}
		p := &cluster.Proposal{}
		if err := p.Unmarshal(e.Data, ms); err != nil {
			continue
		}
		for _, ent := range p.Entities {
			if ent.Type == nil {
				continue
			}
			if cluster.IsIngestablePosition(ent.Type.ID) {
				return true
			}
		}
	}
	return false
}
