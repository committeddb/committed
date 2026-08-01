package db_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// faithfulBlockApplyStorage is like slowApplyStorage (it blocks user-proposal
// apply until Unblock) but reports a REAL applied/event index that advances as
// entries apply. slowApplyStorage stubs AppliedIndex to 0, so the freeze-exit
// apply-drain — which waits for AppliedIndex to reach the commit index — could
// never complete against it. Here AppliedIndex trails the commit index while
// apply is blocked and catches up once Unblock releases it, which is exactly the
// apply-lag the drain must wait out. EventIndex mirrors AppliedIndex so the Ready
// loop's P_local==R_local invariant holds.
type faithfulBlockApplyStorage struct {
	*MemoryStorage
	applyBlocked atomic.Int32
	release      chan struct{}
	applied      atomic.Uint64
}

func newFaithfulBlockApplyStorage(inner *MemoryStorage) *faithfulBlockApplyStorage {
	s := &faithfulBlockApplyStorage{MemoryStorage: inner, release: make(chan struct{})}
	s.applyBlocked.Store(1)
	return s
}

// ApplyCommitted blocks user proposals until Unblock, then records the real
// applied index. Conf-change and no-op entries pass through so single-node
// election can complete.
func (s *faithfulBlockApplyStorage) ApplyCommitted(e *raftpb.Entry) error {
	if s.applyBlocked.Load() == 1 && e.GetType() == raftpb.EntryNormal && len(e.Data) > 0 {
		<-s.release
	}
	if err := s.MemoryStorage.ApplyCommitted(e); err != nil {
		return err
	}
	for {
		cur := s.applied.Load()
		if e.GetIndex() <= cur || s.applied.CompareAndSwap(cur, e.GetIndex()) {
			break
		}
	}
	return nil
}

func (s *faithfulBlockApplyStorage) ApplyCommittedBatch(entries []*raftpb.Entry) error {
	for _, e := range entries {
		if err := s.ApplyCommitted(e); err != nil {
			return err
		}
	}
	return nil
}

func (s *faithfulBlockApplyStorage) AppliedIndex() uint64 { return s.applied.Load() }
func (s *faithfulBlockApplyStorage) EventIndex() uint64   { return s.applied.Load() }

func (s *faithfulBlockApplyStorage) Unblock() {
	if s.applyBlocked.CompareAndSwap(1, 0) {
		close(s.release)
	}
}

// TestIngest_FreezeDrainsApplyBeforeRestart proves the freeze-exit apply-drain
// (spawnIngestWorkerLocked): under apply-lag (commit ahead of applied), a frozen
// ingest worker must NOT be restarted from its durable position until apply
// catches up. Restarting on a stale position would re-read behind committed-but-
// unapplied batches and re-emit them into the permanent event log — the
// effectively-once violation this fix closes. With apply blocked the restart is
// gated (Ingest is not invoked a second time); once apply is unblocked and
// AppliedIndex reaches the commit index, the drain completes and the restart
// proceeds. Neutralizing the drain makes the restart fire while apply is still
// behind — the red proof.
func TestIngest_FreezeDrainsApplyBeforeRestart(t *testing.T) {
	id := "drain-before-restart"

	peers := make(db.Peers)
	peers[1] = ""
	inner := NewMemoryStorage()
	inner.SetNode(1)
	s := newFaithfulBlockApplyStorage(inner)

	// Drain timeout left at 0 (unbounded — the production path): this storage's
	// AppliedIndex is faithful, so the drain completes when apply catches up, not
	// by a timeout. That is the behavior under test.
	d := db.New(1, peers, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithLeaderChangeGracePeriod(50*time.Millisecond),
		db.WithIngestSupervisorInitialBackoff(1*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(5*time.Millisecond),
	)
	t.Cleanup(func() { s.Unblock(); _ = d.Close() })

	require.Eventually(t, func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond)

	proposal := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("v"),
	}}}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos")))
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	require.Eventually(t, func() bool { return ing.IngestCalls() >= 1 },
		2*time.Second, 5*time.Millisecond, "initial ingest never ran")

	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never registered a Propose waiter")

	// Inject the freeze. The proposal committed but its apply is blocked, so
	// commit > applied: the drain must hold the restart until apply catches up.
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	// While apply stays blocked, the supervisor must NOT restart the worker —
	// the freeze-exit drain gates it on apply catch-up.
	require.Never(t, func() bool { return ing.IngestCalls() >= 2 },
		500*time.Millisecond, 25*time.Millisecond,
		"worker restarted before apply caught up — the freeze-exit drain is not gating the restart")

	// Let apply catch up: AppliedIndex reaches the commit index, the drain
	// completes, and only now may the supervisor restart the worker.
	s.Unblock()
	require.Eventually(t, func() bool { return ing.IngestCalls() >= 2 },
		5*time.Second, 10*time.Millisecond,
		"supervisor never restarted after apply caught up")
}
