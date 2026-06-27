package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// transientSyncable fails any user proposal whose payload is in `stuck` with
// a transient (non-permanent) error — so the worker retries it forever — and
// succeeds on anything else, recording the payload. It is a single-proposal
// syncable (no SyncBatch), so it drives the syncSingle path.
type transientSyncable struct {
	stuck        map[string]bool
	transientErr error

	mu        sync.Mutex
	synced    []string
	stuckHits int
}

func (s *transientSyncable) Sync(_ context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	payload := string(p.Entities[0].Data)
	if s.stuck[payload] {
		s.mu.Lock()
		s.stuckHits++
		s.mu.Unlock()
		return cluster.ShouldSnapshot(false), s.transientErr
	}
	s.mu.Lock()
	s.synced = append(s.synced, payload)
	s.mu.Unlock()
	return cluster.ShouldSnapshot(true), nil
}

func (s *transientSyncable) Close() error { return nil }

func (s *transientSyncable) syncedPayloads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.synced))
	copy(out, s.synced)
	return out
}

// sawStuck reports how many times Sync was invoked for a stuck payload.
func (s *transientSyncable) sawStuck() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stuckHits
}

// newWalDBStuck is newWalDB with a short stuck-debounce so tests don't wait
// the 30s production threshold for a wedged worker to publish its replicated
// SyncableStuck record.
func newWalDBStuck(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithSyncStuckThreshold(50*time.Millisecond))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

// TestDeadLetterStuckSyncable_NotStuck asserts the request is rejected when
// the syncable isn't currently blocked — there is nothing to skip.
func TestDeadLetterStuckSyncable_NotStuck(t *testing.T) {
	d, _ := newWalDB(t)

	_, err := d.DeadLetterStuckSyncable(context.Background(), "never-registered")
	require.ErrorIs(t, err, cluster.ErrSyncNotStuck)
}

// TestDeadLetterStuckSyncable_SingleProposalSkipped is the headline single-
// proposal criterion: a syncable wedged retrying a transient error is
// unstuck by an operator request, which dead-letters the wedged proposal as
// "manual" and lets the worker advance to the next one.
func TestDeadLetterStuckSyncable_SingleProposalSkipped(t *testing.T) {
	d, s := newWalDBStuck(t)
	id := "stuck-single"
	seedUserProposals(t, d, s, "evt", []string{"poison", "ok"})

	syncable := &transientSyncable{
		stuck:        map[string]bool{"poison": true},
		transientErr: fmt.Errorf("downstream rejected the row"),
	}
	require.NoError(t, d.Sync(context.Background(), id, syncable))

	// The worker publishes its stuck status (replicated) after the debounce;
	// once visible, ask any node to skip what it's stuck on.
	var stuckIndex uint64
	require.Eventually(t, func() bool {
		idx, err := d.DeadLetterStuckSyncable(context.Background(), id)
		if err == nil {
			stuckIndex = idx
		}
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "worker should publish that it's blocked on the poison proposal")
	require.Greater(t, stuckIndex, uint64(0))

	// The skip is recorded as a manual dead letter for the wedged proposal.
	var dls []cluster.SyncableDeadLetter
	require.Eventually(t, func() bool {
		dls, _ = d.SyncableDeadLetters(id, 0, 10)
		return len(dls) == 1
	}, 5*time.Second, 5*time.Millisecond, "the operator skip must record one manual dead letter")
	require.Equal(t, "manual", dls[0].Kind)
	require.Equal(t, stuckIndex, dls[0].Index)
	require.Contains(t, dls[0].Message, "downstream rejected the row",
		"the dead letter carries the last transient error")

	// Having skipped the poison, the worker advances and syncs the next one.
	require.Eventually(t, func() bool {
		for _, p := range syncable.syncedPayloads() {
			if p == "ok" {
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond, "worker must advance past the skipped proposal")
}

// TestSyncableStuck_PublishedAfterDebounce asserts the replicated status that
// backs GET /syncable/{id}/status and the stuck gauge: a wedged worker
// publishes its blocked index and last error (after the debounce), readable
// from storage on any node.
func TestSyncableStuck_PublishedAfterDebounce(t *testing.T) {
	d, s := newWalDBStuck(t)
	id := "stuck-status"
	seedUserProposals(t, d, s, "evt", []string{"poison"})

	syncable := &transientSyncable{
		stuck:        map[string]bool{"poison": true},
		transientErr: fmt.Errorf("downstream rejected the row"),
	}
	require.NoError(t, d.Sync(context.Background(), id, syncable))

	var stuck cluster.SyncableStuck
	require.Eventually(t, func() bool {
		var ok bool
		stuck, ok, _ = d.SyncableStuck(id)
		return ok
	}, 10*time.Second, 10*time.Millisecond, "worker should publish a replicated stuck record")
	require.Greater(t, stuck.Index, uint64(0))
	require.Greater(t, stuck.SinceUnixNano, int64(0))
	require.Contains(t, stuck.Message, "downstream rejected the row")
}

// batchTransientSyncable is a BatchSyncable: a batch containing the poison
// payload fails atomically (transient), and per-proposal Sync (used by the
// worker's isolation fallback) fails only on the poison and succeeds on
// everything else. This drives the syncBatch path and the operator-requested
// isolation.
type batchTransientSyncable struct {
	poison       string
	transientErr error

	mu      sync.Mutex
	batches int
	solo    []string
}

func (s *batchTransientSyncable) SyncBatch(_ context.Context, ps []*cluster.Actual) (bool, error) {
	s.mu.Lock()
	s.batches++
	s.mu.Unlock()
	for _, p := range ps {
		if allSystem(p) {
			continue
		}
		if string(p.Entities[0].Data) == s.poison {
			return false, s.transientErr
		}
	}
	return true, nil
}

func (s *batchTransientSyncable) Sync(_ context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	payload := string(p.Entities[0].Data)
	if payload == s.poison {
		return cluster.ShouldSnapshot(false), s.transientErr
	}
	s.mu.Lock()
	s.solo = append(s.solo, payload)
	s.mu.Unlock()
	return cluster.ShouldSnapshot(true), nil
}

func (s *batchTransientSyncable) Close() error { return nil }

func (s *batchTransientSyncable) soloSynced() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.solo))
	copy(out, s.solo)
	return out
}

// TestDeadLetterStuckSyncable_BatchIsolation asserts that for a batch
// syncable, an operator skip isolates the wedged batch per-proposal: only the
// proposal that actually fails is dead-lettered ("manual"), and the healthy
// proposals in the same batch still go through.
func TestDeadLetterStuckSyncable_BatchIsolation(t *testing.T) {
	d, s := newWalDBStuck(t)
	id := "stuck-batch"
	seedUserProposals(t, d, s, "evt", []string{"a", "poison", "b"})

	syncable := &batchTransientSyncable{
		poison:       "poison",
		transientErr: fmt.Errorf("downstream rejected the row"),
	}
	require.NoError(t, d.Sync(context.Background(), id, syncable))

	require.Eventually(t, func() bool {
		_, err := d.DeadLetterStuckSyncable(context.Background(), id)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "worker should publish that the batch is blocked")

	// Only the poison proposal is dead-lettered; the healthy proposals in the
	// batch are synced individually during isolation.
	var dls []cluster.SyncableDeadLetter
	require.Eventually(t, func() bool {
		dls, _ = d.SyncableDeadLetters(id, 0, 10)
		return len(dls) == 1
	}, 5*time.Second, 5*time.Millisecond, "isolation must dead-letter exactly the failing proposal")
	require.Equal(t, "manual", dls[0].Kind)

	require.Eventually(t, func() bool {
		got := syncable.soloSynced()
		return contains(got, "a") && contains(got, "b")
	}, 5*time.Second, 5*time.Millisecond, "healthy proposals in the wedged batch must still sync")
	require.NotContains(t, syncable.soloSynced(), "poison")
}

// TestDeadLetterStuckSyncable_SurvivesRestart asserts the durability of a
// manual skip: after the worker dead-letters a wedged proposal and the node
// restarts, the worker excludes the proposal via the replicated dead-letter
// record instead of re-wedging on it (a transient error would otherwise
// re-occur — the syncable never declares it permanent).
func TestDeadLetterStuckSyncable_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	id := "stuck-restart"
	p := parser.New()

	// Run 1: wedge on the (last) proposal, then operator-skip it.
	s1, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })
	d1 := db.New(uint64(1), db.Peers{1: ""}, s1, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithSyncStuckThreshold(50*time.Millisecond))
	seedUserProposals(t, d1, s1, "evt", []string{"poison"})

	syncable1 := &transientSyncable{
		stuck:        map[string]bool{"poison": true},
		transientErr: fmt.Errorf("downstream rejected the row"),
	}
	require.NoError(t, d1.Sync(context.Background(), id, syncable1))

	require.Eventually(t, func() bool {
		_, err := d1.DeadLetterStuckSyncable(context.Background(), id)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		dls, _ := d1.SyncableDeadLetters(id, 0, 10)
		return len(dls) == 1 && dls[0].Kind == "manual"
	}, 5*time.Second, 5*time.Millisecond, "manual dead letter must be durably recorded before restart")

	// Wait for the worker to clear its stuck record after honoring the skip,
	// so run 2 starts from a clean replicated state (otherwise a stale stuck
	// record would make it look blocked before its worker even runs).
	require.Eventually(t, func() bool {
		_, ok, _ := d1.SyncableStuck(id)
		return !ok
	}, 5*time.Second, 5*time.Millisecond, "worker should clear its stuck record after the skip")

	require.NoError(t, d1.Close())
	require.NoError(t, s1.Close())

	// Run 2: reopen the same data dir. The worker must NOT re-wedge — the
	// durable dead-letter record excludes the poison proposal before Sync.
	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d2 := db.New(uint64(1), db.Peers{1: ""}, s2, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithSyncStuckThreshold(50*time.Millisecond))
	t.Cleanup(func() { _ = d2.Close(); _ = s2.Close() })

	syncable2 := &transientSyncable{
		stuck:        map[string]bool{"poison": true},
		transientErr: fmt.Errorf("downstream rejected the row"),
	}
	require.NoError(t, d2.Sync(context.Background(), id, syncable2))

	// Give the worker time to catch up. It should never report being stuck,
	// because it skips the already-dead-lettered proposal without syncing it.
	require.Never(t, func() bool {
		_, err := d2.DeadLetterStuckSyncable(context.Background(), id)
		return err == nil
	}, 2*time.Second, 50*time.Millisecond, "a dead-lettered proposal must not re-wedge the worker after restart")

	// And the syncable's Sync was never invoked for the excluded proposal.
	require.Empty(t, syncable2.syncedPayloads())
	require.Zero(t, syncable2.sawStuck())
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
