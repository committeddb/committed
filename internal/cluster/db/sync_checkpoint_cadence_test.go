package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// cadenceSyncable is a single-path (HTTP-shape) syncable for the cadence
// tests: every user entity is a matched, validated checkpoint boundary
// (shouldSnapshot=true), it carries a configurable CheckpointPolicy, records
// the raft index of each completed sync, and can PARK (block until ctx is
// canceled) on a chosen sync so a test can freeze the worker mid-stream — a
// deterministic stand-in for a crash that leaves an un-checkpointed tail, with
// no reader EOF (and thus no EOF flush) to muddy the persisted index.
type cadenceSyncable struct {
	policy cluster.CheckpointPolicy

	mu     sync.Mutex
	synced []uint64

	// parkAt, if > 0, blocks the parkAt-th user Sync on ctx.Done (recording
	// nothing for it) and closes parked once it has blocked.
	parkAt int
	parked chan struct{}
}

func (c *cadenceSyncable) CheckpointPolicy() cluster.CheckpointPolicy { return c.policy }

func (c *cadenceSyncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(a) {
		return cluster.ShouldSnapshot(false), nil
	}

	c.mu.Lock()
	n := len(c.synced) + 1
	c.mu.Unlock()

	if c.parkAt > 0 && n == c.parkAt {
		if c.parked != nil {
			close(c.parked)
		}
		<-ctx.Done() // freeze here; the replace/Close cancel releases us
		return cluster.ShouldSnapshot(false), nil
	}

	c.mu.Lock()
	c.synced = append(c.synced, a.Index)
	c.mu.Unlock()
	return cluster.ShouldSnapshot(true), nil
}

func (c *cadenceSyncable) Close() error { return nil }

func (c *cadenceSyncable) syncedIndices() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uint64, len(c.synced))
	copy(out, c.synced)
	return out
}

// TestCheckpointCadence_EveryStridesByN proves checkpointEvery=N persists the
// index once per N successful syncs: the probe reads the persisted
// SyncableIndex at the start of each sync, and with N=3 over 6 proposals it
// sees [0,0,0, p3,p3,p3] — no bump until the 3rd sync completes, then a single
// bump that holds through the 4th–6th.
func TestCheckpointCadence_EveryStridesByN(t *testing.T) {
	d, s := newWalDB(t)
	const id = "cadence-stride"
	seedSyncableConfig(t, d, id)

	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2", "v3", "v4", "v5"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &probeSyncable{
		s: s, id: id,
		policy: cluster.CheckpointPolicy{Every: 3},
		doneAt: 6,
		cancel: cancel,
	}
	require.NoError(t, d.Sync(context.Background(), id, probe))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never consumed all proposals")
	}

	got := probe.observed()
	require.GreaterOrEqual(t, len(got), 6)

	// First group of 3: no bump yet.
	require.Equal(t, uint64(0), got[0])
	require.Equal(t, uint64(0), got[1])
	require.Equal(t, uint64(0), got[2])
	// Exactly one bump fired after the 3rd sync; it holds across the next
	// group (proof of the N=3 stride — not per-sync, not never).
	require.Greater(t, got[3], uint64(0), "checkpoint must have advanced after the 3rd sync")
	require.Equal(t, got[3], got[4])
	require.Equal(t, got[3], got[5])
}

// TestCheckpointCadence_EOFFlushesSubEveryTail proves the caught-up flush: a
// syncable with a coarse cadence (Every=100) that syncs only 3 proposals and
// then idles still persists those 3 — the EOF flush bumps the checkpoint to
// the consumed head, so a low-traffic syncable never lags forever.
func TestCheckpointCadence_EOFFlushesSubEveryTail(t *testing.T) {
	d, s := newWalDB(t)
	const id = "cadence-eof"
	seedSyncableConfig(t, d, id)

	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2"})

	probe := &probeSyncable{s: s, id: id, policy: cluster.CheckpointPolicy{Every: 100}}
	require.NoError(t, d.Sync(context.Background(), id, probe))

	// The data head is the last user proposal's index. The EOF flush must
	// advance the checkpoint all the way to it despite the sub-100 count.
	want := s.DataEventIndex()
	require.NotZero(t, want)
	require.Eventually(t, func() bool {
		cp, err := s.GetSyncableIndex(id)
		return err == nil && cp == want
	}, 10*time.Second, 10*time.Millisecond,
		"EOF flush should persist the sub-Every tail (checkpoint should reach the data head)")
}

// TestCheckpointCadence_RecoveryReDeliversAtMostEvery is the crash/recovery
// bound: at any moment the synced-but-uncheckpointed tail is at most `every`
// proposals, so a crash re-delivers at most that many. The worker is frozen
// mid-stream (parked, no EOF flush) and we assert directly on the gap between
// what it synced and what it durably checkpointed; then a recovery worker
// resumes from the checkpoint and re-delivers exactly that tail.
func TestCheckpointCadence_RecoveryReDeliversAtMostEvery(t *testing.T) {
	d, s := newWalDB(t)
	const id = "cadence-recovery"
	seedSyncableConfig(t, d, id)
	const every = 4

	// 8 proposals: with Every=4 the worker bumps once (after the 4th), then
	// syncs the 5th–7th un-bumped and parks on the 8th — a 3-proposal
	// un-checkpointed tail (< every), the crash window.
	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"})

	parked := make(chan struct{})
	worker1 := &cadenceSyncable{
		policy: cluster.CheckpointPolicy{Every: every},
		parkAt: 8,
		parked: parked,
	}
	require.NoError(t, d.Sync(context.Background(), id, worker1))

	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never reached the park point")
	}

	// The bound: entries synced but not yet checkpointed (what a crash here
	// would re-deliver) is in (0, every].
	cp, err := s.GetSyncableIndex(id)
	require.NoError(t, err)
	unbumped := 0
	for _, idx := range worker1.syncedIndices() {
		if idx > cp {
			unbumped++
		}
	}
	require.Greater(t, unbumped, 0, "test should exercise a real un-checkpointed tail")
	require.LessOrEqual(t, unbumped, every,
		"a crash must re-deliver at most `every` already-synced proposals")

	// Recovery: a fresh worker replaces the parked one (the replace cancels it,
	// releasing the park) and resumes from the persisted checkpoint. It
	// re-delivers exactly the un-checkpointed tail, then catches up.
	worker2 := &cadenceSyncable{policy: cluster.CheckpointPolicy{Every: 1}}
	require.NoError(t, d.Sync(context.Background(), id, worker2))

	head := s.DataEventIndex()
	require.Eventually(t, func() bool {
		got, err := s.GetSyncableIndex(id)
		return err == nil && got == head
	}, 10*time.Second, 10*time.Millisecond, "recovery worker should catch up to the data head")

	// Every index worker2 re-delivered was > the checkpoint worker1 left, and
	// the redelivery count honors the bound.
	redelivered := worker2.syncedIndices()
	require.LessOrEqual(t, len(redelivered), every+1,
		"recovery re-delivers only the un-checkpointed tail (plus the parked entry), bounded by `every`")
	for _, idx := range redelivered {
		require.Greater(t, idx, cp, "recovery must not re-deliver already-checkpointed proposals")
	}
}

// TestCheckpointCadence_MaxAgeFlushesUnderEvery proves the age bound: with a
// coarse count (Every=1000) but a tight MaxAge and a slow sink, the checkpoint
// advances on the age trigger rather than waiting for 1000 syncs. The probe
// observes a non-zero persisted index well before 1000 syncs — impossible
// without the age flush at this count.
func TestCheckpointCadence_MaxAgeFlushesUnderEvery(t *testing.T) {
	d, s := newWalDB(t)
	const id = "cadence-maxage"
	seedSyncableConfig(t, d, id)

	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2", "v3"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &probeSyncable{
		s: s, id: id,
		// Count effectively never fires; the 5ms sink delay reliably exceeds
		// the 1ms age bound, so the age trigger drives the checkpoint.
		policy:    cluster.CheckpointPolicy{Every: 1000, MaxAge: time.Millisecond},
		syncDelay: 5 * time.Millisecond,
		doneAt:    4,
		cancel:    cancel,
	}
	require.NoError(t, d.Sync(context.Background(), id, probe))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never consumed all proposals")
	}

	// With Every=1000 and no age bound the checkpoint would stay 0 until EOF;
	// the age flush makes it advance mid-stream, so a later sync observes a
	// non-zero persisted index.
	got := probe.observed()
	require.GreaterOrEqual(t, len(got), 4)
	require.Greater(t, got[len(got)-1], uint64(0),
		"MaxAge should have flushed a checkpoint before the Every=1000 count was reached")
}
