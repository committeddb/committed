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
)

// proposeEntity proposes a single user entity of the given (already
// registered + resolved) type and blocks until it applies.
func proposeEntity(t *testing.T, d *db.DB, typ *cluster.Type, key, payload string) {
	t.Helper()
	p := &cluster.Proposal{Entities: []*cluster.Entity{{Type: typ, Key: []byte(key), Data: []byte(payload)}}}
	require.NoError(t, d.Propose(testCtx(t), p))
}

// topicSyncable is a single-path (HTTP-shape) syncable that only "owns"
// entities of matchType: it returns shouldSnapshot=true (→ a checkpoint bump)
// for them and false for everything else (other topics, config/system
// entries), exactly like the HTTP webhook syncable on a topic mismatch. It
// never implements SyncBatch, so the worker runs the single path.
type topicSyncable struct {
	matchType string
	mu        sync.Mutex
	matched   int
}

func (s *topicSyncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	mine := false
	for _, e := range a.Entities {
		if e.Type != nil && e.Type.ID == s.matchType {
			mine = true
		}
	}
	if !mine {
		return cluster.ShouldSnapshot(false), nil
	}
	s.mu.Lock()
	s.matched++
	s.mu.Unlock()
	return cluster.ShouldSnapshot(true), nil
}

func (s *topicSyncable) Close() error { return nil }

// batchAllSyncable is a BatchSyncable (SQL-shape) that accepts every batch and
// requests a snapshot, so the worker advances the checkpoint to the consumed
// head (the last index in each batch) — the path that is already
// consumed-head and needs no EOF change.
type batchAllSyncable struct {
	mu sync.Mutex
	n  int
}

func (b *batchAllSyncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return cluster.ShouldSnapshot(true), nil
}

func (b *batchAllSyncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	b.mu.Lock()
	b.n += len(as)
	b.mu.Unlock()
	return true, nil
}

func (b *batchAllSyncable) Close() error { return nil }

func waitCaughtUp(t *testing.T, d *db.DB, id string, wantHead uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		cp, head, err := d.SyncableProgress(id)
		return err == nil && head == wantHead && cp == head
	}, 5*time.Second, 10*time.Millisecond,
		"syncable %q never reached lag 0 (checkpoint==head==%d)", id, wantHead)
}

// TestSyncableProgress_SelectiveSingleZeroLagWithOtherTopicTail is the
// regression the naive design fails: a selective single (HTTP-shape) syncable
// whose log tail is entirely OTHER-topic entries must still report lag 0 when
// caught up. The consumed-head EOF bump advances its checkpoint past the
// trailing run it read and cheaply skipped; without it the checkpoint would
// stall at the last matching entry and lag would read a permanent, non-zero
// phantom backlog equal to the tail length.
func TestSyncableProgress_SelectiveSingleZeroLagWithOtherTopicTail(t *testing.T) {
	d, s := newWalDB(t)
	const id = "selective-1"
	const mine, other = "topic-mine", "topic-other"

	proposeTypeTOML(t, d, mine, mine, "", "")
	proposeTypeTOML(t, d, other, other, "", "")
	mineT, err := s.ResolveType(cluster.LatestTypeRef(mine))
	require.NoError(t, err)
	otherT, err := s.ResolveType(cluster.LatestTypeRef(other))
	require.NoError(t, err)

	// Two of the syncable's own topic, then a TRAILING run of other-topic
	// entries it will read and skip without bumping.
	proposeEntity(t, d, mineT, "m0", "a")
	proposeEntity(t, d, mineT, "m1", "b")
	headAfterMine := s.DataEventIndex()

	proposeEntity(t, d, otherT, "o0", "c")
	proposeEntity(t, d, otherT, "o1", "d")
	proposeEntity(t, d, otherT, "o2", "e")
	head := s.DataEventIndex()
	require.Greater(t, head, headAfterMine, "the other-topic tail must have raised the data head")

	sel := &topicSyncable{matchType: mine}
	require.NoError(t, d.Sync(context.Background(), id, sel))

	waitCaughtUp(t, d, id, head)

	// The crux: the checkpoint advanced PAST the last matching entry into the
	// other-topic tail — proof the EOF bump targets the consumed head, not the
	// last matched index.
	cp, _, err := d.SyncableProgress(id)
	require.NoError(t, err)
	require.Greater(t, cp, headAfterMine,
		"checkpoint must advance past the last matched entry to the consumed head")
}

// TestSyncableProgress_BatchZeroLag confirms the SQL/batch shape reports lag 0
// when caught up. The batch path already advances to the consumed head
// (batch[len-1].Index), so this needs no EOF change — it guards against a
// regression there.
func TestSyncableProgress_BatchZeroLag(t *testing.T) {
	d, s := newWalDB(t)
	const id = "batch-1"
	const a, b = "topic-a", "topic-b"

	proposeTypeTOML(t, d, a, a, "", "")
	proposeTypeTOML(t, d, b, b, "", "")
	aT, err := s.ResolveType(cluster.LatestTypeRef(a))
	require.NoError(t, err)
	bT, err := s.ResolveType(cluster.LatestTypeRef(b))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		proposeEntity(t, d, aT, fmt.Sprintf("a%d", i), "x")
	}
	proposeEntity(t, d, bT, "b0", "y") // a different topic in the tail
	head := s.DataEventIndex()
	require.NotZero(t, head)

	require.NoError(t, d.Sync(context.Background(), id, &batchAllSyncable{}))
	waitCaughtUp(t, d, id, head)
}

// TestSyncableProgress_NeverCheckpointedReportsFullLag covers the accessor
// directly: a syncable that has never checkpointed reports checkpoint 0 and a
// head equal to the data head, so lag == head (it hasn't synced anything yet).
// No worker needed — this pins the accessor's contract deterministically.
func TestSyncableProgress_NeverCheckpointedReportsFullLag(t *testing.T) {
	d, s := newWalDB(t)
	const topic = "topic-z"

	proposeTypeTOML(t, d, topic, topic, "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef(topic))
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		proposeEntity(t, d, typ, fmt.Sprintf("k%d", i), "v")
	}
	head := s.DataEventIndex()
	require.NotZero(t, head)

	cp, gotHead, err := d.SyncableProgress("never-synced")
	require.NoError(t, err)
	require.Zero(t, cp, "a never-checkpointed syncable reports checkpoint 0")
	require.Equal(t, head, gotHead)
	// lag (computed by the handler) = max(0, head-0) = head > 0.
}

// TestSyncableProgress_InMemoryStorageReportsZeros documents that the
// optional reporter is absent on the in-memory test double, so SyncableProgress
// degrades to (0, 0, nil) rather than failing. Production always uses
// wal.Storage; this guards the type-assertion fallback.
func TestSyncableProgress_InMemoryStorageReportsZeros(t *testing.T) {
	d := createDB()
	defer d.Close()

	cp, head, err := d.SyncableProgress("anything")
	require.NoError(t, err)
	require.Zero(t, cp)
	require.Zero(t, head)
}
