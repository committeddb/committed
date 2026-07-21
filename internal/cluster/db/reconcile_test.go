package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// reconcileFakeSyncable is a do-nothing syncable that records whether its
// destination teardown ran — the reconcile-cancel must NEVER tear down.
type reconcileFakeSyncable struct {
	mu        sync.Mutex
	teardowns int
}

func (f *reconcileFakeSyncable) Init(context.Context) error { return nil }
func (f *reconcileFakeSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (f *reconcileFakeSyncable) Close() error { return nil }
func (f *reconcileFakeSyncable) Teardown() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.teardowns++
	return nil
}

func (f *reconcileFakeSyncable) teardownCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.teardowns
}

// reconcileHarness wires a db whose sync/ingest channels the TEST also holds,
// so it can inject hand-crafted reconcile requests into the real listener.
func reconcileHarness(t *testing.T) (*db.DB, chan *db.SyncableWithID, chan *db.IngestableWithID) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	syncCh := make(chan *db.SyncableWithID)
	ingestCh := make(chan *db.IngestableWithID)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, syncCh, ingestCh
}

// TestReconcile_CancelsSyncWorkerWithoutConfig pins the compacted-delete
// zombie fix: a RUNNING worker whose id is absent from the reconcile's fresh
// config list is cancelled — and its destination is NOT torn down (the live
// delete path owns teardown; a reconcile only converges the worker set).
// Before the reconciler, such a worker (its delete compacted into an
// InstallSnapshot, so no apply event ever fired) idled until leadership and
// then synced a deleted syncable forever.
func TestReconcile_CancelsSyncWorkerWithoutConfig(t *testing.T) {
	d, syncCh, _ := reconcileHarness(t)

	fake := &reconcileFakeSyncable{}
	require.NoError(t, d.Sync(context.Background(), "zombie", fake))
	require.Eventually(t, func() bool { return d.HasSyncWorkerForTest("zombie") },
		2*time.Second, 5*time.Millisecond)

	// Inject a reconcile whose fresh list has NO configs — as after an
	// InstallSnapshot that learned the delete.
	syncCh <- &db.SyncableWithID{ReconcileList: func() ([]*db.SyncableWithID, error) {
		return nil, nil
	}}

	require.Eventually(t, func() bool { return !d.HasSyncWorkerForTest("zombie") },
		5*time.Second, 10*time.Millisecond,
		"the reconcile must cancel a worker whose config no longer exists")
	require.Zero(t, fake.teardownCount(),
		"a reconcile cancel must never run destination teardown")
}

// reconcileFakeIngestable idles until cancelled.
type reconcileFakeIngestable struct{}

func (reconcileFakeIngestable) Ingest(ctx context.Context, _ cluster.Position, _ chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	<-ctx.Done()
	return nil
}
func (reconcileFakeIngestable) Close() error { return nil }
func (reconcileFakeIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// TestReconcile_CancelsIngestWorkerWithoutConfig is the ingest twin — the
// worse zombie (on the leader it resumes streaming a deleted ingestable from
// the source).
func TestReconcile_CancelsIngestWorkerWithoutConfig(t *testing.T) {
	d, _, ingestCh := reconcileHarness(t)

	require.NoError(t, d.Ingest(context.Background(), "zombie-ing", reconcileFakeIngestable{}))
	require.Eventually(t, func() bool { return d.HasIngestWorkerForTest("zombie-ing") },
		2*time.Second, 5*time.Millisecond)

	ingestCh <- &db.IngestableWithID{ReconcileList: func() ([]*db.IngestableWithID, error) {
		return nil, nil
	}}

	require.Eventually(t, func() bool { return !d.HasIngestWorkerForTest("zombie-ing") },
		5*time.Second, 10*time.Millisecond,
		"the reconcile must cancel an ingest worker whose config no longer exists")
}
