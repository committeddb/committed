package db_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// ingestTeardownRecorder counts Teardown() and Close() calls and can be primed
// to fail the drop, so the owner-gated source teardown AND the lifecycle-release
// Close are both observable without a real source. Teardown is the destructive,
// owner-only slot drop; Close is the node-local resource release every teardown
// path must invoke.
type ingestTeardownRecorder struct {
	mu        sync.Mutex
	teardowns int
	closes    int
	failWith  error
}

func (r *ingestTeardownRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.teardowns
}

func (r *ingestTeardownRecorder) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

// fakeIngestable implements cluster.Ingestable + cluster.IngestableTeardownable.
// Ingest checkpoints one position (so the position-clear on delete is provable)
// then blocks until cancelled, like a streaming worker.
type fakeIngestable struct{ rec *ingestTeardownRecorder }

func (f *fakeIngestable) Ingest(ctx context.Context, _ cluster.Position, _ chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	select {
	case po <- cluster.Position("cp-1"):
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (f *fakeIngestable) Close() error {
	f.rec.mu.Lock()
	defer f.rec.mu.Unlock()
	f.rec.closes++
	return nil
}

func (f *fakeIngestable) Teardown() error {
	f.rec.mu.Lock()
	defer f.rec.mu.Unlock()
	f.rec.teardowns++
	return f.rec.failWith
}

type fakeIngestableParser struct{ rec *ingestTeardownRecorder }

func (p *fakeIngestableParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) {
	return &fakeIngestable{rec: p.rec}, nil
}

func newDeleteIngestTestDB(t *testing.T, dir string, rec *ingestTeardownRecorder) (*db.DB, *wal.Storage) {
	t.Helper()
	p := parser.New()
	p.AddIngestableParser("test", &fakeIngestableParser{rec: rec})
	ingest := make(chan *db.IngestableWithID)
	s, err := wal.Open(dir, p, nil, ingest, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, ingest, db.WithTickInterval(testTickInterval))
	return d, s
}

func configureIngestable(t *testing.T, d *db.DB, id string) {
	t.Helper()
	require.NoError(t, d.ProposeIngestable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[ingestable]\ntype = \"test\"\nname = \"" + id + "\""),
	}))
}

func hasIngestable(t *testing.T, s *wal.Storage, id string) bool {
	t.Helper()
	cfgs, err := s.Ingestables()
	require.NoError(t, err)
	for _, c := range cfgs {
		if c.ID == id {
			return true
		}
	}
	return false
}

// TestDeleteIngestable_RemovesConfigAndCheckpoint_AndTearsDownOnOwner is the
// end-to-end proof: DELETE removes the config and the checkpoint atomically, and
// the owner drops the source-side replication resources best-effort.
func TestDeleteIngestable_RemovesConfigAndCheckpoint_AndTearsDownOnOwner(t *testing.T) {
	dir := t.TempDir()
	const id = "del-ingest"
	rec := &ingestTeardownRecorder{}
	d, s := newDeleteIngestTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureIngestable(t, d, id)
	require.True(t, hasIngestable(t, s, id))
	// The worker checkpoints a position — so we can prove the delete clears it.
	require.Eventually(t, func() bool { return s.Position(id) != nil },
		10*time.Second, 10*time.Millisecond, "the worker should checkpoint a position before delete")

	require.NoError(t, d.DeleteIngestable(testCtx(t), id))

	// Propose blocks until applied, so config + checkpoint are already gone.
	require.False(t, hasIngestable(t, s, id), "config must be removed")
	require.Nil(t, s.Position(id), "checkpoint must be cleared (a same-id recreate re-snapshots)")

	// The owner tears the source down (drops the slot) async via deleteIngest.
	require.Eventually(t, func() bool { return rec.count() == 1 },
		10*time.Second, 10*time.Millisecond, "owner should tear the source down exactly once")
}

// TestDeleteIngestable_ClosesDrainedIngestable proves the A8d release half of the
// lifecycle contract: deleting an ingestable drains its worker and then calls
// Ingestable.Close() to release the node-local source resources (connection pool,
// binlog syncer). This is independent of, and precedes, the owner-only destructive
// Teardown (the slot drop). Pre-fix, Close() was wired to nothing — no teardown
// path invoked it — so a redeploy leaked whatever the Ingestable held.
func TestDeleteIngestable_ClosesDrainedIngestable(t *testing.T) {
	dir := t.TempDir()
	const id = "del-ingest-close"
	rec := &ingestTeardownRecorder{}
	d, s := newDeleteIngestTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureIngestable(t, d, id)
	require.True(t, hasIngestable(t, s, id))
	// Gate on a checkpoint so the worker is provably live (built + running) with a
	// handle to close before we delete.
	require.Eventually(t, func() bool { return s.Position(id) != nil },
		10*time.Second, 10*time.Millisecond, "the worker should be running before delete")

	require.NoError(t, d.DeleteIngestable(testCtx(t), id))

	// deleteIngest drains the worker, then releases its Ingestable via Close. The
	// teardown runs async off the apply signal (like the Teardown assertion below),
	// so poll.
	require.Eventually(t, func() bool { return rec.closeCount() == 1 },
		10*time.Second, 10*time.Millisecond,
		"delete must Close the drained ingestable exactly once")
}

// TestCloseDB_ClosesDrainedIngestable proves the shutdown teardown path (A8d):
// db.Close drains every ingest worker and releases its Ingestable via Close. Close
// is synchronous (it waits out the worker drain before returning), so the release
// has already happened by the time Close returns.
func TestCloseDB_ClosesDrainedIngestable(t *testing.T) {
	dir := t.TempDir()
	const id = "shutdown-ingest-close"
	rec := &ingestTeardownRecorder{}
	d, s := newDeleteIngestTestDB(t, dir, rec)

	configureIngestable(t, d, id)
	require.Eventually(t, func() bool { return s.Position(id) != nil },
		10*time.Second, 10*time.Millisecond, "the worker should be running before shutdown")

	require.NoError(t, d.Close())

	require.Equal(t, 1, rec.closeCount(),
		"Close must release the drained ingestable exactly once on shutdown")
}

// TestReplaceIngestable_ClosesSupersededIngestable proves the replace teardown
// path (A8d): a re-POST rebuilds the worker, and db.Ingest's replace loop drains
// and Closes the superseded Ingestable so a redeploy doesn't leak it. saveIngestable
// re-sends on the ingest channel for every upsert (even a byte-identical one), so a
// repeat POST of the same config exercises the replace path.
func TestReplaceIngestable_ClosesSupersededIngestable(t *testing.T) {
	dir := t.TempDir()
	const id = "replace-ingest-close"
	rec := &ingestTeardownRecorder{}
	d, s := newDeleteIngestTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureIngestable(t, d, id)
	require.Eventually(t, func() bool { return s.Position(id) != nil },
		10*time.Second, 10*time.Millisecond, "the first worker should be running before replace")

	// Re-POST the same id: the apply builds a fresh worker and db.Ingest's replace
	// loop must Close the superseded one. (t.Cleanup's Close will later close the
	// replacement — a second Close — but we assert the superseded close here.)
	configureIngestable(t, d, id)

	require.Eventually(t, func() bool { return rec.closeCount() == 1 },
		10*time.Second, 10*time.Millisecond,
		"re-POST must Close the superseded ingestable exactly once")
}

// A failed source teardown must not fail the delete: the logical deletion already
// committed via consensus, so the worst case is an orphaned slot.
func TestDeleteIngestable_TeardownFailureDoesNotFailDelete(t *testing.T) {
	dir := t.TempDir()
	const id = "del-ingest-fail"
	rec := &ingestTeardownRecorder{failWith: errors.New("slot busy")}
	d, s := newDeleteIngestTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureIngestable(t, d, id)
	require.NoError(t, d.DeleteIngestable(testCtx(t), id), "delete must succeed even if teardown fails")
	require.False(t, hasIngestable(t, s, id))
	require.Eventually(t, func() bool { return rec.count() >= 1 },
		10*time.Second, 10*time.Millisecond, "teardown should still be attempted")
}
