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

// teardownRecorder counts Teardown() calls across every syncable a parser
// builds (the pointer is shared across incarnations), can be primed to fail
// the drop, and records the payloads Sync observed (so a rebuild's
// replay-from-0 is provable: the same payloads are delivered again).
type teardownRecorder struct {
	mu        sync.Mutex
	teardowns int
	closes    int
	failWith  error
	synced    []string
}

func (r *teardownRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.teardowns
}

// closeCount reports how many times a syncable's Close (prepared-statement
// release) ran — worker teardown must call it so redeploys don't leak.
func (r *teardownRecorder) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

func (r *teardownRecorder) syncedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.synced)
}

// teardownSyncable implements cluster.Teardownable so deleteSync's owner-gated
// drop is observable without a real database.
type teardownSyncable struct{ rec *teardownRecorder }

func (s *teardownSyncable) Sync(_ context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	s.rec.mu.Lock()
	s.rec.synced = append(s.rec.synced, string(p.Entities[0].Data))
	s.rec.mu.Unlock()
	// Snapshot=true advances the persisted SyncableIndex past user proposals,
	// so the checkpoint is non-zero before the delete/rebuild — which makes the
	// index-tombstone meaningful.
	return cluster.ShouldSnapshot(true), nil
}

func (s *teardownSyncable) Close() error {
	s.rec.mu.Lock()
	s.rec.closes++
	s.rec.mu.Unlock()
	return nil
}

func (s *teardownSyncable) Teardown() error {
	s.rec.mu.Lock()
	defer s.rec.mu.Unlock()
	s.rec.teardowns++
	return s.rec.failWith
}

type teardownParser struct{ rec *teardownRecorder }

func (p *teardownParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return &teardownSyncable{rec: p.rec}, nil
}

func newDeleteTestDB(t *testing.T, dir string, rec *teardownRecorder) (*db.DB, *wal.Storage) {
	t.Helper()
	p := parser.New()
	p.AddSyncableParser("test", &teardownParser{rec: rec})
	sync := make(chan *db.SyncableWithID)
	s, err := wal.Open(dir, p, sync, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, sync, nil, db.WithTickInterval(testTickInterval))
	return d, s
}

func configureDeleteSyncable(t *testing.T, d *db.DB, id string) {
	t.Helper()
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[syncable]\ntype = \"test\"\nname = \"" + id + "\""),
	}))
}

func hasSyncable(t *testing.T, s *wal.Storage, id string) bool {
	t.Helper()
	cfgs, err := s.Syncables()
	require.NoError(t, err)
	for _, c := range cfgs {
		if c.ID == id {
			return true
		}
	}
	return false
}

// TestDeleteSyncable_RemovesConfigAndIndex_AndTearsDownOnOwner is the core
// Phase 2 proof: DELETE removes the config and the checkpoint atomically (one
// Actual, so Propose returns with both already gone), and the owner drops the
// destination table best-effort.
func TestDeleteSyncable_RemovesConfigAndIndex_AndTearsDownOnOwner(t *testing.T) {
	dir := t.TempDir()
	const id = "del-sync"
	rec := &teardownRecorder{}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"a", "b"})

	require.Eventually(t, func() bool {
		cp, _ := s.GetSyncableIndex(id)
		return cp > 0
	}, 10*time.Second, 10*time.Millisecond, "checkpoint should advance above 0 before delete")
	require.True(t, hasSyncable(t, s, id))

	require.NoError(t, d.DeleteSyncable(testCtx(t), id, false))

	// Propose blocks until applied, so config + index are already gone.
	require.False(t, hasSyncable(t, s, id), "config must be removed")
	cp, err := s.GetSyncableIndex(id)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cp, "checkpoint must be removed (a same-name create resumes from 0)")

	// The owner drops the table (async via deleteSync).
	require.Eventually(t, func() bool { return rec.count() == 1 },
		10*time.Second, 10*time.Millisecond, "owner should tear the table down exactly once")

	// And the worker teardown releases the syncable's prepared statements
	// (Close) — node-local, so it runs on delete regardless of ownership.
	require.Eventually(t, func() bool { return rec.closeCount() == 1 },
		10*time.Second, 10*time.Millisecond, "delete must close the syncable exactly once")
}

// keepData preserves the destination: the logical delete still happens, but
// the owner does not tear the destination down.
func TestDeleteSyncable_KeepData_SkipsTeardown(t *testing.T) {
	dir := t.TempDir()
	const id = "del-keep"
	rec := &teardownRecorder{}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)

	require.NoError(t, d.DeleteSyncable(testCtx(t), id, true))
	require.False(t, hasSyncable(t, s, id), "config must still be removed")

	require.Never(t, func() bool { return rec.count() > 0 },
		500*time.Millisecond, 20*time.Millisecond, "keepData must skip the teardown")
}

// A failed table drop must not fail the delete: the logical deletion already
// committed via consensus, so the worst case is an orphaned table.
func TestDeleteSyncable_TeardownFailureDoesNotFailDelete(t *testing.T) {
	dir := t.TempDir()
	const id = "del-failteardown"
	rec := &teardownRecorder{failWith: errors.New("permission denied")}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)

	require.NoError(t, d.DeleteSyncable(testCtx(t), id, false), "delete must succeed even if teardown fails")
	require.False(t, hasSyncable(t, s, id))
	require.Eventually(t, func() bool { return rec.count() >= 1 },
		10*time.Second, 10*time.Millisecond, "teardown should still be attempted")
}

// A syncable that owns no destination state (implements no Teardownable — e.g.
// an HTTP webhook) deletes cleanly through the generic path: the worker stops
// and the config + checkpoint are removed, with no teardown attempted and no
// SQL machinery involved. This is the proof the delete path is syncable-
// agnostic.
func TestDeleteSyncable_NonTeardownableSyncable(t *testing.T) {
	dir := t.TempDir()
	const id = "del-webhook-like"

	// resumeParser builds resumeSyncable, which implements only Sync/Close —
	// no Teardownable, like the webhook syncable.
	p := parser.New()
	p.AddSyncableParser("test", &resumeParser{r: &resumeRecorder{}})
	sync := make(chan *db.SyncableWithID)
	s, err := wal.Open(dir, p, sync, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, sync, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[syncable]\ntype = \"test\"\nname = \"" + id + "\""),
	}))
	require.True(t, hasSyncable(t, s, id))

	// No panic, no error, config gone — even though there's nothing to tear down.
	require.NoError(t, d.DeleteSyncable(testCtx(t), id, false))
	require.False(t, hasSyncable(t, s, id))
}

// Restart-then-apply safety: after DELETE the config is off the log, so a
// node restart must NOT resurrect it (no re-Init of the removed schema), even
// through RestoreSyncableWorkers.
func TestDeleteSyncable_RestartDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	const id = "del-restart"
	rec := &teardownRecorder{}

	d1, s1 := newDeleteTestDB(t, dir, rec)
	configureDeleteSyncable(t, d1, id)
	require.True(t, hasSyncable(t, s1, id))
	require.NoError(t, d1.DeleteSyncable(testCtx(t), id, false))
	require.False(t, hasSyncable(t, s1, id))
	require.NoError(t, d1.Close())
	require.NoError(t, s1.Close())

	// Reopen the same data dir: the deleted config must stay gone.
	d2, s2 := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d2.Close() })
	require.False(t, hasSyncable(t, s2, id), "deleted syncable must not be resurrected on restart")
	s2.RestoreSyncableWorkers()
	require.False(t, hasSyncable(t, s2, id), "RestoreSyncableWorkers must not rebuild a deleted syncable")
}

// TestDeleteSyncable_StaleCheckpointBumpDoesNotOrphan is the delete-race
// regression: a worker checkpoint bump that commits AFTER the config+checkpoint
// delete must not re-establish an orphaned SyncableIndex — otherwise a same-id
// recreate resumes from it and silently skips history. The invariant enforced in
// saveSyncableIndex ("a checkpoint persists only while its config exists") makes
// the stale bump a no-op, independent of worker/propose timing.
func TestDeleteSyncable_StaleCheckpointBumpDoesNotOrphan(t *testing.T) {
	dir := t.TempDir()
	const id = "orphan-sync"
	rec := &teardownRecorder{}
	d, s := newDeleteTestDB(t, dir, rec)
	t.Cleanup(func() { _ = d.Close() })

	configureDeleteSyncable(t, d, id)

	// Establish a checkpoint, as a running worker would.
	bump := func(n uint64) {
		e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: n})
		require.NoError(t, err)
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
	}
	bump(5)
	cp, err := s.GetSyncableIndex(id)
	require.NoError(t, err)
	require.Equal(t, uint64(5), cp, "checkpoint persists while the config exists")

	// Delete the syncable (config + checkpoint tombstone, one Actual).
	require.NoError(t, d.DeleteSyncable(testCtx(t), id, false))
	require.False(t, hasSyncable(t, s, id), "config removed")
	cp, err = s.GetSyncableIndex(id)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cp, "checkpoint cleared by the delete")

	// A stale bump commits after the delete — must NOT re-orphan the checkpoint.
	bump(9)
	cp, err = s.GetSyncableIndex(id)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cp, "a checkpoint bump for a deleted syncable must be dropped (no orphan)")
}
