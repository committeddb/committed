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

// resumeRecorder collects the user payloads a syncable observes, shared
// across restart incarnations: the parser hands every syncable it builds
// the same recorder pointer, so the post-restart worker's deliveries land
// in the same place as the pre-restart worker's. That's what lets the test
// assert "data proposed AFTER the restart reached the syncable" — i.e. the
// worker actually resumed.
type resumeRecorder struct {
	mu     sync.Mutex
	synced []string
}

func (r *resumeRecorder) add(payload string) {
	r.mu.Lock()
	r.synced = append(r.synced, payload)
	r.mu.Unlock()
}

func (r *resumeRecorder) has(payload string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.synced {
		if s == payload {
			return true
		}
	}
	return false
}

type resumeSyncable struct{ r *resumeRecorder }

func (s *resumeSyncable) Sync(_ context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	s.r.add(string(p.Entities[0].Data))
	// Snapshot=true bumps the persisted SyncableIndex past this proposal, so
	// a resumed worker starts AFTER it rather than re-delivering it.
	return cluster.ShouldSnapshot(true), nil
}

func (s *resumeSyncable) Close() error { return nil }

type resumeParser struct{ r *resumeRecorder }

func (p *resumeParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return &resumeSyncable{r: p.r}, nil
}

// TestRestartResumeSyncable is the end-to-end proof for
// RestoreSyncableWorkers: a syncable configured before a node restart must
// resume syncing newly-proposed data after the restart, without the config
// being re-POSTed.
//
// It mirrors the ingestable restart-resume story but in-process (no Docker):
// a real wal.Storage is closed and reopened at the same data dir, and a fresh
// db.DB is built on top — the in-process stand-in for a node process exiting
// and restarting. Because the persisted appliedIndex makes ApplyCommitted
// idempotent, reopening does NOT re-run handleSyncable, so the apply-path send
// (the only thing that ever starts a syncable worker) does not fire. Without
// RestoreSyncableWorkers the worker would stay dead and the syncable would
// silently stop — this test pins that RestoreSyncableWorkers respawns it.
func TestRestartResumeSyncable(t *testing.T) {
	dir := t.TempDir()
	const id = "resume-sync"
	rec := &resumeRecorder{}

	// --- Incarnation 1: configure the syncable and sync one proposal. ---
	p1 := parser.New()
	p1.AddSyncableParser("test", &resumeParser{r: rec})
	sync1 := make(chan *db.SyncableWithID)
	s1, err := wal.Open(dir, p1, sync1, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })
	d1 := db.New(uint64(1), db.Peers{1: ""}, s1, p1, sync1, nil, db.WithTickInterval(testTickInterval))

	// ProposeSyncable applies the config; saveSyncable's apply-path send
	// starts the live worker (the path that does NOT re-fire on restart).
	require.NoError(t, d1.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[syncable]\ntype = \"test\"\nname = \"" + id + "\""),
	}))

	seedUserProposals(t, d1, s1, "evt1", []string{"phase1"})
	require.Eventually(t, func() bool { return rec.has("phase1") },
		10*time.Second, 10*time.Millisecond,
		"phase-1 proposal should reach the live syncable before the restart")

	// --- Restart: stop the DB, then close the storage, then reopen the
	// same dir. db.Close blocks until the raft loop (and thus Storage.Save)
	// has stopped, so closing the storage afterward can't race a write, and
	// the bbolt/WAL file locks are released before the reopen below. ---
	require.NoError(t, d1.Close())
	require.NoError(t, s1.Close())

	// --- Incarnation 2: fresh parser + storage + DB on the same dir. ---
	p2 := parser.New()
	p2.AddSyncableParser("test", &resumeParser{r: rec})
	sync2 := make(chan *db.SyncableWithID)
	s2, err := wal.Open(dir, p2, sync2, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })
	d2 := db.New(uint64(1), db.Peers{1: ""}, s2, p2, sync2, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d2.Close() })

	// Propose new data AFTER the restart but BEFORE restoring workers. The
	// resume gap: nothing is draining for this syncable, so it must NOT be
	// synced yet. (The wal unit test TestRestoreSyncableWorkers proves Open
	// does not auto-restore; here we prove the consequence end-to-end.)
	//
	// Reuse the type registered in incarnation 1 — it is already durable, so
	// seedUserProposals' type propose is a byte-identical no-op and its
	// ResolveType reads the persisted type without racing the just-restarted
	// node's apply backlog (which a brand-new type would).
	seedUserProposals(t, d2, s2, "evt1", []string{"phase2"})
	require.Never(t, func() bool { return rec.has("phase2") },
		300*time.Millisecond, 20*time.Millisecond,
		"without RestoreSyncableWorkers the resumed node must not sync new data")

	// The explicit restore — exactly what cmd/node.go runs after wiring the
	// parsers — respawns the worker, which resumes from the persisted
	// SyncableIndex and delivers the post-restart proposal.
	s2.RestoreSyncableWorkers()
	require.Eventually(t, func() bool { return rec.has("phase2") },
		10*time.Second, 10*time.Millisecond,
		"RestoreSyncableWorkers must respawn the worker so post-restart data syncs")
}
