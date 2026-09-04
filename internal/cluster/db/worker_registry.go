package db

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// workerRegistry is the sync/ingest worker registry: the per-ID worker
// handles for both kinds, the closed flag, and the one lock guarding them.
// The tables stay separate (never one map keyed by kind) so a reconcile of
// one kind can never cancel the other's workers.
//
// The two tables key running per-ID worker goroutines (one per syncable /
// ingestable ID), so that a second db.Sync / db.Ingest call for the same ID
// cancels and replaces the existing worker instead of spawning a duplicate
// that would race with the first over the same Reader, Position, and
// proposeC slot. db.Close cancels every entry and waits for the workers'
// done channels.
//
// closed is set to true by db.Close after it has drained the registry.
// db.Sync / db.Ingest check it under mu and reject installs with ErrClosed
// when set, so a late caller (e.g., listenForSyncables waking up after the
// drain has run) can't spawn an unobserved worker that escapes the drain.
// Without this flag, the spawn-vs-Close race produced a brief leak window
// where a goroutine could outlive Close by however long it took to observe
// db.ctx.Done() on its own.
//
// drainTimeout bounds how long the listener-path worker handoffs
// (Sync/Ingest replace, deleteSync/deleteIngest) wait for a cancelled worker
// to exit before abandoning it. Unbounded, a worker wedged in tx.Commit
// against an unreachable destination would park the single-threaded
// listener and stall the raft apply loop on its next config send. Defaults
// to closeDrainTimeout; tests override it to keep the wedged-worker cases
// fast.
type workerRegistry struct {
	mu           sync.Mutex
	sync         map[string]*workerHandle
	ingest       map[string]*workerHandle
	closed       bool
	drainTimeout time.Duration
}

func newWorkerRegistry(drainTimeout time.Duration) *workerRegistry {
	return &workerRegistry{
		sync:         make(map[string]*workerHandle),
		ingest:       make(map[string]*workerHandle),
		drainTimeout: drainTimeout,
	}
}

// workerHandle is the registry entry for a per-ID Sync or Ingest
// goroutine. cancel terminates the worker's context; done is closed
// by the worker itself just before it returns. Replace and Close
// both wait on done so they can guarantee the previous worker has
// fully exited (released its Reader, finished any in-flight Propose,
// returned from the user-supplied Sync/Ingest callback) before
// proceeding.
// readerRef boxes an ActualReader so workerHandle.activeReader (an
// atomic.Pointer) can hold-and-clear it: the worker goroutine publishes its
// reader on leadership gain and clears on loss, and the status path reads it
// concurrently for the opt-in readPosition diagnostic.
type readerRef struct{ r ActualReader }

type workerHandle struct {
	cancel context.CancelFunc
	// activeReader is the sync worker's live log reader while it owns the
	// syncable (nil while idle/non-owner). Status queries type-assert the
	// optional Position() capability on it — see DB.SyncableReadPosition.
	activeReader atomic.Pointer[readerRef]
	// ctx is the worker goroutine's context (the one cancel cancels). Retained so
	// a teardown path that must release the context node — notably the ingest
	// supervisor's restart, which drops a frozen handle whose goroutine exited via
	// ingestExitFreeze rather than a cancel — can cancel it, and so a test can
	// observe that it did.
	ctx  context.Context
	done chan struct{}
	// condemned marks a handle whose owner has begun tearing it down. A teardown
	// path (cancelIngestWorker, db.Ingest's replace loop) sets it under workersMu
	// BEFORE dropping the lock to drain, so the ingest supervisor's restart
	// preflight — which reacquires workersMu during that drain window — sees the
	// frozen handle is being removed and refuses to resurrect it. Without this,
	// the supervisor's `workers.ingest[id] != frozen` check passes during the
	// window (the map entry is deleted only after the relock), and it installs a
	// fresh worker on the SAME Ingestable instance that the teardown then Closes
	// out from under it. Guarded by the registry lock.
	condemned bool
	// closeResources runs this handle's resource release (its Syncable or
	// Ingestable Close) at most once, however many teardown paths reach it
	// concurrently (delete, reconcile, db.Close, replace). The Close contract
	// does not promise concurrency safety — a binlog syncer is not safe to Close
	// twice — so the drain-then-close helpers route the actual Close through it.
	closeResources sync.Once
	// stageRecoveryFolded/-Target publish a running stage-state
	// re-derivation's progress (target = the checkpoint being re-derived
	// to; 0 = no re-derivation active). Owner-local observability for
	// the status endpoint — the field finding: a re-deriving worker read
	// as plain "running" with silently climbing lag for ~30 minutes.
	stageRecoveryFolded atomic.Uint64
	stageRecoveryTarget atomic.Uint64
	// syncable is the parsed Syncable this worker runs, retained so the delete
	// path can reuse it as a teardown handle (e.g. DROP TABLE for a SQL
	// syncable) without re-parsing the config — which would re-run Init. It is
	// nil for ingest workers (deleteSync only reads it for syncables).
	syncable cluster.Syncable
	// ingestable is the parsed Ingestable this worker runs, retained so the
	// status path can ask it to decode its persisted position and query source
	// lag (IngestableStatus) without re-parsing. It is nil for syncable workers.
	ingestable cluster.Ingestable
}
