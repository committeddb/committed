package db

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// workerKind selects one of the registry's two tables.
type workerKind string

const (
	workerKindSync   workerKind = "sync"
	workerKindIngest workerKind = "ingest"
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

// table returns the handle table for kind. Callers hold mu.
func (r *workerRegistry) table(kind workerKind) map[string]*workerHandle {
	if kind == workerKindIngest {
		return r.ingest
	}
	return r.sync
}

// replace installs a fresh worker for id, first cancelling and draining any
// worker already registered there. spawn runs under the lock once the slot
// is empty (replaced reports whether it evicted anything) and returns the
// handle to register; onDrained runs OUTSIDE the lock for each evicted
// handle, after its bounded drain, with whether the drain completed — the
// caller's hook for kind-specific resource release. Returns ErrClosed if
// the registry is closed at entry or becomes closed during a drain.
//
// The loop is the concurrent-replace fix: the naive "if existing { cancel;
// wait; install }" races when two callers replace the same id — both
// observe the original entry, both wait on it, both install, and the
// loser's worker is orphaned (running but unreferenced, so no future
// replace can find it). Re-checking the slot after every wait converges:
// the entry we drained is deleted only if the slot still points at it
// (never a successor's), and an entry a concurrent caller slipped in is
// drained too. The lock is dropped only during the wait, so the exiting
// worker is never blocked by us; closed is re-checked after every
// reacquisition so a concurrent db.Close cannot be escaped by a late
// install.
//
// Condemn before dropping the lock to drain: an evicted handle with a
// pending supervisor (a frozen ingest worker) would otherwise be
// resurrected by a supervisor that reacquires the lock inside the drain
// window and still sees its handle registered — on the very instance
// onDrained is about to Close. The flag is what stops that; the map
// re-check alone cannot. See workerHandle.condemned.
func (r *workerRegistry) replace(kind workerKind, id string, spawn func(replaced bool) *workerHandle, onDrained func(prev *workerHandle, drained bool)) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	table := r.table(kind)
	replaced := false
	for {
		existing, ok := table[id]
		if !ok {
			break
		}
		replaced = true
		existing.condemned = true
		existing.cancel()
		r.mu.Unlock()
		drained := waitDone(existing.done, r.drainTimeout)
		onDrained(existing, drained)
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return ErrClosed
		}
		if table[id] == existing {
			delete(table, id)
		}
	}
	table[id] = spawn(replaced)
	r.mu.Unlock()
	return nil
}

// wedgePolicy is what remove does with a worker that did not exit within
// the drain timeout.
type wedgePolicy int

const (
	// abandonOnWedge deregisters the wedged worker and moves on — the
	// delete and reconcile-cancel paths. Its goroutine dies on process exit
	// and the sync re-applies idempotently on the next start.
	abandonOnWedge wedgePolicy = iota
	// keepOnWedge leaves the wedged worker REGISTERED — the rebuild and
	// re-materialize stops, which abort on a failed drain: a retry must
	// find (and re-check) the still-live worker rather than run a
	// checkpoint reset it could still defeat.
	keepOnWedge
)

// remove cancels and drains id's worker, if one is registered, and
// deregisters it. Returns the handle (nil when none was registered) and
// whether the drain completed within the timeout; on a wedge, policy
// decides whether the handle is deregistered anyway.
//
// The handle is condemned under the same hold that cancels it, before the
// lock is dropped to drain: a frozen ingest worker's supervisor may
// reacquire the lock inside the drain window and, seeing the not-yet-
// deleted entry still equal to its frozen handle, resurrect it on the same
// instance the caller is about to Close — the condemned flag makes that
// preflight bail. The drain runs outside the lock and bounded, and the
// entry is deleted only if the slot still points at this handle (never a
// successor's). beforeRelock, if non-nil, runs inside the drain window
// just before the lock is reacquired — the test-only poise point the
// resurrection tests use.
func (r *workerRegistry) remove(kind workerKind, id string, policy wedgePolicy, beforeRelock func()) (handle *workerHandle, drained bool) {
	r.mu.Lock()
	table := r.table(kind)
	handle, ok := table[id]
	if !ok {
		r.mu.Unlock()
		return nil, true
	}
	handle.condemned = true
	handle.cancel()
	r.mu.Unlock()
	drained = waitDone(handle.done, r.drainTimeout)
	if !drained && policy == keepOnWedge {
		return handle, false
	}
	if beforeRelock != nil {
		beforeRelock()
	}
	r.mu.Lock()
	if table[id] == handle {
		delete(table, id)
	}
	r.mu.Unlock()
	return handle, drained
}

// swapIfCurrent replaces expect with a fresh worker in ONE lock hold, or
// does nothing: the registry must be open, expect must still be the handle
// registered for id, and expect must not be condemned. Returns whether the
// swap happened. This is the supervisor's restart — preflight and install
// without releasing the lock, so a concurrent user replace cannot slip a
// handle in between; the registry moves from {expect} to {spawned}
// atomically, and a replace that arrives after the unlock still wins the
// final state via replace's loop.
//
// expect's context is cancelled before it is dropped: a frozen worker
// returned normally (ingestExitFreeze, not a ctx cancel), so its context is
// still an un-cancelled child of the engine's; without this each restart
// leaks one context node until db.Close. Cancelling an already-exited
// worker is a harmless no-op that just releases the node. No drain: the
// caller is downstream of the worker's exit and its done is closed.
func (r *workerRegistry) swapIfCurrent(kind workerKind, id string, expect *workerHandle, spawn func() *workerHandle) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	table := r.table(kind)
	if r.closed || table[id] != expect || expect.condemned {
		return false
	}
	expect.cancel()
	table[id] = spawn()
	return true
}
