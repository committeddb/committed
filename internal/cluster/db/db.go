package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/metrics"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// ErrClosed is returned by db.Sync, db.Ingest, and db.Propose when the
// DB has been (or is being) closed. Callers can use errors.Is(err,
// db.ErrClosed) to distinguish a normal shutdown from other failures.
var ErrClosed = errors.New("db: closed")

type Peers map[uint64]string

type DB struct {
	CommitC     <-chan []byte
	ErrorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	raft        *Raft
	storage     Storage
	ctx         context.Context
	cancelSyncs context.CancelFunc
	parser      Parser
	leaderState *LeaderState

	// waiters maps request IDs (set in db.Propose) to channels that
	// receive nil after the proposal is applied. The raft Ready loop
	// dispatches to notifyApplied after each successful ApplyCommitted,
	// which looks up the waiter by RequestID and signals it. db.Propose
	// blocks on the waiter (or ctx.Done) so callers see read-after-write.
	waitersMu     sync.Mutex
	waiters       map[uint64]chan error
	nextRequestID atomic.Uint64

	// workersMu guards syncWorkers / ingestWorkers / closed. The two
	// maps key running per-ID worker goroutines (one per syncable /
	// ingestable ID), so that a second db.Sync / db.Ingest call for
	// the same ID cancels and replaces the existing worker instead of
	// spawning a duplicate that would race with the first over the
	// same Reader, Position, and proposeC slot. db.Close cancels every
	// entry and waits for the workers' done channels.
	//
	// closed is set to true by db.Close after it has drained the
	// registry. db.Sync / db.Ingest check it under workersMu and
	// reject installs with ErrClosed when set, so a late caller (e.g.,
	// listenForSyncables waking up after the drain has run) can't
	// spawn an unobserved worker that escapes the drain. Without this
	// flag, the spawn-vs-Close race produced a brief leak window
	// where a goroutine could outlive Close by however long it took
	// to observe db.ctx.Done() on its own.
	workersMu     sync.Mutex
	syncWorkers   map[string]*workerHandle
	ingestWorkers map[string]*workerHandle
	closed        bool

	logger  *zap.Logger
	metrics *metrics.Metrics
}

// workerHandle is the registry entry for a per-ID Sync or Ingest
// goroutine. cancel terminates the worker's context; done is closed
// by the worker itself just before it returns. Replace and Close
// both wait on done so they can guarantee the previous worker has
// fully exited (released its Reader, finished any in-flight Propose,
// returned from the user-supplied Sync/Ingest callback) before
// proceeding.
type workerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func New(id uint64, peers Peers, s Storage, p Parser, sync <-chan *SyncableWithID, ingest <-chan *IngestableWithID, opts ...Option) *DB {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	ctx, cancelSyncs := context.WithCancel(context.Background())

	db := &DB{
		proposeC:      proposeC,
		confChangeC:   confChangeC,
		storage:       s,
		ctx:           ctx,
		cancelSyncs:   cancelSyncs,
		parser:        p,
		waiters:       make(map[uint64]chan error),
		syncWorkers:   make(map[string]*workerHandle),
		ingestWorkers: make(map[string]*workerHandle),
		logger:        cfg.logger,
		metrics:       cfg.metrics,
	}

	// The applied notifier is wired into the raft Ready loop. After each
	// successful ApplyCommitted, raft.go calls db.notifyApplied with the
	// raw entry data so we can unmarshal once, look up the waiter by
	// p.RequestID, and signal it. This shape works uniformly for
	// wal.Storage (real apply) and testing.MemoryStorage (no-op apply) —
	// both go through the same raft.go iteration over rd.CommittedEntries.
	commitC, errorC, raft := newRaftWithOptions(id, rpeers, s, proposeC, confChangeC, db.notifyApplied, cfg.logger, cfg)

	db.CommitC = commitC
	db.ErrorC = errorC
	db.raft = raft
	db.leaderState = raft.leaderState

	// EatCommitC was previously the caller's responsibility — forgetting
	// to call it deadlocks the raft Ready loop on the unbuffered commitC
	// send. Now that the only legitimate use of CommitC was as a test
	// synchronization barrier (rendered redundant by blocking Propose),
	// it's safe to unconditionally drain. Tests that previously read
	// <-db.CommitC for sync now use blocking Propose instead.
	db.EatCommitC()

	go db.listenForSyncables(sync)
	go db.listenForIngestables(ingest)

	return db
}

// notifyApplied is invoked from the raft Ready loop after each successful
// ApplyCommitted. It unmarshals the entry, looks up any blocking
// db.Propose call by RequestID, and signals the waiter. Entries with
// RequestID == 0 are system-internal proposals (or pre-PR2 entries) that
// have no waiter; for those this is a no-op.
func (db *DB) notifyApplied(data []byte) {
	if data == nil {
		return
	}
	p := &cluster.Proposal{}
	if err := p.Unmarshal(data, db.storage); err != nil {
		// Defense-in-depth: ApplyCommitted also unmarshals the entry
		// and fatal-exits on failure, so reaching here with an
		// unmarshal error means the apply path succeeded on one
		// decode and this one failed — pathological, not expected.
		// Log and skip; Propose's caller will time out via ctx if
		// its waiter is never signaled.
		db.logger.Warn("notifyApplied unmarshal failed", zap.Error(err))
		return
	}
	if p.RequestID == 0 {
		return
	}
	db.waitersMu.Lock()
	ack, ok := db.waiters[p.RequestID]
	db.waitersMu.Unlock()
	if !ok {
		return
	}
	// Buffered channel of capacity 1 set up by Propose, so this never
	// blocks. We don't delete the waiter here — Propose's defer cleans
	// it up after the receive.
	select {
	case ack <- nil:
	default:
	}
}

// listenForSyncables forwards syncable registrations from the input
// channel to db.Sync. It exits cleanly when db.ctx is canceled (db.Close)
// or when the input channel is closed. The previous "for sync != nil"
// shape was a pre-PR3 typo: a non-nil channel reference never becomes
// nil, so the loop ran forever and the goroutine leaked across Close.
// PR3's tighter shutdown semantics surfaced the leak as a race against
// db.Sync's closed-flag check, so we fix the listener to terminate
// properly here.
//
// nil input channels are handled explicitly so test setups that pass
// nil don't waste a goroutine waiting on db.ctx.
func (db *DB) listenForSyncables(sync <-chan *SyncableWithID) {
	if sync == nil {
		return
	}
	for {
		select {
		case syncable, ok := <-sync:
			if !ok {
				return
			}
			db.Sync(context.Background(), syncable.ID, syncable.Syncable)
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *DB) listenForIngestables(ingest <-chan *IngestableWithID) {
	if ingest == nil {
		return
	}
	for {
		select {
		case ingestable, ok := <-ingest:
			if !ok {
				return
			}
			db.Ingest(context.Background(), ingestable.ID, ingestable.Ingestable)
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *DB) EatCommitC() {
	go func() {
		for range db.CommitC {
			db.logger.Debug("ate a commit")
		}
	}()
}

// Propose submits a proposal to raft and blocks until it has been applied
// to bucket state on this node, or until ctx is canceled. Callers that
// need read-after-write semantics (HTTP handlers chaining "create type,
// then immediately use it") get them for free: by the time Propose
// returns nil, db.storage.Type/Database/etc. will see the new entity.
//
// On ctx cancellation Propose returns ctx.Err() and the deferred waiter
// cleanup runs. The proposal may still be applied later (raft has already
// accepted it via the proposeC send) — the caller just no longer waits.
//
// System-internal proposals that don't need to wait for apply (ingest
// position bumps, syncable index bumps) should call proposeFireAndForget
// instead so they don't tie up a request ID + waiter slot.
func (db *DB) Propose(ctx context.Context, p *cluster.Proposal) error {
	start := time.Now()
	p.RequestID = db.nextRequestID.Add(1)

	ack := make(chan error, 1)
	db.waitersMu.Lock()
	db.waiters[p.RequestID] = ack
	db.waitersMu.Unlock()

	defer func() {
		db.waitersMu.Lock()
		delete(db.waiters, p.RequestID)
		db.waitersMu.Unlock()
	}()

	bs, err := p.Marshal()
	if err != nil {
		return err
	}

	kind := proposalKind(p)
	if db.metrics != nil {
		db.metrics.ProposalSubmitted(kind)
	}

	db.logger.Debug("proposing", zap.Uint64("requestID", p.RequestID), zap.Int("entities", len(p.Entities)))

	// db.ctx in the select handles the "db is shutting down" case so a
	// worker (e.g., the ingest goroutine) doesn't race against
	// db.Close's raft teardown and either panic on send or hang.
	select {
	case db.proposeC <- bs:
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}

	select {
	case err := <-ack:
		if db.metrics != nil {
			db.metrics.ProposalApplied(time.Since(start))
		}
		db.logger.Debug("proposal applied", zap.Uint64("requestID", p.RequestID))
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}
}

// proposalKind classifies a proposal as "user" or "config" based on
// whether its entities contain system types.
func proposalKind(p *cluster.Proposal) string {
	if len(p.Entities) > 0 && p.Entities[0].Type != nil && cluster.IsSystem(p.Entities[0].Type.ID) {
		return "config"
	}
	return "user"
}

// proposeFireAndForget marshals and enqueues a proposal without waiting
// for apply. Used by system-internal proposers (ingest worker, sync
// worker) where the caller doesn't care when the position/index bump
// becomes visible — only that it eventually does. The proposal's
// RequestID stays 0 so notifyApplied skips the waiter lookup entirely.
func (db *DB) proposeFireAndForget(ctx context.Context, p *cluster.Proposal) error {
	bs, err := p.Marshal()
	if err != nil {
		return err
	}
	select {
	case db.proposeC <- bs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}
}

func (db *DB) ProposeDeleteType(ctx context.Context, id string) error {
	deleteTypeEntity := cluster.NewDeleteTypeEntity(id)

	p := &cluster.Proposal{}
	p.Entities = append(p.Entities, deleteTypeEntity)

	return db.Propose(ctx, p)
}


// Close tears down the DB. Order matters:
//
//  1. Cancel db.ctx via cancelSyncs FIRST. Every worker's context is
//     derived from db.ctx (see db.Sync / db.Ingest), so this propagates
//     into every worker, every inner Ingest goroutine, and every
//     in-flight db.Propose / proposeFireAndForget select. Crucially,
//     proposeFireAndForget's select only watches db.ctx (not the
//     worker ctx), so if we drained workers BEFORE canceling db.ctx
//     and any worker were stuck in a position/index bump while raft
//     was wedged (quorum loss, slow Ready loop), the drain would hang
//     forever. Cancel-first guarantees workers can always reach exit.
//  2. Snapshot the worker registry under workersMu and wait on every
//     handle's done channel. By now the workers are already racing
//     toward exit (their contexts are canceled); the drain is just
//     waiting for them to finish unwinding so we know the user-supplied
//     Sync/Ingest callbacks have fully torn down before we touch raft.
//     We still call h.cancel() in the snapshot loop — it's a no-op
//     (the parent context already canceled the child), but explicit
//     and self-documenting.
//  3. Stop the raft layer, which signals closeC (telling serveChannels'
//     proposeC reader to exit) and waits for serveChannels to actually
//     stop. We do NOT close db.proposeC ourselves: the raft reader is
//     closeC-driven, and a worker that hasn't yet noticed db.ctx
//     cancellation could otherwise panic on send.
//
// db.Close is idempotent: cancelSyncs is a CancelFunc (safe to call
// multiple times), the registry drain is a no-op on the second call
// (the maps are empty after the first), and raft.Close uses sync.Once.
func (db *DB) Close() error {
	db.logger.Info("closing db")

	db.cancelSyncs()

	db.workersMu.Lock()
	db.closed = true
	handles := make([]*workerHandle, 0, len(db.ingestWorkers)+len(db.syncWorkers))
	for id, h := range db.ingestWorkers {
		handles = append(handles, h)
		h.cancel()
		delete(db.ingestWorkers, id)
	}
	for id, h := range db.syncWorkers {
		handles = append(handles, h)
		h.cancel()
		delete(db.syncWorkers, id)
	}
	db.workersMu.Unlock()

	for _, h := range handles {
		<-h.done
	}

	return db.raft.Close()
}

// proposeSyncableIndex bumps the persisted SyncableIndex for a syncable
// after a successful Sync. Called from the sync worker, which doesn't
// need to wait for apply — fire-and-forget keeps the worker loop tight.
//
// We use db.ctx (lifecycle) here even though a worker ctx is in scope.
// Post-PR3 the worker ctx is canceled by registry replace and by Close.
// On a replace, the worker is being torn down because a new syncable
// instance is taking over the same id; we still want this index bump
// to land so the replacement starts from the correct position. The
// Sync upsert is idempotent, so re-syncing the same proposal on the
// replacement worker is safe but wasteful — landing the bump avoids
// the waste. On Close, db.ctx is canceled too, so the proposeC send
// short-circuits and we lose the bump anyway; that's fine because
// recovery on the next process start will re-sync from whatever
// SyncableIndex DID land.
func (db *DB) proposeSyncableIndex(_ context.Context, i *cluster.SyncableIndex) error {
	entity, err := cluster.NewUpsertSyncableIndexEntity(i)
	if err != nil {
		return err
	}
	if db.metrics != nil {
		db.metrics.ProposalSubmitted("index")
	}
	return db.proposeFireAndForget(db.ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// proposeIngestablePosition bumps the persisted Position for an
// ingestable after the upstream source advances. Called from the
// ingest worker, which doesn't need to wait for apply —
// fire-and-forget keeps the worker loop tight.
//
// Uses db.ctx for the same reason as proposeSyncableIndex: a registry
// replace shouldn't drop the position bump (the upstream source
// emitted it; losing it means the replacement re-reads from the
// previous position and re-emits the same rows), and on Close the
// bump is naturally lost via db.ctx cancellation. Recovery rebuilds
// from whatever position DID land.
func (db *DB) proposeIngestablePosition(_ context.Context, p *cluster.IngestablePosition) error {
	entity, err := cluster.NewUpsertIngestablePositionEntity(p)
	if err != nil {
		return err
	}
	if db.metrics != nil {
		db.metrics.ProposalSubmitted("position")
	}
	return db.proposeFireAndForget(db.ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

func (db *DB) ID() uint64 {
	return db.raft.id
}

// Leader returns the raft node ID this DB believes is the current leader,
// or 0 if no leader is known. Pass-through to Raft.Leader, which reads
// through to etcd raft's Status() snapshot. Used by the /ready HTTP probe.
func (db *DB) Leader() uint64 {
	return db.raft.Leader()
}

// AppliedIndex returns the highest log index that has been fully applied
// to local application state. Pass-through to Storage.AppliedIndex. Used
// by the /ready HTTP probe to gate readiness on this node having caught
// up; a freshly-started node reports 0 until at least one entry applies.
func (db *DB) AppliedIndex() uint64 {
	return db.storage.AppliedIndex()
}
