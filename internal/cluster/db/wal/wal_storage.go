package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

var (
	ErrOutOfBounds     = errors.New("requested index is greater than last index")
	ErrTypeMissing     = errors.New("type not found")
	ErrDatabaseMissing = errors.New("database not found")
	ErrBucketMissing   = errors.New("key value bucket missing")
)

var (
	typeBucket               = []byte("types")
	databaseBucket           = []byte("databases")
	ingestableBucket         = []byte("ingestables")
	ingestablePositionBucket = []byte("ingestablePositions")
	// ingestSourceSeqBucket maps an ingestable id to the big-endian
	// uint64 highwater of the highest applied source sequence. Drives
	// effectively-once ingest dedup — see ingest_source_seq.go.
	ingestSourceSeqBucket = []byte("ingestSourceSeq")
	// eventTombstoneBucket records, per user-defined (topic) (type, key), the
	// raft indices at which it was delete-proposed. The RTBF scrubber consults
	// it to physically remove a subject's PII from the permanent event log. See
	// tombstone.go for the encoding and the determinism rationale.
	eventTombstoneBucket = []byte("eventTombstones")
	// memberAPIURLBucket maps a raft node id (8 big-endian bytes, the key
	// cluster.NodeAPIURLKey produces) to a marshaled cluster.NodeAPIURL — the
	// node's self-announced advertised HTTP API base URL. Written from the
	// apply path (handleNodeAPIURL) so the mapping replicates to every node and
	// rides along in snapshots; read by the leader-read proxy to resolve a
	// peer's (in particular the leader's) API address. See member_api_url.go
	// and raft-leader-read-proxy.md.
	memberAPIURLBucket = []byte("memberAPIURLs")
)

var (
	syncableBucket      = []byte("syncables")
	syncableIndexBucket = []byte("syncableIndexes")
	// syncableDeadLetterBucket holds one nested sub-bucket per syncable
	// id; each sub-bucket maps a big-endian raft index to a marshaled
	// cluster.SyncableDeadLetter. Written deterministically from the
	// apply path (handleSyncableDeadLetter) so the records replicate to
	// every node and ride along in snapshots. See dead_letter.go.
	syncableDeadLetterBucket = []byte("syncableDeadLetters")
	// syncableStuckBucket and syncableSkipRequestBucket each hold one entry
	// per syncable id (last-writer-wins): the worker's current "blocked on
	// index N" status and the operator's pending skip request. Replicated
	// metadata for the node-agnostic manual dead-letter flow; see stuck.go.
	syncableStuckBucket       = []byte("syncableStuck")
	syncableSkipRequestBucket = []byte("syncableSkipRequests")
	// typeMigrationDeadLetterBucket is the type-keyed twin of
	// syncableDeadLetterBucket: one nested sub-bucket per type id, mapping
	// a big-endian raft index to a marshaled cluster.TypeMigrationDeadLetter
	// (a runtime migration-program failure). Written deterministically from
	// the apply path (handleTypeMigrationDeadLetter). See
	// type_migration_dead_letter.go.
	typeMigrationDeadLetterBucket = []byte("typeMigrationDeadLetters")
)

// appliedIndexBucket holds a single key ("idx") whose value is the
// big-endian uint64 of the highest raft entry index that ApplyCommitted has
// fully applied. Persisted so that on restart the Ready loop's replay of
// already-applied committed entries is a no-op.
var (
	appliedIndexBucket = []byte("appliedIndex")
	appliedIndexKey    = []byte("idx")
)

// pendingScrubBucket holds the scrubber's durable state: the highest requested
// Scrub upper-bound ("bound") and the highest one this node has finished
// rewriting to ("completed"). handleScrub bumps "bound"; the background worker
// advances "completed" after a swap. Persisted so a pending scrub resumes after
// restart even though the replayed Scrub entry is skipped by ApplyCommitted's
// already-applied guard. See scrub.go.
var (
	pendingScrubBucket   = []byte("pendingScrub")
	pendingScrubBoundKey = []byte("bound")
	scrubCompletedKey    = []byte("completed")
)

// internalEntity binds a built-in entity type to the bucket it lives in and
// the handler that applies it. applyEntity dispatches by scanning this table
// (predicates are mutually exclusive, so first match wins), and the bucket set
// is derived from it — so adding a built-in entity is one row here plus its
// handler, not parallel edits to a dispatch switch AND a bucket list.
type internalEntity struct {
	is      func(string) bool
	name    string // for apply-error wrapping; matches the handler's name
	bucket  []byte
	handler func(*Storage, *cluster.Entity) error
}

var internalEntities = []internalEntity{
	{cluster.IsType, "handleType", typeBucket, (*Storage).handleType},
	{cluster.IsDatabase, "handleDatabase", databaseBucket, (*Storage).handleDatabase},
	{cluster.IsIngestable, "handleIngestable", ingestableBucket, (*Storage).handleIngestable},
	{cluster.IsSyncable, "handleSyncable", syncableBucket, (*Storage).handleSyncable},
	{cluster.IsSyncableIndex, "handleSyncableIndex", syncableIndexBucket, (*Storage).handleSyncableIndex},
	{cluster.IsSyncableDeadLetter, "handleSyncableDeadLetter", syncableDeadLetterBucket, (*Storage).handleSyncableDeadLetter},
	{cluster.IsTypeMigrationDeadLetter, "handleTypeMigrationDeadLetter", typeMigrationDeadLetterBucket, (*Storage).handleTypeMigrationDeadLetter},
	{cluster.IsSyncableStuck, "handleSyncableStuck", syncableStuckBucket, (*Storage).handleSyncableStuck},
	{cluster.IsSyncableSkipRequest, "handleSyncableSkipRequest", syncableSkipRequestBucket, (*Storage).handleSyncableSkipRequest},
	{cluster.IsIngestablePosition, "saveIngestablePosition", ingestablePositionBucket, (*Storage).saveIngestablePosition},
	{cluster.IsScrub, "handleScrub", pendingScrubBucket, (*Storage).handleScrub},
	{cluster.IsNodeAPIURL, "handleNodeAPIURL", memberAPIURLBucket, (*Storage).handleNodeAPIURL},
}

// buckets is every bbolt bucket Open must create: one per internal entity type
// (derived from internalEntities) plus the ones that aren't entity-driven — the
// ingest source-seq highwater and the applied-index marker.
var buckets = func() [][]byte {
	bs := make([][]byte, 0, len(internalEntities)+3)
	for _, ie := range internalEntities {
		bs = append(bs, ie.bucket)
	}
	return append(bs, ingestSourceSeqBucket, appliedIndexBucket, eventTombstoneBucket)
}()

type StateType int

const (
	HardState = 0
	Snapshot  = 1
)

type State struct {
	Type StateType
	Data []byte
}

type Storage struct {
	// raftLogDir is the on-disk directory tidwall/wal writes entry-log
	// segments into. Kept around so RaftLogApproxSize can stat the
	// segment files without poking through tidwall's internals.
	raftLogDir string
	// entryMu guards swaps of the EntryLog handle: resetEntryLogToSnapshot
	// (an in-place snapshot install) replaces the whole log with a fresh one.
	// entry() — the read chokepoint raft's node.run goroutine hits via
	// Entries/Term — takes it for read; the swap takes it for write. Writers
	// (appendEntries, Compact) run on the same serveChannels goroutine as the
	// swap, so they cannot race it and stay lock-free.
	entryMu  sync.RWMutex
	EntryLog *wal.Log
	// eventLog is the permanent event log — the "forever" tier described
	// in docs/event-log-architecture.md. ApplyCommitted mirrors every
	// committed raft entry into it, keyed by raft index. Unlike EntryLog,
	// eventLog is never compacted: it is the canonical store that
	// syncables read from, and the one a rebuild rsync covers.
	//
	// Unexported so the raft-index ↔ wal-seq mapping (maintained by
	// appendEvent + the in-memory eventIndex / firstEventIndex atomics)
	// can't be bypassed by an outside caller writing directly. Access
	// goes through appendEvent / readEventAt / EventIndex /
	// firstEventSeq / lastEventSeq.
	eventLog *wal.Log
	// eventLogDir is the on-disk directory for eventLog (<datadir>/events).
	// The scrubber rewrites the event log by building a sibling directory and
	// renaming it over this one, so it needs the path (tidwall/wal doesn't
	// expose it). Set once in Open and never changed.
	eventLogDir string
	// eventMu guards the s.eventLog handle pointer and the on-disk events/
	// directory identity against the scrubber's swap (close → rename → reopen),
	// exactly mirroring how kvMu guards the bbolt handle against RestoreSnapshot.
	// Every event-log read/append takes eventMu.RLock (readEventAt,
	// firstEventSeq, lastEventSeq, appendEvent); the scrub swap takes
	// eventMu.Lock so it waits for in-flight readers/the appender to drain
	// before closing the handle. tidwall/wal serializes concurrent read/write
	// internally, so RLock holders don't need to exclude each other — only the
	// swap is exclusive. Lock order is eventMu → kvMu (the swap clears bbolt
	// scrub state outside the eventMu section, so the two never nest).
	eventMu sync.RWMutex
	// scrubSignal pokes the background scrubWorker that a Scrub command has
	// recorded a new pending bound (buffered cap 1: a poke that arrives while
	// one is queued is coalesced — the worker re-reads the latest bound from
	// bbolt anyway). scrubStop is closed by Close to stop the worker;
	// scrubDone is closed by the worker on exit so Close can wait for it.
	scrubSignal   chan struct{}
	scrubStop     chan struct{}
	scrubDone     chan struct{}
	scrubStopOnce sync.Once
	// scrubGen increments on every completed scrub swap. A scrub re-densifies
	// the wal sequence numbers, so a Reader's cached walSeq cursor becomes
	// stale across a swap. Readers compare this against the generation they
	// last resolved at and, on a change, re-derive walSeq from their (stable,
	// never-renumbered) raft-index position. Bumped under eventMu.Lock by the
	// scrub worker; read by Readers under eventMu.RLock.
	scrubGen atomic.Uint64
	// lastScrubbedBound mirrors the persisted "completed" scrub bound (the
	// highest Scrub upper-bound this node has finished rewriting to). Loaded in
	// Open, advanced by the worker; read by the automatic-scrub scheduler.
	lastScrubbedBound atomic.Uint64
	// metadataBacklog counts system-tombstonable (internal-snapshot) entities
	// applied since the last completed scrub — a cheap, in-memory proxy for
	// "superseded metadata is waiting to be GC'd." HasScrubBacklog consults it so
	// a metadata-heavy, RTBF-free cluster still triggers the automatic scrubber
	// (see metadata-gc-scrubber). Non-durable and leader-local by design: only
	// the leader proposes, the rewrite's removal set is what must be
	// deterministic, and a restart simply re-accumulates from the still-present
	// log. Reset on scrub completion (markScrubComplete).
	metadataBacklog atomic.Int64
	// StateLog persists raft's HardState and snapshot. Records are written
	// only when the state actually changes, and appendState truncates behind
	// the newest Snapshot + HardState records, so the log holds a handful of
	// records, not history (see appendState).
	//
	// Deliberately NOT in bbolt: bbolt holds replicated state and
	// RestoreSnapshot swaps the whole file for the leader's copy, while the
	// hard state is node-LOCAL — letting a snapshot install replace this
	// node's term/vote could make it vote twice in one term. The snapshot
	// record's Data is itself the serialized bbolt file, which would make
	// storing it there circular besides.
	StateLog        *wal.Log
	keyValueStorage *bolt.DB
	// kvMu guards swaps of keyValueStorage (RestoreSnapshot replaces the
	// bbolt handle wholesale: close → rename file → reopen → reassign).
	// Every bbolt transaction routes through view / update, which take
	// kvMu.RLock; RestoreSnapshot takes kvMu.Lock so it waits for
	// in-flight reads to drain before closing the handle and blocks new
	// reads until the reopen finishes. Without this an HTTP query that
	// happened to run mid-restore would hit a closed-file error.
	kvMu sync.RWMutex
	// snapMu guards snapshot and hardState. Both are written from the
	// raft serveChannels goroutine (Save, ConfState, CreateSnapshot,
	// RestoreSnapshot) and read from the etcd raft node.run goroutine
	// (Snapshot, called via raftLog.snapshot → maybeSendAppend whenever
	// the leader needs to ship a snapshot to a lagging follower).
	// Without serialization, the race detector flags the cross-
	// goroutine read/write of s.snapshot during severe-lag scenarios.
	snapMu    sync.RWMutex
	snapshot  *pb.Snapshot
	hardState *pb.HardState
	// firstIndex and lastIndex are mutated only from the raft serveChannels
	// goroutine (via appendEntries and Compact) but read from many other
	// goroutines: sync workers (Reader.Read), ingest workers, HTTP handlers
	// (db.ents()), and tests. Atomics make those reads race-free without
	// having to thread a mutex through every caller. The single-writer
	// invariant means no compare-and-swap is needed.
	//
	// firstIndex semantics: the raft index that maps to wal sequence 1
	// under the invariant seq = raftIndex - firstIndex + 1. Set once on
	// the first Save and NEVER updated after that, so the seq mapping
	// stays stable across compactions. tidwall/wal preserves absolute
	// sequence numbers across TruncateFront, so the formula continues to
	// work against the surviving portion of the wal even after the low
	// end has been truncated. (Compact moves the COMPACTION BOUNDARY,
	// tracked in compactedUpTo below; it does not re-base the seq
	// mapping.)
	firstIndex atomic.Uint64
	lastIndex  atomic.Uint64
	// compactedUpTo is the highest raft index that has been compacted
	// away on this node's raft log (conceptually the "dummy" entry at
	// the current wal first-seq — readable via Term for the match
	// check, not via Entries). Zero means no compaction has happened
	// on this storage. Mutated only from the raft serveChannels
	// goroutine (via Compact) so single-writer atomic semantics apply.
	//
	// Why separate from firstIndex: before this split, firstIndex was
	// overloaded to mean BOTH the seq-mapping offset AND the compaction
	// boundary. Compact updated it to the compact index, which broke
	// the seq mapping for future reads and writes (see the function-
	// level comment on firstIndex). Splitting them lets each carry a
	// single, invariant responsibility.
	compactedUpTo atomic.Uint64
	stateIndex    uint64
	// snapDirty marks that s.snapshot changed — a CreateSnapshot, an adopted
	// InstallSnapshot, or a ConfState update — since the last Snapshot record
	// was appended to the state log. Save consumes it: the snapshot is
	// persisted only when dirty, NOT on every Ready. Persisting it per Ready
	// is the write-amplification bug that filled disks: each record carries
	// the entire serialized bbolt database, and message-only Readys
	// (heartbeats, pre-vote rounds) fire constantly. Guarded by snapMu like
	// the snapshot it describes.
	snapDirty bool
	// lastSnapSeq / lastHardStateSeq are the state-log seqs of the newest
	// Snapshot record and the newest non-empty HardState record. Recovery
	// (getLastStates) only ever needs those two records, so appendState
	// truncates the log to the older of the two after each write — the state
	// log stays a handful of records instead of growing forever. Mutated only
	// from the raft serveChannels goroutine (via Save) and Open, like
	// stateIndex.
	lastSnapSeq      uint64
	lastHardStateSeq uint64
	// hardStateBytesSinceSnap accumulates the bytes of HardState records
	// written since lastSnapSeq. When they outgrow one snapshot record
	// (lastSnapBytes, floored at stateLogReanchorFloor), appendState
	// re-appends the snapshot at the tail so the truncation cut can advance
	// past the aging snapshot record — bounding the log at ~2 snapshot
	// copies even when compaction (the usual snapshot refresher) is disabled.
	hardStateBytesSinceSnap int
	lastSnapBytes           int
	databases               map[string]cluster.Database
	parser                  db.Parser
	sync                    chan<- *db.SyncableWithID
	ingest                  chan<- *db.IngestableWithID

	// configErrMu guards configErrors. configErrors records, per
	// "kind/id" (e.g. "database/orders"), the most recent failure to
	// BUILD a config's live object on THIS node — a node-local condition
	// (a missing ${VAR} secret, a parse error) that must not fail the
	// deterministic apply. The raw config bytes are always persisted;
	// only the local construction is deferred. A successful build clears
	// the entry. db.DB reads the count via ConfigBuildErrorCount to emit
	// a gauge. See secrets.go / saveDatabase for the rationale.
	configErrMu  sync.Mutex
	configErrors map[string]error
	// appliedIndex is the highest raft entry index that ApplyCommitted has
	// fully applied. Bumped after each successful per-entry apply (and
	// persisted to bbolt in the same step). Loaded from bbolt on Open.
	//
	// This is "R_local" in the storage invariant P_local == R_local.
	appliedIndex atomic.Uint64
	// eventIndex is the highest raft index written to EventLog. Bumped
	// before appliedIndex in ApplyCommitted so that a crash between the
	// EventLog write and the bbolt appliedIndex persist doesn't lose
	// events: on restart, appliedIndex is <= eventIndex, replay re-runs
	// ApplyCommitted for the gap, and the crash-window guard skips the
	// duplicate EventLog write.
	//
	// This is "P_local" in the storage invariant P_local == R_local.
	eventIndex atomic.Uint64
	// firstEventIndex is the raft index of the first entry in EventLog. 0
	// means the event log is empty. It is NOT used for an arithmetic seq
	// mapping: the Reader binary-searches the (ascending) raft-index column
	// (resolveStartSeq / ActualAt), which tolerates the gaps a scrub leaves
	// behind. firstEventIndex is maintained for Open recovery and diagnostics
	// — stamped on the first append, and recomputed from the first surviving
	// entry after a scrub rewrite (which can raise it past 1). Mutated only
	// from the serveChannels goroutine (first write) and the scrub worker
	// (under eventMu.Lock), so the atomic is for race-free reads, not CAS.
	firstEventIndex atomic.Uint64
	// dataEventIndex is the highest raft index of a user-topic-DATA entry
	// applied on this node — the head the per-syncable reader converges to
	// at EOF, since the reader skips committed's internal entries (config +
	// all coordination: index bumps, dead-letters, stuck/skip, positions,
	// scrub, …). It is the reference for per-syncable lag: lag = max(0,
	// dataEventIndex − checkpoint), which is 0 exactly when a worker is
	// genuinely caught up (see SyncableProgress / GET /syncable/{id}/status).
	// Distinct from eventIndex, which counts ALL applied entries (internal
	// included) and so would show a permanent phantom backlog for an idle
	// syncable.
	//
	// Bumped in ApplyCommitted with the same IsInternal filter the reader
	// uses, so the two agree by construction. Monotonic (entries apply in
	// index order). Mutated only from the serveChannels goroutine
	// (ApplyCommitted) and Open; the atomic is for race-free reads from the
	// HTTP status path on other goroutines, not CAS.
	dataEventIndex atomic.Uint64

	logger *zap.Logger
	// metrics drives the committed.wal.corrupt_entries counter, bumped by
	// recordCorrupt when a per-entry checksum verification fails on read.
	// Nil when metrics are disabled (no OTel endpoint); every use is
	// nil-guarded, so there is zero overhead in that case.
	metrics *metrics.Metrics
	// lostCallback is invoked from appendEntries after a higher-term
	// leader's AppendEntries truncates uncommitted tail entries this node
	// held, with the non-zero RequestIDs those entries carried. db.New
	// wires db.notifyLost here (via SetLostNotifier) so blocking
	// db.Propose waiters get the definitive ErrProposalLost. nil disables
	// detection. Single-writer: set once at Open (WithLostCallback) or by
	// SetLostNotifier before the raft serveChannels goroutine — the sole
	// appendEntries caller — starts, so reads in appendEntries need no
	// lock. See SetLostNotifier.
	lostCallback func([]uint64)
}

// Returns a *WalStorage, whether this storage existed already, or an error
// func Open() (*WalStorage, bool, error) {
func Open(dir string, p db.Parser, sync chan<- *db.SyncableWithID, ingest chan<- *db.IngestableWithID, opts ...Option) (*Storage, error) {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	entryLogDir := filepath.Join(dir, "raft", "log")
	stateLogDir := filepath.Join(dir, "raft", "state")
	eventLogDir := filepath.Join(dir, "events")
	keyValueStorageDir := filepath.Join(dir, "metadata")

	// Recover from a scrub that crashed mid-rewrite or mid-swap BEFORE the
	// MkdirAll below — otherwise, if a swap had renamed events/ out of the way,
	// MkdirAll would recreate an empty events/ and we'd open an empty event log
	// (data loss / storage-invariant violation). recoverScrubDirs rolls an
	// interrupted swap back to the pre-swap directory and removes temp/leftover
	// dirs; the persisted pending bound re-drives the rewrite once the worker
	// starts. See scrub.go.
	if err := recoverScrubDirs(dir); err != nil {
		return nil, fmt.Errorf("recover scrub dirs: %w", err)
	}

	// An in-place snapshot install (resetEntryLogToSnapshot) renames the old
	// entry log dir aside before recreating it; a crash can leave the renamed
	// dir behind. Remove it before opening — it's discarded consensus
	// transport, never data (the permanent event log is untouched by resets).
	if err := os.RemoveAll(entryLogDiscardDir(entryLogDir)); err != nil {
		return nil, fmt.Errorf("remove discarded entry log: %w", err)
	}

	// 0700: WAL directories hold raft state and committed log entries;
	// only the owning process needs access. os.ModePerm (0777) is too
	// permissive for data storage.
	for _, d := range []string{entryLogDir, stateLogDir, eventLogDir, keyValueStorageDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}

	entryLog, err := wal.Open(entryLogDir, nil)
	if err != nil {
		return nil, err
	}
	stateLog, err := wal.Open(stateLogDir, nil)
	if err != nil {
		return nil, err
	}
	eventLog, err := wal.Open(eventLogDir, nil)
	if err != nil {
		return nil, err
	}

	// bbolt is excluded from the per-entry WAL checksums below: it is a
	// B+tree with built-in page-level checksums (meta-page CRC64 + per-page
	// validation), so torn or bitflipped pages are already detected on read.
	boltOpts := &bolt.Options{Timeout: 1 * time.Second, NoSync: cfg.fsyncDisabled}
	keyValueStorage, err := bolt.Open(filepath.Join(keyValueStorageDir, "bbolt.db"), 0o600, boltOpts)
	if err != nil {
		return nil, err
	}

	err = keyValueStorage.Update(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Migrate flat key=value entries in resource buckets to the versioned
	// nested-bucket layout. Idempotent: a no-op if already migrated.
	err = keyValueStorage.Update(func(tx *bolt.Tx) error {
		return migrateToVersionedBuckets(tx)
	})
	if err != nil {
		return nil, fmt.Errorf("migrate to versioned buckets: %w", err)
	}

	logger := cfg.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	dbs := make(map[string]cluster.Database)
	ws := &Storage{
		raftLogDir:      entryLogDir,
		EntryLog:        entryLog,
		eventLog:        eventLog,
		eventLogDir:     eventLogDir,
		StateLog:        stateLog,
		keyValueStorage: keyValueStorage,
		databases:       dbs,
		parser:          p,
		sync:            sync,
		ingest:          ingest,
		logger:          logger,
		configErrors:    make(map[string]error),
		metrics:         cfg.metrics,
		lostCallback:    cfg.lostCallback,
		scrubSignal:     make(chan struct{}, 1),
		scrubStop:       make(chan struct{}),
		scrubDone:       make(chan struct{}),
	}

	fi, err := entryLog.FirstIndex()
	if err != nil {
		return nil, err
	}

	if fi > 0 {
		// Recover firstIndex (stable seq-mapping offset) and
		// compactedUpTo (current compaction boundary) from the wal
		// state: the entry at wal seq `fi` is by construction the one
		// whose raft index equals the current compaction boundary
		// (seq 1 pre-compaction, or the new first-seq after a Compact
		// moved it forward). Inverting the seq-mapping formula
		// seq = raftIndex - firstIndex + 1 gives
		// firstIndex = raftIndex - seq + 1, which is valid whether
		// seq 1 still physically exists or not — tidwall/wal preserves
		// absolute seq numbers across TruncateFront, so a post-compact
		// read of seq `fi` returns the same bytes it would have
		// returned pre-compact.
		fe, _, err := ws.entry(fi)
		if err != nil {
			return nil, err
		}
		ws.firstIndex.Store(fe.GetIndex() - fi + 1)
		ws.compactedUpTo.Store(fe.GetIndex())
	}

	li, err := entryLog.LastIndex()
	if err != nil {
		return nil, err
	}
	ws.lastIndex.Store(li)

	if li > 0 {
		le, _, err := ws.entry(li)
		if err != nil {
			return nil, err
		}
		ws.lastIndex.Store(le.GetIndex())
	}

	li, err = stateLog.LastIndex()
	if err != nil {
		return nil, err
	}
	ws.stateIndex = li

	st, snap, err := ws.getLastStates(ws.stateIndex)
	if err != nil {
		return nil, err
	}
	ws.hardState = st
	ws.snapshot = snap

	// Complete an in-place snapshot install that crashed between persisting
	// the snapshot record (saveWithSnapshot's appendState) and cutting the
	// entry log over. The persisted snapshot is the durable intent: if the
	// entry log doesn't reach the snapshot point, raft's RestartNode would
	// panic on a hard-state commit past lastIndex, so the cut-over is re-run.
	if err := ws.reconcileEntryLogWithSnapshot(); err != nil {
		return nil, fmt.Errorf("reconcile entry log with snapshot: %w", err)
	}

	// Rewrite the recovered state as fresh records at the state-log tail,
	// then truncate everything older (appendState does both). Recovery above
	// only ever uses the newest Snapshot and the newest non-empty HardState
	// record, so the rest of the log is dead weight — and on data dirs
	// written by the pre-truncation code (which appended the full snapshot on
	// every Ready) it can be tens of GB. One snapshot-sized write per boot
	// buys the reclaim.
	if ws.stateIndex > 0 {
		if err := ws.appendState(ws.hardState, ws.snapshot, !raft.IsEmptyHardState(ws.hardState), true); err != nil {
			return nil, fmt.Errorf("compact state log: %w", err)
		}
	}

	// Fail fast if any persisted config templates a secret env var this
	// node is missing, before opening database connections or spawning
	// workers below. See validateConfigSecrets for why missing secrets
	// are fatal while other parse errors are left to the paths below.
	if err = ws.validateConfigSecrets(); err != nil {
		return nil, err
	}

	err = ws.loadDatabases()
	if err != nil {
		return nil, err
	}

	// NOTE: ingestable workers for configs applied in a previous run are
	// restored by the caller via RestoreIngestableWorkers AFTER it has wired
	// up the ingestable sub-parsers and started the channel consumer — NOT
	// here. Open used to `go ws.restoreIngestableWorkers()` directly, but that
	// goroutine raced the caller's parser registration (and the parser map
	// write), silently skipping every ingestable on a loaded machine. See the
	// ordering contract on RestoreIngestableWorkers. (ApplyCommitted's
	// idempotency guard means re-replay on restart does NOT re-call
	// handleIngestable, so this explicit restore step is what makes
	// restart-resume work end-to-end, together with the position persistence
	// in ingestable_position.go.)

	idx, err := ws.loadAppliedIndex()
	if err != nil {
		return nil, err
	}
	ws.appliedIndex.Store(idx)

	// eventIndex (P_local) is the raft index of the last entry durably
	// written to EventLog. EventLog's own sequence numbers are
	// monotonic-from-1 and don't match raft index, so we recover the
	// raft index by unmarshaling the last stored entry. Empty EventLog
	// (fresh install or pre-Phase-1 migration) leaves eventIndex at 0.
	evLast, err := eventLog.LastIndex()
	if err != nil {
		return nil, err
	}
	if evLast > 0 {
		// Reads go through ws.readEventAt so the checksum verify happens at
		// the single chokepoint; a corrupt boundary entry fails Open here
		// (node fatal-exits with the ErrCorruptEntry message → rebuild.md).
		data, err := ws.readEventAt(evLast)
		if err != nil {
			return nil, fmt.Errorf("event log read last entry: %w", err)
		}
		last := &pb.Entry{}
		if err := proto.Unmarshal(data, last); err != nil {
			return nil, fmt.Errorf("event log unmarshal last entry: %w", err)
		}
		ws.eventIndex.Store(last.GetIndex())

		// Read the first entry to initialize firstEventIndex so
		// Reader.Read can map raft index ↔ wal seq.
		evFirst, err := eventLog.FirstIndex()
		if err != nil {
			return nil, err
		}
		firstData, err := ws.readEventAt(evFirst)
		if err != nil {
			return nil, fmt.Errorf("event log read first entry: %w", err)
		}
		first := &pb.Entry{}
		if err := proto.Unmarshal(firstData, first); err != nil {
			return nil, fmt.Errorf("event log unmarshal first entry: %w", err)
		}
		ws.firstEventIndex.Store(first.GetIndex())

		// Derive dataEventIndex (the head for per-syncable lag) by scanning
		// the event log backward from the tail to the first user-topic-data
		// entry, mirroring the reader's IsInternal filter. ApplyCommitted
		// skips already-applied entries on restart, so this Open-time
		// derivation — not replay — is what sets the head for the existing
		// log (and it is the only source on an rsync-restored node, which
		// never replays apply). The trailing internal run (index bumps,
		// dead-letters, config, positions) is normally tiny; cap the scan so
		// a pathological tail can't make Open O(log). On the cap, or any
		// read / decode failure, leave the head at 0: under-reporting it only
		// makes lag look smaller, never negative, and the next applied data
		// entry corrects it.
		const dataHeadBackscanCap = 4096
		for seq, scanned := evLast, 0; seq >= evFirst && scanned < dataHeadBackscanCap; seq, scanned = seq-1, scanned+1 {
			raw, rerr := ws.readEventAt(seq)
			if rerr != nil {
				ws.logger.Warn("dataEventIndex backscan: read failed; leaving head at 0",
					zap.Uint64("seq", seq), zap.Error(rerr))
				break
			}
			e := &pb.Entry{}
			if uerr := proto.Unmarshal(raw, e); uerr != nil {
				ws.logger.Warn("dataEventIndex backscan: entry unmarshal failed; leaving head at 0",
					zap.Uint64("seq", seq), zap.Error(uerr))
				break
			}
			if e.GetType() != pb.EntryNormal || len(e.Data) == 0 {
				continue
			}
			typeID, ok, derr := cluster.FirstEntityTypeID(e.Data)
			if derr != nil {
				ws.logger.Warn("dataEventIndex backscan: proposal decode failed; leaving head at 0",
					zap.Uint64("seq", seq), zap.Uint64("index", e.GetIndex()), zap.Error(derr))
				break
			}
			if ok && !cluster.IsInternal(typeID) {
				ws.dataEventIndex.Store(e.GetIndex())
				break
			}
		}
	}

	// Load the highest completed scrub bound so the worker skips redundant
	// rewrites and the scheduler can see progress. Then start the background
	// scrubber: it immediately resumes any pending scrub (a Scrub command that
	// committed before a crash leaves a durable bound that outlives the
	// replay-skipped entry) and thereafter runs on each signal.
	completed, err := ws.loadScrubCompleted()
	if err != nil {
		return nil, err
	}
	ws.lastScrubbedBound.Store(completed)
	go ws.scrubWorker()

	return ws, nil
}

func (s *Storage) Close() error {
	var finalErr error

	// Stop the background scrubber before closing the event log it rewrites.
	// scrubStop signals it; scrubDone confirms it has returned (so no swap is
	// mid-flight when we close the handle below). Idempotent close of scrubStop
	// is handled in stopScrubWorker.
	s.stopScrubWorker()

	finalErr = s.EntryLog.Close()

	err := s.eventLog.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	err = s.StateLog.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	err = s.keyValueStorage.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	for _, db := range s.databases {
		err = db.Close()
		if err != nil && finalErr == nil {
			finalErr = err
		}
	}

	return finalErr
}

func (s *Storage) getLastStates(li uint64) (*pb.HardState, *pb.Snapshot, error) {
	st := &pb.HardState{}
	snap := &pb.Snapshot{
		Data: nil,
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{
				Voters:         []uint64{},
				Learners:       []uint64{},
				VotersOutgoing: []uint64{},
				LearnersNext:   []uint64{},
			},
		},
	}

	if li > 0 {
		stDone := false
		snapDone := false

		fi, err := s.StateLog.FirstIndex()
		if err != nil {
			return nil, nil, err
		}

		for i := li; i >= fi; i-- {
			e, err := s.state(i)
			if err != nil {
				return nil, nil, err
			}

			if e.Type == HardState && !stDone {
				hs := &pb.HardState{}
				if err := proto.Unmarshal(e.Data, hs); err != nil {
					return nil, nil, err
				}
				// Skip empty records: raft hands Save an empty HardState on
				// every Ready where it didn't change, and the code before
				// the per-Ready write fix persisted those as-is — so on a
				// legacy log the newest HardState record is usually empty
				// and the real term/vote lives further back. Adopting the
				// empty one would restart the node at Term 0 (taking the
				// StartNode bootstrap path) and forget its vote.
				if !raft.IsEmptyHardState(hs) {
					st = hs
					stDone = true
				}
			} else if e.Type == Snapshot && !snapDone {
				err = proto.Unmarshal(e.Data, snap)
				if err != nil {
					return nil, nil, err
				}
				snapDone = true
			}

			if stDone && snapDone {
				break
			}
		}
	}

	return st, snap, nil
}

// ConfState records the ConfState most recently produced by
// raft.Node.ApplyConfChange into the storage's in-memory snapshot
// metadata. Marking the snapshot dirty makes the next Save call persist
// it (with this ConfState) to the state log via appendState, so
// InitialState returns the correct voter set on restart.
//
// Called by the raft Ready loop from raft.go after each EntryConfChange
// apply. This is the write half of the "conf state must survive across
// restart" invariant; the read half is wal.Open reloading the last
// snapshot out of stateLog.
func (s *Storage) ConfState(c *pb.ConfState) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	// raft 3.7's Snapshot/SnapshotMetadata are pointer fields, so a zero-value
	// snapshot carries nil Metadata — initialize before storing the conf state.
	if s.snapshot == nil {
		s.snapshot = &pb.Snapshot{}
	}
	if s.snapshot.Metadata == nil {
		s.snapshot.Metadata = &pb.SnapshotMetadata{}
	}
	s.snapshot.Metadata.ConfState = c
	s.snapDirty = true
}

// Save persists raft state and entries durably. It does NOT apply entities
// to BoltDB / time series — that happens in ApplyCommitted, which raft.go
// calls separately on rd.CommittedEntries. Splitting these is important
// because the raft Ready loop hands Save the *to-persist* slice (rd.Entries),
// which on a multi-node follower may include uncommitted entries; applying
// them to bucket state before commit would diverge the cluster.
//
// Empty-value handling: per the etcd raft contract, rd.HardState and
// rd.Snapshot are EMPTY on every Ready where they didn't change — which is
// most of them (message-only Readys for heartbeats and elections carry
// neither). Neither empty value is adopted or persisted. Adopting an empty
// snap would destroy the ConfState that the conf-change apply path wrote via
// (*Storage).ConfState, which breaks raft.RestartNode's voter-set recovery;
// adopting an empty HardState would zero the in-memory term/vote, and
// persisting it would shadow the real record on recovery (see
// getLastStates). The snapshot is persisted only when snapDirty says it
// actually changed, so an unchanged-state Ready writes nothing at all —
// persisting the full snapshot (the entire serialized bbolt database) on
// every Ready is the write amplification that could fill a disk in hours
// under a crash loop.
//
// A Ready carrying a non-empty snapshot (an InstallSnapshot) is a log
// REPLACEMENT, not an append, and is handled by saveWithSnapshot: the
// entries that accompany it follow the snapshot index, not this node's
// existing entry log, so appending them here would break the seq mapping.
func (s *Storage) Save(st *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if !raft.IsEmptySnap(snap) {
		return s.saveWithSnapshot(st, ents, snap)
	}

	s.snapMu.Lock()
	if !raft.IsEmptyHardState(st) {
		// Store raft's HardState pointer directly: raft builds a fresh
		// HardState value per Ready and never mutates it after return, and
		// this code never mutates *s.hardState in place — so aliasing is safe
		// and avoids copying the (lock-bearing) protobuf struct by value.
		s.hardState = st
	}
	hardCopy := s.hardState
	snapCopy := s.snapshot
	snapDirty := s.snapDirty
	// Consumed here rather than after the write: a failed appendState stops
	// the node (see the Save error policy in raft.go's Ready loop), so
	// clearing early can't lose a snapshot.
	s.snapDirty = false
	s.snapMu.Unlock()

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	if err := s.appendState(hardCopy, snapCopy, !raft.IsEmptyHardState(st), snapDirty); err != nil {
		return fmt.Errorf("[wal.storage] appendState: %w", err)
	}

	return nil
}

// saveWithSnapshot handles the Ready that carries an InstallSnapshot. raft
// has already reset its in-memory log to the snapshot point; the entries in
// ents (if any — the leader's follow-up MsgApp can coalesce into the same
// Ready) start at snap index+1 and connect to NOTHING in this node's
// existing entry log. Before this path existed, appendEntries hit its
// contiguity guard on those entries and the node silently stopped
// replicating — the in-place snapshot catch-up path was broken.
//
// Severe lag is rejected here WITHOUT persisting or mutating anything: if
// the snapshot leaps past the permanent event log, this node must be
// rebuilt (processSnapshot fatal-exits with the rebuild message right after
// this returns). Persisting any of the Ready first would let a restart
// without the rsync rebuild come up with raft state ahead of the event log
// and silently leave a permanent gap in it; restarting with the
// pre-snapshot state instead makes the leader re-send the snapshot and the
// fatal re-fire until the operator runs the rebuild.
//
// On accept the order is: persist the snapshot+hard state (durable intent),
// cut the entry log over to a dummy at the snapshot point, then append the
// follow-up entries. A crash between the persist and the cut-over is
// completed at the next Open by reconcileEntryLogWithSnapshot.
func (s *Storage) saveWithSnapshot(st *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if snap.Metadata.GetIndex() > s.eventIndex.Load() {
		return nil
	}

	s.snapMu.Lock()
	if !raft.IsEmptyHardState(st) {
		s.hardState = st
	}
	// Clone, don't alias: raft's rd.Snapshot may point at its internal
	// unstable snapshot, and ConfState() mutates s.snapshot.Metadata in
	// place — so we must own this copy.
	s.snapshot = proto.Clone(snap).(*pb.Snapshot)
	// Consumed by the appendState call below, which always persists this
	// (newer) snapshot.
	s.snapDirty = false
	hardCopy := s.hardState
	s.snapMu.Unlock()

	if err := s.appendState(hardCopy, snap, !raft.IsEmptyHardState(st), true); err != nil {
		return fmt.Errorf("[wal.storage] appendState: %w", err)
	}

	if err := s.resetEntryLogToSnapshot(snap.Metadata.GetIndex(), snap.Metadata.GetTerm()); err != nil {
		return fmt.Errorf("[wal.storage] reset entry log: %w", err)
	}

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	return nil
}

// entryLogDiscardDir is where resetEntryLogToSnapshot renames the
// superseded entry log before recreating a fresh one. A crash can strand
// it; Open removes it before opening the live dir.
func entryLogDiscardDir(entryLogDir string) string {
	return entryLogDir + ".discarded"
}

// resetEntryLogToSnapshot replaces the entry log with a single dummy entry
// at the snapshot point — the durable analogue of etcd
// MemoryStorage.ApplySnapshot's ents = [{Index, Term}]. The dummy sits at
// the compaction boundary: unreadable through Entries, but it supplies
// Term(index) for raft's log-matching probe, and the seq mapping is rebased
// (firstIndex = index) so post-snapshot entries append contiguously after
// it. Discarding the old entries loses nothing durable: the entry log is
// consensus transport; the permanent event log is a separate store this
// function never touches.
//
// Not crash-atomic — close/rename/recreate are separate steps. The
// preceding appendState persisted the snapshot record, which Open treats as
// the durable intent: reconcileEntryLogWithSnapshot re-runs this cut-over
// if a crash lands anywhere inside it, and Open removes a stranded
// .discarded dir. An error mid-way leaves the handle unusable and is
// returned, which stops the node (the Save error posture); the next boot
// heals.
func (s *Storage) resetEntryLogToSnapshot(index, term uint64) error {
	dummy := pb.Entry{Index: &index, Term: &term}
	data, err := proto.Marshal(&dummy)
	if err != nil {
		return err
	}

	discard := entryLogDiscardDir(s.raftLogDir)
	if err := os.RemoveAll(discard); err != nil {
		return fmt.Errorf("clear stale discard dir: %w", err)
	}

	s.entryMu.Lock()
	defer s.entryMu.Unlock()

	if err := s.EntryLog.Close(); err != nil {
		return fmt.Errorf("close entry log: %w", err)
	}
	if err := os.Rename(s.raftLogDir, discard); err != nil {
		return fmt.Errorf("rename entry log dir: %w", err)
	}
	if err := os.MkdirAll(s.raftLogDir, 0o700); err != nil {
		return err
	}
	fresh, err := wal.Open(s.raftLogDir, nil)
	if err != nil {
		return fmt.Errorf("reopen entry log: %w", err)
	}
	if err := fresh.Write(1, frame(data)); err != nil {
		return fmt.Errorf("write snapshot dummy: %w", err)
	}
	s.EntryLog = fresh
	// Best effort — Open clears a leftover before the next boot's open.
	_ = os.RemoveAll(discard)

	s.firstIndex.Store(index)
	s.compactedUpTo.Store(index)
	s.lastIndex.Store(index)
	return nil
}

// reconcileEntryLogWithSnapshot completes an in-place snapshot install that
// crashed between saveWithSnapshot's appendState (snapshot persisted) and
// its resetEntryLogToSnapshot (entry log cut over). Called from Open after
// both logs are recovered. A node whose entry log already reaches the
// snapshot point — every normal node, since compaction keeps the log's tail
// well past the last snapshot — is left alone.
func (s *Storage) reconcileEntryLogWithSnapshot() error {
	snapIdx := s.snapshot.Metadata.GetIndex()
	if snapIdx == 0 || s.lastIndex.Load() >= snapIdx {
		return nil
	}
	return s.resetEntryLogToSnapshot(snapIdx, s.snapshot.Metadata.GetTerm())
}

// ApplyCommitted applies a single committed raft entry to application
// state. It is called by the raft Ready loop on each entry from
// rd.CommittedEntries, after Save has persisted the entry and before
// node.Advance(). Apply must complete before Advance per etcd-raft contract.
//
// ApplyCommitted is idempotent on re-apply: entries with index <=
// AppliedIndex are skipped. The applied index is bumped (and persisted to
// bbolt) after each successful apply, so a restart that replays committed
// entries through the Ready loop will skip the already-applied portion.
//
// It also mirrors the raw entry into the permanent EventLog — every
// committed entry, regardless of EntryType or entity kind, so EventLog's
// sequence number stays 1:1 with raft index. Readers filter at read time.
// This is what keeps the storage invariant P_local == R_local true under
// normal operation: both advance together per entry.
//
// Errors here are treated as fatal by raft.go; see the apply error policy
// comment in raft.go's Ready loop.
func (s *Storage) ApplyCommitted(entry *pb.Entry) error {
	// Skip already-applied entries (replay-on-restart safety). A bare
	// EntryNormal with nil data (e.g. the leader's post-election no-op
	// entry from raft) still counts as applied — we bump appliedIndex so
	// the Ready loop's invariant check stays P_local == R_local.
	if entry.GetIndex() <= s.appliedIndex.Load() {
		return nil
	}

	// Mirror the raw raft entry into the permanent event log. The guard
	// against entry.GetIndex() <= eventIndex covers the crash window where
	// appendEvent succeeded but saveAppliedIndex didn't persist:
	// without it, replay would try to append the same entry again and
	// diverge seq vs. raft index on disk.
	if entry.GetIndex() > s.eventIndex.Load() {
		if err := s.appendEvent(entry); err != nil {
			return fmt.Errorf("[wal.storage] %w", err)
		}
	}

	// For EntryNormal with data, apply entities to bucket state and the
	// time-series store. Other entry types (conf changes, no-op entries)
	// still count as "applied" for invariant purposes — we just have
	// nothing to do at the entity level.
	//
	// Unmarshal failures here are returned as errors (not swallowed):
	// the raft ready loop's apply-error policy fatal-exits the node,
	// which is the correct behavior. Swallowing would let appliedIndex
	// advance past an entry that wasn't applied, diverging replicas
	// (node A applies, node B skips) and silently dropping entities
	// from any future snapshot taken at that index.
	if entry.GetType() == pb.EntryNormal && entry.Data != nil {
		p := &cluster.Proposal{}
		if err := p.Unmarshal(entry.Data, s); err != nil {
			s.logger.Error("unmarshal committed proposal",
				zap.Uint64("index", entry.GetIndex()),
				zap.Stringer("entryType", entry.GetType()),
				zap.Error(err))
			return fmt.Errorf("[wal.storage] unmarshal proposal at index %d: %w", entry.GetIndex(), err)
		}

		// Advance the data-entry head iff this is user topic data — exactly
		// the filter the per-syncable reader applies in reader.go, so
		// dataEventIndex tracks the index the reader converges to at EOF.
		// Internal entries (committed's config + coordination) are skipped
		// here just as the reader skips them, so an idle syncable's lag
		// reads 0 rather than a phantom backlog of trailing internal
		// entries. Monotonic: entries apply in index order.
		if len(p.Entities) > 0 && !cluster.IsInternal(p.Entities[0].Type.ID) {
			s.dataEventIndex.Store(entry.GetIndex())
		}

		for _, entity := range p.Entities {
			s.logger.Debug("applying entity", zap.String("typeID", entity.Type.ID), zap.String("key", string(entity.Key)))
			if err := s.applyEntity(entity); err != nil {
				return err
			}

			// Record an event-log tombstone for a user-defined delete so a
			// future Scrub can physically remove this subject's PII from the
			// permanent event log. Built-in config/coordination deletes are not
			// PII and are never scrubbed. Deterministic: keyed on the committed
			// entity and stamped with this entry's raft index, so every replica
			// stores identical bytes.
			if isUserDefinedType(entity.Type.ID) && entity.IsDelete() {
				if err := s.recordEventTombstone(entity.Type.ID, entity.Key, entry.GetIndex()); err != nil {
					return fmt.Errorf("[wal.storage] recordEventTombstone: %w", err)
				}
			}

			// Count an applied EntityKindSnapshot entity as metadata-GC backlog so
			// the automatic scrubber fires on a Snapshot-heavy cluster even with no
			// RTBF deletes — covering both internal bookkeeping and user Snapshot
			// streams. (Revision configs and Standalone dead-letters are retained,
			// not counted.) This is a leader-local trigger heuristic, so the live
			// resolved kind is fine here; it over-counts distinct-key writes
			// slightly, which is harmless (see metadataBacklog).
			if entity.Type.EntityKind == cluster.EntityKindSnapshot {
				s.metadataBacklog.Add(1)
			}
		}

		// Advance the per-ingestable source-seq highwater for ingest
		// proposals. Deterministic across replicas (derived only from the
		// committed proposal) and done before saveAppliedIndex so a
		// replayed entry re-applies the idempotent max. Non-ingest
		// proposals carry "" / 0 and no-op here.
		if err := s.advanceIngestSourceSeq(p.IngestableID, p.SourceSeq); err != nil {
			return fmt.Errorf("[wal.storage] advanceIngestSourceSeq: %w", err)
		}
	}

	s.appliedIndex.Store(entry.GetIndex())
	return s.saveAppliedIndex(entry.GetIndex())
}

func (s *Storage) applyEntity(entity *cluster.Entity) error {
	for _, ie := range internalEntities {
		if ie.is(entity.ID) {
			if err := ie.handler(s, entity); err != nil {
				return fmt.Errorf("[wal.storage] %s: %w", ie.name, err)
			}
			return nil
		}
	}
	// User-defined (topic) entities have no apply-time side effect: their
	// durable record is the permanent event log, written in ApplyCommitted
	// before this entity-level dispatch runs.
	return nil
}

// AppliedIndex returns the highest raft entry index that ApplyCommitted has
// fully applied. Loaded from bbolt on Open and bumped after each successful
// apply. This is "R_local" in the storage invariant P_local == R_local.
func (s *Storage) AppliedIndex() uint64 {
	return s.appliedIndex.Load()
}

// EventIndex returns the highest raft entry index that has been durably
// written to the permanent event log on this node. This is "P_local" in
// the storage invariant P_local == R_local, which the Ready loop checks
// after every iteration (see raft.go's checkStorageInvariant). On a fresh
// or brand-new node eventLog starts empty and EventIndex is 0.
func (s *Storage) EventIndex() uint64 {
	return s.eventIndex.Load()
}

// DataEventIndex returns the highest raft index of a DATA (non-metadata)
// entry applied on this node — the head a per-syncable reader converges to
// at EOF. It is the reference for per-syncable lag (lag = max(0,
// DataEventIndex − checkpoint)); unlike EventIndex it excludes the
// syncable-metadata entries the reader skips, so a caught-up syncable reads
// lag 0 instead of a phantom backlog of trailing index bumps. O(1), local,
// and replicated-deterministic (every node applied through index i computes
// the same value). 0 on a node that has applied no data entries yet.
func (s *Storage) DataEventIndex() uint64 {
	return s.dataEventIndex.Load()
}

// RaftLogApproxSize returns the approximate on-disk size (bytes) of the
// raft log by summing tidwall/wal's segment file sizes under
// raft/log/. Called by the compaction trigger in raft.go to decide
// whether the "size > threshold" limb of the 10GB/1hr policy has been
// crossed. Approximate because tidwall/wal cycles segments, so a
// segment that's still being written may be sized below its final
// on-disk length by a few kB — close enough for a trigger decision.
// Errors walking the directory are returned as (0, err); callers
// treat that as "no trigger" rather than panicking.
func (s *Storage) RaftLogApproxSize() (uint64, error) {
	entries, err := os.ReadDir(s.raftLogDir)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			// File vanished between ReadDir and Info (e.g. a
			// concurrent cycle); skip, don't fail the whole sum.
			continue
		}
		sz := info.Size()
		if sz < 0 {
			// os.FileInfo.Size() is non-negative for regular files;
			// guard against exotic filesystems returning -1 for
			// unknown sizes rather than wrap into a giant uint64.
			continue
		}
		total += uint64(sz)
	}
	return total, nil
}

// firstEventSeq returns the wal sequence of the first entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
// Takes eventMu.RLock so it never observes the handle mid-swap.
func (s *Storage) firstEventSeq() (uint64, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.firstEventSeqLocked()
}

// firstEventSeqLocked is firstEventSeq without the lock; the caller must hold
// eventMu (R or W). Used by the scrub swap, which already holds eventMu.Lock
// and would deadlock re-acquiring RLock.
func (s *Storage) firstEventSeqLocked() (uint64, error) {
	return s.eventLog.FirstIndex()
}

// lastEventSeq returns the wal sequence of the last entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
func (s *Storage) lastEventSeq() (uint64, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.lastEventSeqLocked()
}

// lastEventSeqLocked is lastEventSeq without the lock; caller must hold eventMu.
func (s *Storage) lastEventSeqLocked() (uint64, error) {
	return s.eventLog.LastIndex()
}

// readEventAt reads the pb.Entry bytes at the given wal sequence from the
// permanent event log, verifying the per-entry checksum (see checksum.go).
// Package-internal; callers outside the storage tier should go through the
// Reader abstraction, which handles raft index ↔ wal seq translation and
// filters metadata entries.
func (s *Storage) readEventAt(seq uint64) ([]byte, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.readEventAtLocked(seq)
}

// readEventAtLocked is readEventAt without the lock; caller must hold eventMu.
func (s *Storage) readEventAtLocked(seq uint64) ([]byte, error) {
	raw, err := s.eventLog.Read(seq)
	if err != nil {
		return nil, err
	}
	return s.unframe(raw, "event_log")
}

// unframe verifies and strips the checksum frame from a raw log read,
// recording a corruption-counter sample (attributed to logName) before
// returning ErrCorruptEntry on a CRC mismatch. The metrics handle is
// nil-safe. Legacy un-checksummed entries pass through unchanged.
func (s *Storage) unframe(raw []byte, logName string) ([]byte, error) {
	payload, err := unframe(raw)
	if err != nil {
		s.recordCorrupt(logName)
		return nil, err
	}
	return payload, nil
}

// recordCorrupt bumps the corruption counter for logName when metrics are
// enabled. A nil *Metrics (metrics disabled) is a no-op.
func (s *Storage) recordCorrupt(logName string) {
	if s.metrics != nil {
		s.metrics.WalCorruptEntry(logName)
	}
}

// appendEvent writes a committed raft entry's raw bytes to the
// permanent event log, stamping firstEventIndex on the very first
// write and bumping eventIndex on success. Called only from
// ApplyCommitted, which already guards against writes at or below
// eventIndex (crash-window idempotence). Kept on Storage (not inlined
// into ApplyCommitted) so there's exactly one site that advances
// P_local.
func (s *Storage) appendEvent(entry *pb.Entry) error {
	// RLock for the whole body so the seq it computes (LastIndex+1) and the
	// Write that consumes it can't straddle a scrub swap that would replace the
	// handle underneath them. Shared with concurrent readers; only the swap
	// (eventMu.Lock) is excluded.
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	entryBytes, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry for event log: %w", err)
	}
	nextSeq, err := s.eventLog.LastIndex()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	nextSeq++
	if err := s.eventLog.Write(nextSeq, frame(entryBytes)); err != nil {
		return fmt.Errorf("event log write seq %d (raft index %d): %w", nextSeq, entry.GetIndex(), err)
	}
	if nextSeq == 1 {
		s.firstEventIndex.Store(entry.GetIndex())
	}
	s.eventIndex.Store(entry.GetIndex())
	return nil
}

// view runs fn in a bbolt read-only transaction while holding kvMu for
// read. Guards against RestoreSnapshot swapping the bbolt handle out
// from under an in-flight query.
func (s *Storage) view(fn func(tx *bolt.Tx) error) error {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	return s.keyValueStorage.View(fn)
}

// update runs fn in a bbolt read-write transaction while holding kvMu
// for read. Same concurrency shape as view: many concurrent update
// calls proceed in parallel (bbolt itself serializes writers) and
// RestoreSnapshot waits for them all to drain.
func (s *Storage) update(fn func(tx *bolt.Tx) error) error {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	return s.keyValueStorage.Update(fn)
}

// saveAppliedIndex persists the applied index to bbolt. Called from
// ApplyCommitted after a per-entry apply succeeds. Each apply runs in its
// own short bbolt transaction; this is acceptable because the per-entity
// handlers are not atomic with each other today either.
func (s *Storage) saveAppliedIndex(idx uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		return b.Put(appliedIndexKey, buf[:])
	})
}

// loadAppliedIndex reads the persisted applied index from bbolt, or returns
// 0 if no apply has happened yet (fresh storage).
func (s *Storage) loadAppliedIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get(appliedIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("appliedIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	})
	return idx, err
}

// SetLostNotifier installs the truncation lost-callback. db.New calls it
// (via the lostNotifierSetter optional interface) with db.notifyLost
// because the callback can't be passed at Open time — the DB it closes
// over doesn't exist yet. It MUST be called before the raft serveChannels
// goroutine starts (db wires it inside newRaftWithOptions, before
// startRaft), since appendEntries — the sole reader — runs only on that
// goroutine; setting it after would race. See the lostCallback field doc
// and WithLostCallback (the test-only construction-time equivalent).
func (s *Storage) SetLostNotifier(fn func([]uint64)) {
	s.lostCallback = fn
}

// collectTruncatedRequestIDs reads the entries about to be overwritten by a
// conflicting append — wal seqs [firstSeq, lastSeq] inclusive — and returns
// the set of non-zero cluster.Proposal RequestIDs they carry. These are the
// proposals physically present on this node that a higher-term leader is
// about to truncate before they committed; their waiters (if this node is
// also where Propose ran) get the definitive ErrProposalLost.
//
// Best-effort by design: an entry that fails to read or decode is logged and
// skipped — the truncation is already happening, so signalling the IDs we
// could decode beats signalling none. Config-change and empty (post-election
// no-op) entries carry no RequestID and contribute nothing.
func (s *Storage) collectTruncatedRequestIDs(firstSeq, lastSeq uint64) []uint64 {
	var ids []uint64
	for seq := firstSeq; seq <= lastSeq; seq++ {
		e, _, err := s.entry(seq)
		if err != nil {
			s.logger.Warn("truncation detection: read entry failed; skipping",
				zap.Uint64("seq", seq), zap.Error(err))
			continue
		}
		if e.GetType() != pb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		rid, err := cluster.RequestIDFromProposal(e.Data)
		if err != nil {
			s.logger.Warn("truncation detection: decode proposal failed; skipping",
				zap.Uint64("seq", seq), zap.Uint64("index", e.GetIndex()), zap.Error(err))
			continue
		}
		if rid != 0 {
			ids = append(ids, rid)
		}
	}
	return ids
}

func (s *Storage) appendEntries(ents []*pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	// Snapshot the current bounds. appendEntries is only ever invoked from
	// the raft serveChannels goroutine (the sole writer), so observing them
	// once at the top is sufficient — no other writer can race us.
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	// `first` is the lowest raft index the wal can accept an append at
	// — one past the current compacted-dummy (boundary()). Entries at
	// or below that are already compacted away and must be skipped, not
	// rewritten.
	first := s.boundary() + 1
	last := ents[0].GetIndex() + uint64(len(ents)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > ents[0].GetIndex() {
		ents = ents[first-ents[0].GetIndex():]
	}

	offset := ents[0].GetIndex() - firstIndex
	l := lastIndex - firstIndex + 1

	// Don't error when this is the first write
	if firstIndex > 0 && l < offset {
		return fmt.Errorf("missing log entry [last: %d, append at: %d]", lastIndex, firstIndex)
	}

	// We have received previous log entries a second time and/or have log entries newer than the ones being received
	// This can happen during leadership changes or because we wrote data that was later not accepted by consensus
	// Trust the new data over the old data
	if l > offset {
		// Before overwriting the conflicting tail, capture the RequestIDs
		// of any proposals it carries so the lost-callback can give their
		// blocking-Propose waiters the definitive ErrProposalLost. The
		// truncated entries occupy wal seqs offset+1..l (raft indexes
		// ents[0].GetIndex()..lastIndex) — the very entries TruncateBack drops.
		// Skipped unless a callback is registered (the only case that
		// cares) and there is a prior log to truncate, so the happy path
		// pays nothing.
		var lostIDs []uint64
		if s.lostCallback != nil && firstIndex > 0 {
			lostIDs = s.collectTruncatedRequestIDs(offset+1, l)
		}

		err := s.EntryLog.TruncateBack(offset)
		if err != nil {
			return err
		}

		// Signal only after the truncation actually executed, so a waiter
		// never sees ErrProposalLost for an entry that survived. The
		// callback (db.notifyLost) only does map lookups and non-blocking
		// channel sends, so calling it inline on the serveChannels
		// goroutine — exactly as notifyApplied is — is safe.
		if len(lostIDs) > 0 {
			s.lostCallback(lostIDs)
		}
	}

	// case len > offset:
	// 	// NB: full slice expression protects ms.ents at index >= offset from
	// 	// rewrites, as they may still be referenced from outside MemoryStorage.
	// 	ms.ents = append(ms.ents[:offset:offset], entries...)
	// case len == offset:
	// 	ms.ents = append(ms.ents, entries...)
	// default:
	// 	return fmt.Errorf("missing log entry [last: %d, append at: %d]", s.lastIndex, s.firstIndex)
	// }

	if firstIndex == 0 && lastIndex == 0 && ents != nil {
		firstIndex = ents[0].GetIndex()
		s.firstIndex.Store(firstIndex)
	}

	for _, e := range ents {
		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}

		i := e.GetIndex() - firstIndex + 1
		err = s.EntryLog.Write(i, frame(data))
		if err != nil {
			return fmt.Errorf("index %d to position %d: %w", e.GetIndex(), i, err)
		}
	}

	s.lastIndex.Store(ents[len(ents)-1].GetIndex())

	return nil
}

// stateLogReanchorFloor floors the re-anchor threshold in appendState so a
// small snapshot doesn't get re-appended every few HardState records. A var,
// not a const, only so tests can lower it to exercise re-anchoring without
// writing 64KB of records.
var stateLogReanchorFloor = 64 * 1024

// appendState persists raft state to the state log and truncates the log so
// it stays bounded. cur and snap are the CURRENT in-memory hard state and
// snapshot; hardChanged / snapChanged say whether this Ready actually changed
// them. Each is written only on change — recovery (getLastStates) needs only
// the newest Snapshot and the newest non-empty HardState record, and
// everything older than the older of those two is truncated away after the
// write. An unchanged-state call writes nothing.
//
// Two refinements keep the truncation cut moving:
//
//   - Writing the snapshot only on change lets HardState records pile up
//     behind an aging Snapshot record (the cut can't pass it). Once they
//     outgrow one snapshot copy, the snapshot is re-appended at the tail to
//     advance the cut — at most one extra snapshot write per snapshot-size
//     of hard-state churn, which matters when compaction (the usual
//     hourly snapshot refresher) is disabled.
//   - Symmetrically, a snapshot write re-appends the current hard state so
//     an old HardState record doesn't pin the cut the same way.
//
// The Snapshot record is written before the HardState record, matching the
// raft Ready contract's persist-the-snapshot-first ordering.
func (s *Storage) appendState(cur *pb.HardState, snap *pb.Snapshot, hardChanged, snapChanged bool) error {
	writeSnap := snapChanged
	if !writeSnap && hardChanged && s.lastSnapSeq > 0 &&
		s.hardStateBytesSinceSnap > max(s.lastSnapBytes, stateLogReanchorFloor) {
		writeSnap = true
	}
	writeHard := hardChanged || (writeSnap && !raft.IsEmptyHardState(cur))

	if !writeSnap && !writeHard {
		return nil
	}

	if writeSnap {
		data, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		n, err := s.writeStateRecord(Snapshot, data)
		if err != nil {
			return err
		}
		s.lastSnapSeq = s.stateIndex
		s.lastSnapBytes = n
		s.hardStateBytesSinceSnap = 0
	}

	if writeHard {
		data, err := proto.Marshal(cur)
		if err != nil {
			return err
		}
		n, err := s.writeStateRecord(HardState, data)
		if err != nil {
			return err
		}
		s.lastHardStateSeq = s.stateIndex
		s.hardStateBytesSinceSnap += n
	}

	cut := s.lastSnapSeq
	if s.lastHardStateSeq > 0 && (cut == 0 || s.lastHardStateSeq < cut) {
		cut = s.lastHardStateSeq
	}
	if cut > 0 {
		fi, err := s.StateLog.FirstIndex()
		if err == nil && fi > 0 && fi < cut {
			// Best-effort: a failed truncation costs disk, not correctness —
			// the records we just wrote are durable either way.
			if err := s.StateLog.TruncateFront(cut); err != nil {
				s.logger.Warn("truncate state log", zap.Uint64("cut", cut), zap.Error(err))
			}
		}
	}

	return nil
}

func (s *Storage) writeStateRecord(typ StateType, data []byte) (int, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(State{Type: typ, Data: data}); err != nil {
		return 0, err
	}

	framed := frame(buf.Bytes())
	s.stateIndex++
	if err := s.StateLog.Write(s.stateIndex, framed); err != nil {
		return 0, fmt.Errorf("position %d: %w", s.stateIndex, err)
	}
	return len(framed), nil
}

// InitialState returns the saved HardState and ConfState information.
func (s *Storage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	// raft 3.7's Storage contract: the returned ConfState must not be nil —
	// return an empty one when no snapshot ConfState has been persisted yet.
	cs := s.snapshot.GetMetadata().GetConfState()
	if cs == nil {
		cs = &pb.ConfState{}
	}
	// InitialState is read once at node start, before the serve loop can
	// mutate s.hardState, so returning the pointer directly is safe.
	return s.hardState, cs, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
//
// Boundary check uses boundary(), which returns the higher of
// firstIndex (the seq-mapping offset) and compactedUpTo (the
// current compaction boundary). On a fresh storage both equal the
// first written raft index, which correctly treats that entry as
// the compacted-dummy per existing wal.Storage semantics. On a
// post-compact storage, compactedUpTo dominates and the boundary
// moves with each Compact without disturbing the seq mapping.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	var totalSize uint64

	firstIndex := s.firstIndex.Load()
	if lo <= s.boundary() {
		return nil, raft.ErrCompacted
	}

	var es []*pb.Entry
	logIndex := lo - firstIndex
	for x := lo; x < hi; x++ {
		logIndex++
		e, size, err := s.entry(logIndex)
		if err != nil {
			return nil, err
		}

		totalSize += size
		if len(es) == 0 || totalSize <= maxSize {
			es = append(es, e)
		}
	}

	return es, nil
}

// boundary returns the higher of firstIndex (seq-mapping offset) and
// compactedUpTo (current compaction boundary). Used by Entries, Term,
// FirstIndex, and Compact to locate the "dummy" entry's raft index —
// the index below which data is unavailable (ErrCompacted).
//
// Split rationale (from firstIndex's doc): firstIndex is stable for
// seq mapping, compactedUpTo moves with each Compact. max() collapses
// them to a single boundary for callers that only care about the
// compaction view.
func (s *Storage) boundary() uint64 {
	fi := s.firstIndex.Load()
	cu := s.compactedUpTo.Load()
	if cu > fi {
		return cu
	}
	return fi
}

// Returns the entry, the size of the entry (in bytes) before being unmarshalled, and an error
func (s *Storage) entry(i uint64) (*pb.Entry, uint64, error) {
	e := &pb.Entry{}
	s.entryMu.RLock()
	raw, err := s.EntryLog.Read(i)
	s.entryMu.RUnlock()
	if err != nil {
		return nil, 0, err
	}

	data, err := s.unframe(raw, "entry_log")
	if err != nil {
		return nil, 0, err
	}

	err = proto.Unmarshal(data, e)
	if err != nil {
		return nil, 0, err
	}

	// Size is the unframed payload length — what raft's maxSize budget
	// accounts for — not the on-disk framed length.
	return e, uint64(len(data)), nil
}

func (s *Storage) state(li uint64) (*State, error) {
	e := &State{}
	raw, err := s.StateLog.Read(li)
	if err != nil {
		return nil, err
	}

	data, err := s.unframe(raw, "state_log")
	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(e); err != nil {
		return nil, err
	}

	return e, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *Storage) Term(i uint64) (uint64, error) {
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	if firstIndex == 0 && lastIndex == 0 {
		return uint64(0), nil
	}

	if i < s.boundary() {
		return 0, raft.ErrCompacted
	}

	if i > lastIndex {
		return 0, raft.ErrUnavailable
	}

	logIndex := i - firstIndex + 1
	e, _, err := s.entry(logIndex)
	if err != nil {
		return 0, fmt.Errorf("wal index %d: %w", logIndex, err)
	}

	return e.GetTerm(), nil
}

func (s *Storage) LastIndex() (uint64, error) {
	return s.lastIndex.Load(), nil
}

// FirstIndex returns the first raft index that Entries can return.
// The entry at index boundary() is retained for Term() match checks
// (the compacted-dummy semantic etcd raft expects) but is not
// returnable via Entries — so the first readable index is
// boundary()+1.
func (s *Storage) FirstIndex() (uint64, error) {
	return s.boundary() + 1, nil
}

func (s *Storage) Snapshot() (*pb.Snapshot, error) {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	if s.snapshot == nil {
		return &pb.Snapshot{}, nil
	}
	// Clone under the read lock: this runs on raft's node goroutine,
	// concurrent with the serve loop's ConfState()/Save() writers that
	// mutate or replace s.snapshot. Returning the live pointer would race.
	return proto.Clone(s.snapshot).(*pb.Snapshot), nil
}

// Compact drops raft log entries up to and including compactIndex.
// Only the raft-log tier is trimmed; eventLog is never compacted.
//
// Sequence mapping: compactIndex maps to wal seq
// `compactIndex - firstIndex + 1`. tidwall/wal's TruncateFront removes
// every seq strictly less than the argument and leaves the surviving
// seqs at their absolute positions (no renumbering). firstIndex
// therefore STAYS at its original offset — moving it would break the
// seq-mapping formula for all future reads and writes (the entry at
// raft index R is at wal seq R - firstIndex + 1; once firstIndex has
// been advanced past 1, any subsequent read of R computes a wrong seq).
//
// compactedUpTo records the new compaction boundary so FirstIndex(),
// Entries(), and Term() know where the current compacted-dummy sits.
//
// Store order: compactedUpTo is advanced BEFORE TruncateFront fires.
// Reads from etcd raft's internal node goroutine (storage.Term during
// leader AppendEntries construction, e.g. maybeSendAppend) race
// against the serveChannels Compact call; if we truncated the wal
// first, a racing Term read of an about-to-be-compacted index would
// pass the boundary check against the stale compactedUpTo value,
// fall through to EntryLog.Read on a seq that no longer exists, and
// panic with "wal index N: not found". Advancing compactedUpTo first
// makes such reads return ErrCompacted — raft treats that as a
// signal to send an InstallSnapshot instead, which is safe even if
// the wal still briefly holds the old seq. If TruncateFront fails
// after compactedUpTo has been advanced, we're in an "over-reported
// compaction" state where the wal retains data whose indices
// we'll no longer return via Entries — benign: the data becomes
// unreachable but isn't corrupted.
func (s *Storage) Compact(compactIndex uint64) error {
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	if compactIndex <= s.boundary() {
		return raft.ErrCompacted
	}
	if compactIndex > lastIndex {
		return ErrOutOfBounds
	}

	s.compactedUpTo.Store(compactIndex)

	i := compactIndex - firstIndex + 1
	if err := s.EntryLog.TruncateFront(i); err != nil {
		return err
	}

	return nil
}
