package wal

import (
	"bytes"
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
	// topicRefreshEpochBucket maps a topic (type id) to the big-endian uint64
	// highwater of the highest refresh generation ever committed for it. Drives
	// the delete-surviving epoch floor so a same-topic recreate stamps above the
	// generations still on the sink — see topic_refresh_epoch.go. NOT cleared by
	// DeleteIngestable (topic-scoped, outlives any ingestable incarnation).
	topicRefreshEpochBucket = []byte("topicRefreshEpoch")
	// memberAPIURLBucket maps a raft node id (8 big-endian bytes, the key
	// cluster.NodeAPIURLKey produces) to a marshaled cluster.NodeAPIURL — the
	// node's self-announced advertised HTTP API base URL. Written from the
	// apply path (handleNodeAPIURL) so the mapping replicates to every node and
	// rides along in snapshots; read by the leader-read proxy to resolve a
	// peer's (in particular the leader's) API address. See member_api_url.go
	// and raft-leader-read-proxy.md.
	memberAPIURLBucket = []byte("memberAPIURLs")
	// memberPeerURLBucket maps a raft node id (8 big-endian bytes) to its raft
	// PEER URL (the address the transport dials for raft messages), stored as the
	// raw URL bytes. Unlike memberAPIURLBucket it is NOT entity-driven: raft's
	// ConfState replicates member IDs only, never addresses, and the peer URL
	// rides transiently on the ConfChange Context — so applyConfChange writes it
	// here (and deletes it on remove) to make it durable. On restart / snapshot
	// install the transport is reconciled from this bucket instead of the stale
	// static COMMITTED_PEERS set, so a dynamically-added peer is still reachable.
	// Rides along in snapshots (bbolt is serialized whole into CreateSnapshot).
	// See member_peer_url.go and raft.applyConfChange / reconcileTransport.
	memberPeerURLBucket = []byte("memberPeerURLs")
	// memberVersionBucket maps a raft node id (8 big-endian bytes, the key
	// cluster.NodeVersionKey produces) to a marshaled cluster.NodeVersion — the
	// node's self-announced cluster feature level. Entity-driven (written from
	// the apply path, handleNodeVersion) so it replicates to every node and
	// rides snapshots. Read to compute the cluster-agreed minimum feature level
	// that gates semantically-skewed emission (a new system type, a
	// refresh-boundary marker). See node_version.go and db.featureEnabled.
	memberVersionBucket = []byte("memberVersions")
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

	// confStateBucket durably holds the raft membership ConfState that
	// ApplyConfChange returns. It is written ATOMICALLY with appliedIndex (in
	// saveAppliedIndex) so a crash can never leave the applied index ahead of
	// a stale membership config — see ConfState / confstate-lost-in-crash-window.
	confStateBucket = []byte("confState")
	confStateKey    = []byte("cs")
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
	// scrubCompactOwedKey marks that a scrub PRUNED RTBF tombstones but has not
	// yet physically compacted bbolt. It is written in the SAME tx as the prune
	// and cleared only after compaction succeeds, so a compaction that ENOSPCs or
	// crashes (leaving the erased subject key in freed pages that CreateSnapshot
	// would copy) is re-driven on the next Open / scrub signal instead of being
	// suppressed forever by the already-advanced "completed" bound. See
	// markScrubComplete / runOwedCompaction.
	scrubCompactOwedKey = []byte("compactOwed")
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
	handler func(*Storage, *cluster.Entity, uint64) error
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
	{cluster.IsNodeVersion, "handleNodeVersion", memberVersionBucket, (*Storage).handleNodeVersion},
}

// buckets is every bbolt bucket Open must create: one per internal entity type
// (derived from internalEntities) plus the ones that aren't entity-driven — the
// ingest source-seq highwater and the applied-index marker.
var buckets = func() [][]byte {
	bs := make([][]byte, 0, len(internalEntities)+4)
	for _, ie := range internalEntities {
		bs = append(bs, ie.bucket)
	}
	return append(bs, ingestSourceSeqBucket, appliedIndexBucket, confStateBucket, eventTombstoneBucket, topicRefreshEpochBucket, memberPeerURLBucket)
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
	// failCompactionForTest, when non-nil, forces compactLocked to fail — used to
	// reproduce an ENOSPC/crashed compaction so a test can assert the erased key
	// is re-driven out of bbolt on the next Open. Nil in production.
	failCompactionForTest func() error
	// closeOnce makes Close idempotent: the owner (cmd/node) closes the Storage
	// after db.Close returns, and some callers (and tests) may close it more than
	// once, so a second Close must be a no-op rather than double-closing the
	// bbolt/WAL handles.
	closeOnce sync.Once
	closeErr  error
	// closeC is closed (once, at the top of Close) to signal "this Storage is
	// shutting down." The config-notification sends (saveSyncable/deleteSyncable
	// and their ingest twins on the apply path, plus RequestSyncReconcile/
	// RequestIngestReconcile from the detached restore/startup goroutines) select
	// on it so a send whose db-layer listener has already stopped draining escapes
	// instead of stranding on the unbuffered channel forever. A dropped
	// notification is re-emitted by reconcile on the next start, so nothing is lost.
	closeC chan struct{}
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
	// metadataBacklogBytes approximates the BYTES a metadata-GC scrub would
	// reclaim: each counted supersession adds ~the size of the superseding
	// entity (same-key entries are similarly sized). The volume gate
	// (hasMetadataBacklog) compares it against the event log's size so an
	// O(total-log) rewrite only fires when it reclaims a meaningful fraction
	// of what it rewrites. Reset with metadataBacklog.
	metadataBacklogBytes atomic.Int64
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
	// databasesMu guards the databases handle cache. Writers (saveDatabase,
	// deleteDatabase, loadDatabasesFromTx) all run on the single raft
	// apply/restore goroutine, but readers do NOT: Database(id) is hit from
	// HTTP handler goroutines (every syncable POST/rollback/replay parses its
	// config, which resolves the database) and from the startup
	// RestoreSyncableWorkers goroutine racing catch-up applies. An unguarded
	// read against a concurrent map write is an unrecoverable runtime fatal
	// (node crash). Single-writer discipline means individual ops only need to
	// be atomic — no check-then-act window to protect — so accesses take the
	// mutex briefly and never hold it across parsing or handle Close.
	databasesMu sync.RWMutex
	databases   map[string]cluster.Database
	parser      db.Parser
	sync        chan<- *db.SyncableWithID
	ingest      chan<- *db.IngestableWithID

	// configErrMu guards configErrors. configErrors records, per
	// "kind/id" (e.g. "database/orders"), the most recent failure to
	// BUILD a config's live object on THIS node — a node-local condition
	// (a missing ${VAR} secret, a parse error) that must not fail the
	// deterministic apply. The raw config bytes are always persisted;
	// only the local construction is deferred. A successful build clears
	// the entry. db.DB reads the count via ConfigBuildErrorCount to emit
	// a gauge. See secrets.go / saveDatabase for the rationale.
	configErrMu  sync.Mutex
	configErrors map[string]configErr

	// eventLogWriteOps and appliedIndexPersists count write OPERATIONS (each
	// op is one sync when fsync is on) against the event log and the
	// appliedIndex bucket. Observability for the apply-loop fsync-batching
	// regression test: a Ready batch of N entries must cost 1+1 ops, not N+N.
	eventLogWriteOps     atomic.Int64
	appliedIndexPersists atomic.Int64

	// appliedIndex is the highest raft entry index that ApplyCommitted has
	// fully applied. Bumped after each successful per-entry apply (and
	// persisted to bbolt in the same step). Loaded from bbolt on Open.
	//
	// This is "R_local" in the storage invariant P_local == R_local.
	appliedIndex atomic.Uint64

	// pendingConfState holds the ConfState from a conf-change applied this
	// Ready, staged by ConfState() and flushed to confStateBucket ATOMICALLY
	// with appliedIndex by saveAppliedIndex — so membership durability can
	// never lag the applied index (confstate-lost-in-crash-window). Guarded by
	// snapMu (ConfState already holds it); accessed only on the Ready loop.
	pendingConfState *pb.ConfState
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
	// fsyncDisabled mirrors the WithoutFsync option (also passed to bbolt as
	// NoSync at Open). The file/directory swaps in snapshot.go and scrub.go read
	// it to skip their fsync legs so the test suite stays fast; production leaves
	// it false so a rename is crash-durable.
	fsyncDisabled bool
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

// openLog opens one of a node's tidwall logs, turning tidwall's opaque
// ErrCorrupt ("log corrupt") into an actionable ErrCorruptEntry: it records the
// corruption metric and points the operator at the offline repair CLI and the
// rebuild runbook, so a torn tail or bit-flip fails startup with a message they
// can act on instead of a bare "log corrupt". Other open errors pass through.
func openLog(dir, logName string, m *metrics.Metrics) (*wal.Log, error) {
	lg, err := wal.Open(dir, nil)
	if err == nil {
		return lg, nil
	}
	if errors.Is(err, wal.ErrCorrupt) {
		if m != nil {
			m.WalCorruptEntry(logName)
		}
		return nil, fmt.Errorf("%w: the %s at %q will not open (%v) — stop the node and run `committed wal repair --data <node-data-dir>` to truncate a torn tail, or rebuild this node from a healthy replica; see docs/operations/rebuild.md",
			ErrCorruptEntry, logName, dir, err)
	}
	return nil, fmt.Errorf("open %s: %w", logName, err)
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

	entryLog, err := openLog(entryLogDir, "entry_log", cfg.metrics)
	if err != nil {
		return nil, err
	}
	stateLog, err := openLog(stateLogDir, "state_log", cfg.metrics)
	if err != nil {
		return nil, err
	}
	eventLog, err := openLog(eventLogDir, "event_log", cfg.metrics)
	if err != nil {
		return nil, err
	}

	// Sweep any bbolt.db.restore.* / bbolt.db.compact.* temp file a crash left
	// between a full-DB write and its atomic rename (RestoreSnapshot /
	// compactLocked). Nothing else references these, so without this they linger
	// indefinitely — a disk leak, and for the restore form an RTBF residual (a
	// leader snapshot payload that may hold an erased key). Mirrors the scrub-dir
	// and entry-log-discard recovery above.
	if err := sweepBoltTempFiles(keyValueStorageDir); err != nil {
		return nil, fmt.Errorf("sweep orphaned bbolt temp files: %w", err)
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
		fsyncDisabled:   cfg.fsyncDisabled,
		configErrors:    make(map[string]configErr),
		metrics:         cfg.metrics,
		lostCallback:    cfg.lostCallback,
		scrubSignal:     make(chan struct{}, 1),
		scrubStop:       make(chan struct{}),
		scrubDone:       make(chan struct{}),
		closeC:          make(chan struct{}),
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

	// Complete the OTHER half of the same crashed install: reconcileEntryLogWithSnapshot
	// above heals the entry-log cut, but a crash between that cut and RestoreSnapshot's
	// bbolt swap leaves bbolt's applied index below the snapshot index — raft's
	// RestartNode then panics in appliedTo (a brick). Re-run the bbolt swap from the
	// persisted snapshot payload. A no-op on every normal node (applied >= snapshot
	// index). Runs BEFORE validateConfigSecrets/loadDatabases below so they read the
	// restored bbolt, not the stale pre-install one. Recover eventIndex first so the
	// reconcile can apply the same snapIdx <= eventIndex guard RestoreSnapshot uses.
	if err := ws.recoverEventIndex(); err != nil {
		return nil, fmt.Errorf("recover event index: %w", err)
	}
	if err := ws.reconcileBboltWithSnapshot(); err != nil {
		return nil, fmt.Errorf("reconcile bbolt with snapshot: %w", err)
	}

	// Heal the one-fsync-behind crash: Save persists Entries then HardState to two
	// separately-fsync'd logs, so a crash between the two fsyncs can leave
	// HardState.Term below the last entry's term — which fatals raft's start-time
	// invariant (assertStorageTermInvariant) on every restart, a rebuild-only
	// brick. Truncate the entry tail down to HardState.Term instead. See the method.
	if err := ws.reconcileEntryLogWithHardState(); err != nil {
		return nil, fmt.Errorf("reconcile entry log with hard state: %w", err)
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

	// Complete any RTBF compaction a prior scrub pruned tombstones for but didn't
	// finish (crash/ENOSPC after the prune committed). The already-advanced
	// "completed" bound would otherwise suppress the re-run, leaving the erased
	// subject key physically in bbolt and every snapshot. Synchronous at Open so a
	// restart heals it before this node can serve/snapshot. See runOwedCompaction.
	if err := ws.runOwedCompaction(); err != nil {
		ws.logger.Error("run owed compaction on open", zap.Error(err))
	}

	// Re-derive the sync-stuck gauge from any applied stuck records. The apply
	// path (handleSyncableStuck) is what normally sets it, but it doesn't run for
	// entries already applied before this restart (replay skips <= appliedIndex),
	// so a syncable that was stuck at shutdown would otherwise read 0 until it
	// next makes progress. See refreshSyncStuckGauges.
	ws.refreshSyncStuckGauges()

	return ws, nil
}

func (s *Storage) Close() error {
	s.closeOnce.Do(func() {
		var finalErr error

		// Release any config-notification sender (apply-path or a detached
		// reconcile goroutine) that is blocked on the unbuffered sync/ingest
		// channel because the db-layer listener has already stopped draining.
		// Closed before the log/handle closes below so a straggler send unblocks
		// promptly rather than stranding for the life of the process.
		close(s.closeC)

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

		// Swap the cache out under its mutex (a straggler HTTP read must not
		// race this range), then close the handles outside it. Skip cached
		// nils — a deleted entry stores nil, and Close on a nil interface
		// would panic.
		for _, db := range s.resetDatabaseCache() {
			if db == nil {
				continue
			}
			err = db.Close()
			if err != nil && finalErr == nil {
				finalErr = err
			}
		}

		s.closeErr = finalErr
	})
	return s.closeErr
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
	// Stage for the atomic durable write paired with appliedIndex. The
	// snapshot-metadata copy above is for CreateSnapshot; confStateBucket
	// (written with appliedIndex) is the crash-authoritative one InitialState
	// reads back.
	s.pendingConfState = c
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
// eventLogApproxSize sums the on-disk size of the permanent event log's
// segment files — the cost basis for the metadata-GC volume gate (a scrub
// rewrite reads the whole log twice and rewrites it once). Same shape as
// RaftLogApproxSize; advisory, so a file vanishing mid-walk (a concurrent
// scrub swap) is skipped rather than failed.
func (s *Storage) eventLogApproxSize() (uint64, error) {
	entries, err := os.ReadDir(s.eventLogDir)
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
			continue
		}
		if sz := info.Size(); sz > 0 {
			total += uint64(sz)
		}
	}
	return total, nil
}

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

// InitialState returns the saved HardState and ConfState information.
func (s *Storage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	// raft 3.7's Storage contract: the returned ConfState must not be nil —
	// return an empty one when no snapshot ConfState has been persisted yet.
	// Prefer the crash-authoritative ConfState written atomically with
	// appliedIndex (confStateBucket); it can never lag the applied index the
	// way the snapshot-metadata copy (persisted lazily on the next Save) could.
	// Fall back to the snapshot metadata for a fresh node that has not applied
	// a conf-change yet, or a data dir written before confStateBucket existed.
	cs := s.durableConfState()
	if cs == nil {
		cs = s.snapshot.GetMetadata().GetConfState()
	}
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
			// A Compact can win the race between the boundary check above and
			// this read: compactedUpTo advances, TruncateFront removes the seq,
			// and the read fails with the wal's not-found. The raft Storage
			// contract requires a racing compaction to surface as ErrCompacted
			// (etcd panics on anything else) — re-check the boundary and return
			// the sentinel if the range's head is now compacted. A failure with
			// the boundary UNMOVED is genuine corruption and still propagates.
			if lo <= s.boundary() {
				return nil, raft.ErrCompacted
			}
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
		// Same race as Entries: a Compact between the boundary check and the
		// read removes the seq. Return the BARE raft.ErrCompacted sentinel —
		// raft compares it directly, so it must not be wrapped.
		if i < s.boundary() {
			return 0, raft.ErrCompacted
		}
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
