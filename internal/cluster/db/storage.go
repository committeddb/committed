package db

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/interpretation"
)

// Storage is the engine's durable-state contract, implemented by wal.Storage
// in production and by hand-written in-memory doubles in tests (raft_test's
// MemoryStorage over db/testing.StorageStubs, the faulty wrappers). It is
// deliberately a composition of role interfaces, one per consuming
// subsystem, so the seams stay legible: the raft Ready loop drives
// ConsensusStorage, workers read EventLogStorage plus their own state role,
// admission and the status surface read ConfigStorage, and so on.
// Implementations satisfy the union structurally — the roles exist to name
// the seams, and to let a consumer hold one role instead of the whole
// contract (Raft holds raftStorage). There is deliberately no generated
// fake: a double that needs only a role implements that role.
type Storage interface {
	ConsensusStorage
	EventLogStorage
	ConfigStorage
	SyncableStateStorage
	IngestStateStorage
	TypeStateStorage
	InterpretationStorage
	MembershipStorage

	Close() error
	// ParkedWorkers lists every worker with a terminal parked record (sync + ingest),
	// replicated so any node answers identically. Powers the /node/status summary.
	// A cross-role read over the syncable and ingestable state stores, so it sits
	// on the union rather than in either role.
	ParkedWorkers() ([]cluster.ParkedWorker, error)
}

// ConsensusStorage is the raft Ready loop's persistence surface (raft.go):
// durable log/state writes, committed-entry apply, snapshot lifecycle, and
// the compaction trigger's inputs. The two indexes anchor the storage
// invariant P_local == R_local checked after every Ready iteration.
type ConsensusStorage interface {
	raft.Storage
	// Save persists raft state and entries durably. It does NOT apply the
	// entries to application state — that happens via ApplyCommitted, which
	// the raft Ready loop calls separately on rd.CommittedEntries.
	Save(st *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) error
	// ApplyCommitted dispatches a single committed raft entry to the
	// per-entity handlers that update application state (BoltDB buckets,
	// time series, downstream channels). It is called by the raft Ready loop
	// on entries from rd.CommittedEntries, after Save has persisted them and
	// before n.node.Advance(). Implementations must be idempotent on
	// re-apply (e.g. by skipping entries with index <= AppliedIndex). An
	// error from ApplyCommitted is treated as fatal by raft.go because
	// continuing past a half-applied entry diverges the state machine.
	ApplyCommitted(entry *raftpb.Entry) error
	// ApplyCommittedBatch applies one Ready's worth of committed entries,
	// hoisting the per-entry durable writes (event-log append, appliedIndex
	// persist) to per-batch — the apply loop's fsync-batching. Semantically
	// identical to calling ApplyCommitted per entry (same order, same
	// idempotent skip of already-applied entries); the crash-replay window
	// widens from one entry to at most one batch, which restart replay
	// already covers. The Ready loop calls this once per Ready instead of
	// looping ApplyCommitted.
	ApplyCommittedBatch(entries []*raftpb.Entry) error
	// AppliedIndex returns the highest log index that has been fully
	// applied to application state. Survives restart so the Ready loop's
	// replay of already-applied committed entries is a no-op. This is
	// "R_local" in the storage invariant checked after every Ready
	// iteration.
	AppliedIndex() uint64
	// EventIndex returns the highest raft index that has been durably
	// written to the permanent event log on this node. This is "P_local"
	// in the storage invariant P_local == R_local; violation means the
	// cluster has advanced past this node's recoverable window and is
	// the trigger for the fatal-exit rebuild path described in
	// docs/event-log-architecture.md.
	EventIndex() uint64
	// CreateSnapshot captures the current metadata-bucket state as a
	// pb.Snapshot keyed at the given raft index. The raft serve loop
	// calls this periodically so raft has a snapshot to ship to
	// followers whose raft log has been compacted past.
	CreateSnapshot(index uint64, confState *raftpb.ConfState) (*raftpb.Snapshot, error)
	// ConfState updates the conf state associated with the storage's
	// in-memory snapshot metadata. Called by the raft Ready loop after
	// each EntryConfChange apply, with the new ConfState returned by
	// raft.Node.ApplyConfChange. The update is persisted on the next
	// Save call (which writes the current snapshot metadata to the
	// state log), so InitialState returns the correct voter set on
	// restart. Without this, a restarted node reads an empty ConfState,
	// has no voter progress tracker entries, and cannot accept
	// heartbeats or forward proposals.
	ConfState(c *raftpb.ConfState)
	// RestoreSnapshot installs the metadata state carried by snap onto
	// this node, replacing current bbolt contents. Called from the
	// raft Ready loop when raft delivers a non-empty rd.Snapshot. The
	// permanent event log is NOT in the snapshot; a node whose event
	// log is behind the snapshot's metadata index is expected to
	// fatal-exit at the Ready loop's subsequent invariant check.
	RestoreSnapshot(snap *raftpb.Snapshot) error
	// Compact drops raft log entries up to and including compactIndex.
	// Called from the raft serve loop's compaction trigger after a
	// CreateSnapshot has captured the metadata at the compact point.
	// Implementations must leave EventLog untouched — it is the
	// permanent record and independent of raft log retention.
	Compact(compactIndex uint64) error
	// RaftLogApproxSize reports the approximate on-disk footprint of
	// the raft log in bytes, used by the compaction trigger to decide
	// whether the size limb of the "10GB or 1hr" policy has been
	// crossed. Implementations are allowed to return rough estimates
	// — the trigger treats this as a signal, not an assertion.
	RaftLogApproxSize() (uint64, error)
}

// EventLogStorage reads committed Actuals back out of the permanent event
// log: checkpoint-tracked worker reads, the dry-run's window sampler, and
// replay's single-entry point read.
type EventLogStorage interface {
	Reader(id string) ActualReader
	// ReaderAt reads committed Actuals from an arbitrary raft index —
	// the dry-run's window sampler (checkpoint-tracked reads use Reader).
	ReaderAt(index uint64) ActualReader // Gets current index by id cache. If id is not known, index is 0
	// ActualAt returns the committed Actual at a raft index, read from the
	// permanent event log by binary search. Used by replay to re-drive a
	// single dead-lettered Actual without disturbing any reader cursor.
	ActualAt(index uint64) (*cluster.Actual, error)
}

// ConfigStorage reads the applied configuration documents — databases,
// syncables, ingestables, types — with their version history, plus the
// derivation graph over them. Serves admission reads and the HTTP config
// surface.
type ConfigStorage interface {
	// ResolveType returns the Type identified by ref, with version 0
	// meaning latest. Returns ErrTypeMissing if the ID has never existed;
	// ErrVersionNotFound if the ID exists but the pinned version doesn't.
	ResolveType(ref cluster.TypeRef) (*cluster.Type, error)
	Database(id string) (cluster.Database, error)
	Databases() ([]*cluster.Configuration, error)
	Ingestables() ([]*cluster.Configuration, error)
	Syncables() ([]*cluster.Configuration, error)
	Types() ([]*cluster.Configuration, error)
	// ProducerEdges enumerates the stored producer graph — deriving
	// syncables and topic-producing ingestables, each with the raft index
	// its current version applied at: the single edge source both admission
	// (ReplayWithCandidate) and the build-time replay consume, so the two
	// predicates cannot skew.
	ProducerEdges() ([]DerivationEdge, error)
	// SyncableExists / IngestableExists are the cheap existence point-reads
	// behind the HTTP 404 gates (an absent id must never read as a healthy
	// phantom).
	SyncableExists(id string) (bool, error)
	IngestableExists(id string) (bool, error)
	DatabaseVersions(id string) ([]cluster.VersionInfo, error)
	DatabaseVersion(id string, version uint64) (*cluster.Configuration, error)
	IngestableVersions(id string) ([]cluster.VersionInfo, error)
	IngestableVersion(id string, version uint64) (*cluster.Configuration, error)
	SyncableVersions(id string) ([]cluster.VersionInfo, error)
	SyncableVersion(id string, version uint64) (*cluster.Configuration, error)
	TypeVersions(id string) ([]cluster.VersionInfo, error)
	TypeVersion(id string, version uint64) (*cluster.Configuration, error)
}

// SyncableStateStorage is the replicated per-syncable worker state: the
// ownership pin, checkpoint/re-materialization records, the dead-letter
// store, and the stuck/skip flow. Written from the apply path, so every
// replica answers identically — that's what makes the status and manual
// dead-letter flows node-agnostic.
type SyncableStateStorage interface {
	Node(id string) uint64 // Gets the node id that a worker is assigned to run on
	// SyncableCheckpoint returns the syncable's full checkpoint record —
	// including its interpretation pin — or (nil, false) when none exists.
	SyncableCheckpoint(id string) (*cluster.SyncableIndex, bool)
	// SyncableRematerialization returns the syncable's in-progress
	// re-materialization record, or (nil, false) when none exists. The owner
	// worker observes it (begin marking, sweep at the target head); the
	// status endpoint reports the progress.
	SyncableRematerialization(id string) (*cluster.SyncableRematerialization, bool)
	// SyncableDeadLetters returns the proposals a syncable permanently
	// skipped, in ascending raft-index order. `since` is an exclusive
	// raft-index cursor and `limit` bounds the page. Records are written
	// from the apply path, so they are consistent on every replica and
	// queryable from any node. An unknown id returns an empty slice.
	SyncableDeadLetters(id string, since uint64, limit int) ([]cluster.SyncableDeadLetter, error)
	// SyncableDeadLetterStats returns the count of UNACKNOWLEDGED
	// dead-lettered proposals for a syncable, the count of acknowledged
	// ones, and the raft index of the most recent record of either state
	// (0 when none). The status-surface summary of SyncableDeadLetters.
	SyncableDeadLetterStats(id string) (count, acknowledged, last uint64, err error)
	// SyncableDeadLetterAt returns the record for one (id, index), with
	// ok=false when no dead letter exists there — the acknowledge verb's
	// point read.
	SyncableDeadLetterAt(id string, index uint64) (cluster.SyncableDeadLetter, bool, error)
	// HasSyncableDeadLetter reports whether the proposal at raft index in
	// syncable id has been dead-lettered (permanently skipped or manually
	// skipped by an operator). The sync worker consults it before
	// processing a proposal so a skip survives restart: a proposal already
	// given up on is excluded rather than re-attempted. Backed by the same
	// replicated store as SyncableDeadLetters.
	HasSyncableDeadLetter(id string, index uint64) (bool, error)
	// SyncableStuck returns the syncable's current "blocked on index N"
	// record (ok=false if not blocked), and SyncableSkipRequest the pending
	// operator skip request (ok=false if none). Both are replicated metadata
	// written from the apply path, so any node answers identically — that's
	// what makes the manual dead-letter flow node-agnostic.
	SyncableStuck(id string) (cluster.SyncableStuck, bool, error)
	SyncableSkipRequest(id string) (cluster.SyncableSkipRequest, bool, error)
}

// IngestStateStorage is the replicated per-ingestable worker state: the
// resume position, transaction-scoped dedup, the shape census, refresh
// epochs, and the terminal parked record.
type IngestStateStorage interface {
	Position(id string) cluster.Position // Gets current index by id cache. If id is not known position is 0
	// IngestSourceDedup returns the transaction-scoped ingest dedup record
	// for id: the last applied source-transaction identity ("" = legacy
	// scalar regime) and the SourceSeq highwater within it. The worker's
	// pre-raft skip decision consults this, never the bare highwater —
	// dedup comparisons are only meaningful within one source transaction.
	IngestSourceDedup(id string) (txnID string, seq uint64)
	// IngestableCensus returns the latest applied JSON shape census for the
	// ingestable (published by its worker during the snapshot pass), or
	// (nil, false) when none exists. Replicated state, so any node's status
	// endpoint can serve it; also read by a resuming worker to seed its
	// accumulator at the same refresh epoch.
	IngestableCensus(id string) (*cluster.IngestableCensus, bool)
	// TopicRefreshEpoch returns the highest refresh generation ever committed
	// for a topic (type id), or 0 if none. Keyed by topic and NOT cleared by
	// DeleteIngestable, so a same-topic recreate reads the generation still on the
	// sink and resumes its epoch above it (see wal.Storage.TopicRefreshEpoch).
	TopicRefreshEpoch(topic string) uint64
	// IngestableStuck returns the ingestable's terminal parked record (ok=false if
	// the worker is not parked). Replicated from the apply path, so any node answers
	// identically — that's what makes the parked state visible cluster-wide.
	IngestableStuck(id string) (cluster.IngestableStuck, bool, error)
}

// TypeStateStorage is the type-level runtime state: the validation
// tripwire's announce dedupe and the migration dead-letter store.
type TypeStateStorage interface {
	// HasContractFingerprint reports whether the validation tripwire has
	// already announced the divergent shape (typeID, version, fingerprint) —
	// i.e. an applied LogContractFingerprint mark exists. Read pre-propose by
	// the tripwire so each distinct shape announces once; replicated state,
	// so the dedupe survives restarts and leadership moves.
	HasContractFingerprint(typeID string, version int, fingerprint string) bool
	// TypeMigrationDeadLetters returns the proposals whose entities failed
	// the type's migration program at runtime, in ascending raft-index
	// order — the type-keyed twin of SyncableDeadLetters with the same
	// cursor/limit semantics. An unknown type id returns an empty slice.
	TypeMigrationDeadLetters(typeID string, since uint64, limit int) ([]cluster.TypeMigrationDeadLetter, error)
	// HasTypeMigrationDeadLetter reports whether a migration dead-letter
	// record exists for the proposal at raft index in type id. Migration
	// retry consults it so a retry targets only a recorded failure.
	HasTypeMigrationDeadLetter(typeID string, index uint64) (bool, error)
}

// InterpretationStorage is the interpretation layer's read surface: the
// compiled restatements snapshot, restatement admission reads, and the
// in-place migration-edit coordinate that moves interpretation pins.
type InterpretationStorage interface {
	// InterpretationRegistry returns the current compiled restatements snapshot —
	// immutable, swapped whole on each restatement apply, never nil. The read
	// path resolves effective versions (stamp ⊕ restatement fold) through it.
	InterpretationRegistry() *interpretation.Registry
	// RestatementByID returns the applied restatement with the given id, its raft
	// index, and whether it exists — the admission read behind the
	// immutability rule (restatements are append-only, never edited).
	RestatementByID(id string) (*cluster.Restatement, uint64, bool)
	// AppliedRestatements returns every applied restatement with its raft index,
	// unordered. Powers GET /v1/restatement.
	AppliedRestatements() ([]cluster.AppliedRestatement, error)
	// TypeMigrationEditedAt returns the raft index of the type's latest
	// IN-PLACE migration edit (0 = never edited in place) — the
	// interpretation coordinate such an edit moves. SyncableInterpretation
	// compares it against always-current consumers' pins, and the fresh-pin
	// capture folds it in, so a migration fix flips interpretationStale
	// exactly like a restatement does. See wal/type_migration_edit.go.
	TypeMigrationEditedAt(typeID string) uint64
}

// MembershipStorage is the replicated member registry: each node's
// self-announced API URL, raft peer URL, feature level, and zone. Every
// map is replicated, so any node answers identically — that's what lets a
// follower resolve the leader's API address, the transport re-connect to
// dynamically-added members, and version/zone gates resolve uniformly.
type MembershipStorage interface {
	// MemberAPIURL returns the advertised HTTP API base URL node id
	// self-announced (and whether one is known); MemberAPIURLs returns the
	// whole id → URL map. Both read the replicated memberAPIURLs bucket, so
	// any node answers identically — that's what lets a follower resolve the
	// leader's API address to proxy a leader-only read. An un-announced node
	// (no COMMITTED_API_URL) is absent, not an error.
	MemberAPIURL(id uint64) (string, bool)
	MemberAPIURLs() map[uint64]string
	// DeleteMemberAPIURL drops a removed node's announced URL. Called from the
	// membership-remove apply path so entries don't accumulate across the
	// add/remove churn of rebalancing. Idempotent.
	DeleteMemberAPIURL(id uint64) error
	// PutMemberPeerURL / MemberPeerURLs / DeleteMemberPeerURL persist the raft
	// PEER URL (the transport dial address) per member id. raft's ConfState
	// replicates member IDs only, so applyConfChange writes the URL here on add
	// (and deletes on remove) to make it durable; reconcileTransport reads the
	// map at restart and after a snapshot install to re-connect the transport to
	// dynamically-added members the stale static COMMITTED_PEERS set omits.
	PutMemberPeerURL(id uint64, rawURL []byte) error
	MemberPeerURLs() map[uint64]string
	DeleteMemberPeerURL(id uint64) error
	// MemberVersion / MemberVersions / DeleteMemberVersion persist each member's
	// self-announced cluster feature level (entity-driven, applied via
	// handleNodeVersion). MemberVersions feeds the cluster-agreed minimum that
	// gates semantically-skewed emission (a new system type, a refresh-boundary
	// marker) until every member can apply it; DeleteMemberVersion drops a
	// removed node's entry from applyConfChange. See node_version.go.
	MemberVersion(id uint64) (uint64, bool)
	MemberVersions() map[uint64]uint64
	DeleteMemberVersion(id uint64) error
	// MemberZone / MemberZones / DeleteMemberZone persist each member's
	// self-announced zone (COMMITTED_ZONE; entity-driven, applied via
	// handleNodeZone). MemberZones feeds zone-pinned syncable ownership
	// resolution — intersected with current membership and feature-gated so a
	// mixed-version cluster resolves leader-owns everywhere. See node_zone.go
	// and db/zone.go.
	MemberZone(id uint64) (string, bool)
	MemberZones() map[uint64]string
	DeleteMemberZone(id uint64) error
}

// lostNotifierSetter is the optional Storage extension that accepts the
// truncation lost-callback. A Storage implementing it detects when a
// higher-term leader truncates uncommitted entries it physically held and
// invokes the callback with their RequestIDs; db.New installs db.notifyLost
// so blocking-Propose waiters get the definitive ErrProposalLost. wal.Storage
// implements it (see SetLostNotifier); the in-memory test double in raft_test
// does not, in which case truncation detection is simply absent — the
// leader-change watcher still covers those waiters with ErrProposalUnknown.
// Kept off the Storage interface (an optional interface, like
// scrubBacklogReporter) so the fake and the test double don't have to
// implement it.
type lostNotifierSetter interface {
	SetLostNotifier(func([]uint64))
}
