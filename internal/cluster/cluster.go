package cluster

import (
	"context"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//go:generate protoc --go_out=paths=source_relative:. ./clusterpb/cluster.proto

// TODO There should be a single Propose(p *Proposal) error and then utility functions for preparing different types of proposals
//
//counterfeiter:generate . Cluster
type Cluster interface {
	Propose(ctx context.Context, p *Proposal) error
	ProposeType(ctx context.Context, c *Configuration) error
	ProposeDeleteType(ctx context.Context, id string) error
	ProposeIngestable(ctx context.Context, c *Configuration) error
	ProposeSyncable(ctx context.Context, c *Configuration) error
	ProposeDatabase(ctx context.Context, c *Configuration) error
	// Scrub requests physical removal of already-delete-proposed (RTBF)
	// entities from the permanent event log up to the current applied index.
	// It proposes a Scrub command through Raft and returns once the command has
	// been applied (the rewrite then runs in the background on every node).
	// Idempotent and safe to call repeatedly: it only ever removes entities a
	// delete proposal already requested be forgotten. The automatic scheduler
	// calls this on a cadence; the manual lever (POST /v1/scrub) calls it on
	// demand for SLA-expedited erasure. See docs/event-log-architecture.md
	// § "Right-to-be-forgotten / deletes".
	Scrub(ctx context.Context) error
	// ResolveType returns the Type identified by ref. A TypeRef with
	// Version 0 (constructed via LatestTypeRef) resolves to whatever is
	// current; a TypeRef pinned to a specific version (TypeRefAt)
	// resolves to that historical definition. This is the single entry
	// point for type lookups — callers use the constructors to make
	// their intent explicit at the call site.
	ResolveType(ref TypeRef) (*Type, error)
	Close() error
	// The caller should run this on a separate go routine - or do we want to do this so close() can cancel all contexts?
	Ingest(ctx context.Context, id string, s Ingestable) error
	// The caller should run this on a separate go routine - or do we want to do this so close() can cancel all contexts?
	Sync(ctx context.Context, id string, s Syncable) error
	AddSyncableParser(name string, p SyncableParser)
	AddDatabaseParser(name string, p DatabaseParser)
	Databases() ([]*Configuration, error)
	Ingestables() ([]*Configuration, error)
	Syncables() ([]*Configuration, error)
	Types() ([]*Configuration, error)
	DatabaseVersions(id string) ([]VersionInfo, error)
	DatabaseVersion(id string, version uint64) (*Configuration, error)
	IngestableVersions(id string) ([]VersionInfo, error)
	IngestableVersion(id string, version uint64) (*Configuration, error)
	SyncableVersions(id string) ([]VersionInfo, error)
	SyncableVersion(id string, version uint64) (*Configuration, error)
	// SyncableDeadLetters returns the proposals a syncable gave up on and
	// skipped (dead-lettered), in ascending raft-index order. `since` is
	// an exclusive raft-index cursor for paging; `limit` bounds the page.
	// Backed by replicated state, so any node returns the same answer.
	SyncableDeadLetters(id string, since uint64, limit int) ([]SyncableDeadLetter, error)
	// DeadLetterStuckSyncable skips the proposal a syncable is currently
	// blocked retrying (a transient error retries forever, so the worker
	// stalls visibly rather than losing data until an operator intervenes).
	// It reads the replicated SyncableStuck record to find the blocked index
	// and proposes a skip request through Raft; the worker records a "manual"
	// dead letter and advances. Works from any node (the stuck state is
	// replicated). Returns the targeted raft index, or ErrSyncNotStuck if the
	// syncable isn't currently blocked. See ErrSyncNotStuck.
	DeadLetterStuckSyncable(ctx context.Context, id string) (uint64, error)
	// SyncableStuck reports whether a syncable's worker is currently blocked
	// and, if so, on which raft index (with when and the last error). Backed
	// by replicated state, so any node answers identically — powers
	// GET /syncable/{id}/status.
	SyncableStuck(id string) (SyncableStuck, bool, error)
	// SyncableProgress returns the syncable's checkpoint (the persisted
	// SyncableIndex — the consumed head it has synced, topic-skipped, or
	// dead-lettered through) and head (the highest data-entry raft index
	// applied on this node, i.e. DataEventIndex). The caller computes
	// lag = max(0, head − checkpoint); lag == 0 exactly when the worker has
	// nothing left to process. Both are O(1) local reads answerable on any
	// node without a leader hop — head excludes the syncable-metadata
	// entries (index bumps, dead-letters) the reader skips, so an idle
	// syncable reads lag 0 rather than a phantom backlog. A never-checkpointed
	// syncable reports checkpoint 0 (and lag == head). Powers the progress
	// fields on GET /syncable/{id}/status.
	SyncableProgress(id string) (checkpoint, head uint64, err error)
	// ReplaySyncableDeadLetter re-drives a dead-lettered proposal: it
	// re-runs the syncable's Sync for the proposal at index and, on success,
	// clears the dead-letter record. Node-agnostic. Returns ErrNotDeadLettered
	// if index isn't a dead letter for the syncable, or an error wrapping
	// ErrReplaySyncFailed if the re-sync failed again (the record is left in
	// place). See those errors.
	ReplaySyncableDeadLetter(ctx context.Context, id string, index uint64) error
	TypeVersions(id string) ([]VersionInfo, error)
	TypeVersion(id string, version uint64) (*Configuration, error)
	// TypeMigrationDeadLetters returns the proposals whose entities failed
	// the type's migration program at runtime, in ascending raft-index
	// order — the type-keyed twin of SyncableDeadLetters with the same
	// cursor/limit semantics. Backed by replicated state, so any node
	// returns the same answer.
	TypeMigrationDeadLetters(typeID string, since uint64, limit int) ([]TypeMigrationDeadLetter, error)
	// ReplayTypeMigrationDeadLetter re-runs the (presumably fixed)
	// migration chain for the dead-lettered proposal at index and, on
	// success, clears the type-keyed record. It validates the fix against
	// the exact payload that broke the old program; delivering the result
	// downstream is still ReplaySyncableDeadLetter's job. Node-agnostic.
	// Returns ErrNotDeadLettered if index isn't a migration dead letter
	// for the type, or an error wrapping ErrReplayMigrationFailed if the
	// chain still fails (the record is left in place).
	ReplayTypeMigrationDeadLetter(ctx context.Context, typeID string, index uint64) error
	// AddMember adds a voting node (id, rawURL) to the raft cluster using a
	// joint-consensus membership change and blocks until the change has
	// taken effect or ctx fires. rawURL is the new node's advertised peer
	// URL; the new node must be started in join mode. Partition-safe: joint
	// consensus requires a majority of both the old and new configurations
	// throughout the transition. Callable on any node. Powers POST
	// /membership. See docs/operations/membership.md.
	AddMember(ctx context.Context, id uint64, rawURL string) error
	// RemoveMember removes node id from the raft cluster using a
	// joint-consensus membership change and blocks until the change has
	// taken effect or ctx fires. Partition-safe and callable on any node.
	// Powers DELETE /membership/{id}.
	RemoveMember(ctx context.Context, id uint64) error
	// AddLearner adds a node (id, rawURL) as a non-voting learner using a
	// joint-consensus membership change and blocks until the change has taken
	// effect or ctx fires. A learner replicates the log but does not count
	// toward quorum; promote it to a voter with PromoteMember once it has
	// caught up. Same shape and partition-safety as AddMember. Powers
	// POST /membership with "learner": true. See docs/operations/membership.md.
	AddLearner(ctx context.Context, id uint64, rawURL string) error
	// PromoteMember promotes an existing learner (id) to a voter using a
	// joint-consensus membership change and blocks until the change has taken
	// effect or ctx fires. It validates that id is a current learner
	// (ErrNotLearner otherwise) but does NOT judge whether the learner has
	// caught up — that is the caller's policy, decided from the progress
	// GET /v1/membership reports. Partition-safe and callable on any node.
	// Powers POST /membership/{id}/promote.
	PromoteMember(ctx context.Context, id uint64) error
	// Membership returns a snapshot of the raft cluster configuration and
	// replication progress as observed by this node — voters/learners and,
	// when this node is the leader, each member's matched index. Powers
	// GET /v1/membership, which the HTTP layer proxies to the leader so the
	// per-member progress is populated regardless of which node a caller
	// (behind a load balancer) reaches. See cluster.Membership.
	Membership() Membership
	// MemberAPIURL returns the advertised HTTP API base URL node id
	// self-announced (and whether one is known). Backed by the replicated
	// address map, so it answers on any node — the leader-read proxy uses it
	// to resolve the leader's API address from a follower.
	MemberAPIURL(id uint64) (string, bool)
	// ID returns the raft node ID of this node. Used by GET /node/status
	// to report which node answered (load-bearing behind a load balancer).
	ID() uint64
	// Leader returns the raft node ID this cluster believes is the current
	// leader, or 0 if no leader is known. Used by the /ready HTTP probe to
	// gate readiness on raft having elected a leader.
	Leader() uint64
	// AppliedIndex returns the highest log index that has been fully
	// applied to local application state. Used by the /ready HTTP probe to
	// gate readiness on this node having caught up.
	AppliedIndex() uint64
	// LinearizableRead blocks until the raft leader has confirmed (via the
	// ReadIndex quorum round-trip) the index at which a linearizable read
	// may be served AND this node's applied state has caught up to it. It
	// returns nil when both hold, or ctx.Err() if the leader can't be
	// reached before ctx fires (e.g. this node is partitioned out of the
	// quorum). HTTP read handlers call this before serving replicated
	// state, so a default GET never returns data from a node that has
	// silently fallen behind. Reads that explicitly opt out
	// (?consistency=stale) skip it.
	LinearizableRead(ctx context.Context) error
	// ConfigBuildErrors returns the configs this node persisted but could
	// not build into live objects (degraded — a node-local condition,
	// usually a missing ${VAR} secret). The raw config bytes are valid and
	// replicated cluster-wide; only this node's local construction failed,
	// so the answer is per-node and a healthy node returns none. Powers GET
	// /node/status, the queryable counterpart of the
	// committed_config_build_errors gauge.
	ConfigBuildErrors() []ConfigBuildError
	// ReportDisk records member nodeID's node-local disk-pressure level
	// ("ok", "warn", "critical", or "full") on this node and returns the
	// cluster write-admission verdict computed from all the collected
	// states. Only the leader aggregates reports; on any other node it
	// returns ErrNotLeader and the reporter should re-resolve the leader.
	// Powers POST /v1/node/disk-report — the push half of cluster-aware
	// disk admission (the verdict in the response is the pull half).
	ReportDisk(nodeID uint64, state string) (DiskVerdict, error)
	// DiskAdmission returns this node's current view of write admission:
	// the decision its propose gate is applying right now, whether sourced
	// from a fresh leader-computed cluster verdict or from the node-local
	// fallback. Powers GET /node/status.
	DiskAdmission() DiskAdmissionStatus
	// DiskState returns this node's own disk-pressure level ("ok", "warn",
	// "critical", or "full") as last sampled by the local disk watcher.
	// Node-local diagnostics for GET /node/status, distinct from the
	// cluster-wide verdict DiskAdmission reports.
	DiskState() string
}
