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
	// ProposeType admits a type configuration. Declaring a nonConvertible
	// version bump while always-current syncables consume this type's topics
	// is refused with a StrandedSyncablesError naming them, unless the caller
	// passes AcknowledgeStrandedSyncables (the HTTP layer's ?force=true).
	ProposeType(ctx context.Context, c *Configuration, opts ...ProposeTypeOption) error
	// MigrationEditDependents enumerates the always-current syncables
	// consuming typeID's topic — the consumers an in-place migration edit
	// leaves stale until re-materialized. Powers the POST /type advisory.
	MigrationEditDependents(typeID string) []DependentSyncable
	// ProposeRestatement admits one append-only interpretation-registry
	// statement (see Restatement). Immutable per id: a re-POST with different
	// content is refused; an identical re-POST is an idempotent no-op.
	// Refused with ClusterBelowFeatureLevelError until every member can fold
	// restatements. Powers POST /v1/restatement/{id}.
	ProposeRestatement(ctx context.Context, c *Configuration) error
	// DryRunRestatement rehearses a restatement against the committed log — the
	// same admission-level validation as ProposeRestatement, then a scan of the
	// restatement's own index range through the real interpretation fold,
	// reporting what it selects, what it changes, and which consumers it
	// would stale — without admitting anything. Powers POST
	// /v1/restatement/dryrun.
	DryRunRestatement(ctx context.Context, mimeType string, data []byte, opts DryRunOptions) (*RestatementDryRunReport, error)
	// Restatements returns every applied restatement with its raft index, unordered.
	// Powers GET /v1/restatement.
	Restatements() ([]AppliedRestatement, error)
	ProposeDeleteType(ctx context.Context, id string) error
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
	Syncables() ([]*Configuration, error)
	// Ingestables and IngestableStatus are HOLDOVERS for the type group's
	// pipeline endpoint (GET /type/{id}/pipeline), like Syncables /
	// SyncableProgress / SyncableStuck — deleted when that group migrates
	// off the aggregated interface (see the cluster-interface-retirement
	// ticket).
	Ingestables() ([]*Configuration, error)
	IngestableStatus(ctx context.Context, id string) (IngestableStatus, error)
	Types() ([]*Configuration, error)
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
	// ApplyStalled reports whether this node has committed-but-unapplied
	// raft entries with ZERO apply progress for a sustained threshold — an
	// apply wedge (hung disk write, apply-handler bug). Such a node cannot
	// confirm proposals and serves stale reads, so the /ready probe uses
	// this to take it out of rotation instead of reporting ready while the
	// node is effectively down (the silent-while-green field incident). A
	// slow-but-advancing apply (boot replay of a long backlog) is NOT a
	// stall: progress resets the clock.
	ApplyStalled() bool
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
}
