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
	// ProposeType admits a type configuration. Declaring a nonConvertible
	// version bump while always-current syncables consume this type's topics
	// is refused with a StrandedSyncablesError naming them, unless the caller
	// passes AcknowledgeStrandedSyncables (the HTTP layer's ?force=true).
	ProposeType(ctx context.Context, c *Configuration, opts ...ProposeTypeOption) error
	// ProposeErratum admits one append-only interpretation-registry
	// statement (see Erratum). Immutable per id: a re-POST with different
	// content is refused; an identical re-POST is an idempotent no-op.
	// Refused with ClusterBelowFeatureLevelError until every member can fold
	// errata. Powers POST /v1/erratum/{id}.
	ProposeErratum(ctx context.Context, c *Configuration) error
	// Errata returns every applied erratum with its raft index, unordered.
	// Powers GET /v1/erratum.
	Errata() ([]AppliedErratum, error)
	// SyncableInterpretation reports the syncable's interpretation pin — the
	// registry index its current materialization began under — and whether
	// errata affecting its consumed topics have landed past it (stale: some
	// sink rows were derived under a superseded reading; re-derivation is
	// operator-triggered, never automatic). Part of GET /syncable/{id}/status.
	SyncableInterpretation(id string) (pin uint64, stale bool, err error)
	ProposeDeleteType(ctx context.Context, id string) error
	ProposeIngestable(ctx context.Context, c *Configuration) error
	// DeleteIngestable removes an ingestable: its config and checkpoint position
	// are deleted atomically and, on apply, the owner cancels the worker and tears
	// down the source-side replication resources (drops the Postgres slot +
	// publication) so an orphaned slot can't pin the source's WAL and fill its
	// disk. A later same-id create starts fresh from a full snapshot.
	DeleteIngestable(ctx context.Context, id string) error
	ProposeSyncable(ctx context.Context, c *Configuration) error
	// DryRunSyncable rehearses a syncable configuration against a
	// bounded sample of the committed log — full admission-level
	// parsing, a real fold into a throwaway store, and a diagnostic
	// report — without proposing, storing, or touching a destination.
	DryRunSyncable(ctx context.Context, mimeType string, data []byte, opts DryRunOptions) (*DryRunReport, error)
	// DeleteSyncable removes a syncable: its config and checkpoint are deleted
	// atomically and, unless keepData is set, the owner tears down the
	// syncable's destination state (best-effort — for a SQL syncable, dropping
	// its table). A later same-name create then starts fresh from index 0.
	DeleteSyncable(ctx context.Context, id string, keepData bool) error
	// RebuildSyncable re-materializes a syncable's destination from index 0
	// using the same config: it resets the checkpoint, tears the destination
	// down and back up (clean slate), and replays. The recovery primitive for a
	// drifted or corrupted projection. Returns ErrResourceNotFound if the
	// syncable is unknown.
	RebuildSyncable(ctx context.Context, id string) error
	// RematerializeSyncable replays a syncable's topic from index 0 through
	// the current projection + interpretation while its keyed sink keeps
	// serving — the non-destructive sibling of RebuildSyncable. Keyed
	// upserts converge rows in place; a completion sweep removes rows the
	// replay never re-emitted; the checkpoint's interpretation pin
	// refreshes. Returns ErrNotRematerializable for sinks that cannot
	// converge in place. Powers POST /v1/syncable/{id}/rematerialize.
	RematerializeSyncable(ctx context.Context, id string) error
	// SyncableRematerialization reports the syncable's in-progress
	// re-materialization (nil, false when none) — the status endpoint's
	// progress source.
	SyncableRematerialization(id string) (*SyncableRematerialization, bool)
	// SyncableZonePin reports a stored syncable's zone pin: its configured
	// zone (ok=false for unpinned) and whether the pin is currently
	// unsatisfiable (no current member announces the zone — the strict
	// stall). Part of GET /syncable/{id}/status.
	SyncableZonePin(id string) (zone string, unsatisfiable bool, ok bool)
	// SyncableDerivation reports a stored syncable's derivation provenance —
	// the topic it consumes and the derived topic it produces. ok is false
	// for every non-deriving kind; the status surface omits the fields then.
	SyncableDerivation(id string) (source, target string, ok bool)
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
	// SyncableExists reports whether a syncable config id currently exists.
	// The status/errors endpoints gate on it and 404 an unknown id — an
	// absent id must never synthesize a healthy-looking status from
	// default-zero reads (field finding: a typo'd id in monitoring watched a
	// "running" phantom with real head/lag forever).
	SyncableExists(id string) (bool, error)
	// IngestableExists is SyncableExists's ingest twin.
	IngestableExists(id string) (bool, error)
	Types() ([]*Configuration, error)
	DatabaseVersions(id string) ([]VersionInfo, error)
	DatabaseVersion(id string, version uint64) (*Configuration, error)
	IngestableVersions(id string) ([]VersionInfo, error)
	IngestableVersion(id string, version uint64) (*Configuration, error)
	// IngestableStatus reports an ingestable worker's operational status —
	// snapshot vs. streaming phase, per-table snapshot progress, the CDC
	// position, source lag, and whether it is caught up. It reads the worker's
	// persisted checkpoint (replicated, so consistent on any node behind a
	// linearize barrier) and asks the dialect to decode it and, where supported,
	// query the source for lag. Returns ErrIngestableNotRunning if no worker is
	// registered for id on this node. Powers GET /v1/ingestable/{id}/status.
	IngestableStatus(ctx context.Context, id string) (IngestableStatus, error)
	// IngestableCensus returns the latest JSON shape census the ingestable's
	// worker published during its snapshot pass, or (nil, false) when none
	// exists (census opted out, or no snapshot has run yet). Replicated
	// state, identical on any node. Powers the census section of
	// GET /v1/ingestable/{id}/status.
	IngestableCensus(id string) (*IngestableCensus, bool)
	SyncableVersions(id string) ([]VersionInfo, error)
	SyncableVersion(id string, version uint64) (*Configuration, error)
	// SyncableDeadLetters returns the proposals a syncable gave up on and
	// skipped (dead-lettered), in ascending raft-index order. `since` is
	// an exclusive raft-index cursor for paging; `limit` bounds the page.
	// Backed by replicated state, so any node returns the same answer.
	SyncableDeadLetters(id string, since uint64, limit int) ([]SyncableDeadLetter, error)
	// SyncableDeadLetterStats returns how many proposals the syncable has
	// dead-lettered (skipped) and NOT yet acknowledged, how many an
	// operator has acknowledged as resolved out-of-band, and the raft
	// index of the most recent record of either state (0 when none).
	// Surfaced on GET /syncable/{id}/status so a caught-up sink with
	// skipped rows never reads as fully green — the honest completeness
	// check is caughtUp && deadLetters == 0 && workerState == "running";
	// acknowledging is how a superseded skip stops reading permanently
	// red. Backed by replicated state.
	SyncableDeadLetterStats(id string) (count, acknowledged, last uint64, err error)
	// AcknowledgeSyncableDeadLetter marks the dead-letter record at raft
	// index as resolved out-of-band (the operator attests a later event
	// superseded the skipped proposal, or it was fixed at the source —
	// replaying would REGRESS the sink row, and leaving it inflates the
	// completeness count forever). The record leaves the deadLetters
	// count but stays listable as an audit trail; a later replay is still
	// allowed (success deletes the record entirely). Replicated —
	// acknowledge once, every node agrees. Returns ErrNotDeadLettered
	// when no record exists at that index; acknowledging an
	// already-acknowledged record is an idempotent no-op.
	AcknowledgeSyncableDeadLetter(ctx context.Context, id string, index uint64) error
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
	// SyncableOwner returns the raft node ID that owns the syncable's worker:
	// the pinned node when the config names one, otherwise the current leader
	// (0 when no leader is known). Derived from replicated state, so any node
	// answers identically. Powers the ownerNode field on
	// GET /syncable/{id}/status and routes the opt-in readPosition proxy.
	SyncableOwner(id string) uint64
	// SyncableReadPosition reports the live scan position of the syncable's
	// worker ON THIS NODE — the raft index of the last log entry its reader
	// examined, advancing per entry scanned including skips of other topics'
	// entries. False when this node has no live position (no worker, idle
	// non-owner, or a reader without the capability). Owner-local: only the
	// owning node's worker holds a reader, so the HTTP layer proxies
	// ?readPosition=true status calls to SyncableOwner's node.
	SyncableReadPosition(id string) (position uint64, ok bool)
	// SyncableStageRecovery reports a running stage-state re-derivation on
	// THIS node's worker (folded index, checkpoint target; ok=false none).
	// Owner-local, O(1) — served on the default status call.
	SyncableStageRecovery(id string) (folded, target uint64, ok bool)
	// SyncableStageStats reports the per-stage introspection rows (keys /
	// inputs / fanned) of the syncable's worker on THIS node (ok=false: no
	// worker here, or no stages). Owner-local; see StageIntrospector.
	SyncableStageStats(id string) (stats map[string]StageStat, ok bool)
	// SyncableStageKeyExists probes one stage output key on THIS node's
	// worker (ok=false: no worker here, or no stages). keyParts are the
	// key's parts in keyPath order — the owner renders and composes
	// them. err: undeclared stage name or part-count mismatch.
	// Owner-local; see StageIntrospector.
	SyncableStageKeyExists(id, stage string, keyParts []string) (exists bool, ok bool, err error)
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
	// ConfigBuildErrors returns the configs this node persisted but could
	// not build into live objects (degraded — a node-local condition,
	// usually a missing ${VAR} secret). The raw config bytes are valid and
	// replicated cluster-wide; only this node's local construction failed,
	// so the answer is per-node and a healthy node returns none. Powers GET
	// /node/status, the queryable counterpart of the
	// committed_config_build_errors gauge.
	ConfigBuildErrors() []ConfigBuildError
	// ParkedWorkers lists every worker (sync or ingest) that has TERMINALLY parked
	// and needs operator intervention, from replicated state — so any node answers
	// identically (unlike ConfigBuildErrors, which is node-local). Powers the GET
	// /node/status worker summary, the queryable counterpart of the
	// committed_worker_parked gauge; a healthy cluster returns none.
	ParkedWorkers() ([]ParkedWorker, error)
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
	// SafeMode reports whether THIS node booted with COMMITTED_SAFE_MODE —
	// sync/ingest/scrub workers held, everything else normal. Node-local
	// and ephemeral (per-boot, never replicated); powers GET /node/status
	// so an operator can confirm the escape hatch is active instead of
	// wondering why workers aren't running.
	SafeMode() bool
}
