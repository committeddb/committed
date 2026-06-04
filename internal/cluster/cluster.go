package cluster

import (
	"context"
	"time"
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
	Proposals(n uint64, types ...string) ([]*Proposal, error)
	// ResolveType returns the Type identified by ref. A TypeRef with
	// Version 0 (constructed via LatestTypeRef) resolves to whatever is
	// current; a TypeRef pinned to a specific version (TypeRefAt)
	// resolves to that historical definition. This is the single entry
	// point for type lookups — callers use the constructors to make
	// their intent explicit at the call site.
	ResolveType(ref TypeRef) (*Type, error)
	TypeGraph(typeID string, start time.Time, end time.Time) ([]TimePoint, error)
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
	// ReplaySyncableDeadLetter re-drives a dead-lettered proposal: it
	// re-runs the syncable's Sync for the proposal at index and, on success,
	// clears the dead-letter record. Node-agnostic. Returns ErrNotDeadLettered
	// if index isn't a dead letter for the syncable, or an error wrapping
	// ErrReplaySyncFailed if the re-sync failed again (the record is left in
	// place). See those errors.
	ReplaySyncableDeadLetter(ctx context.Context, id string, index uint64) error
	TypeVersions(id string) ([]VersionInfo, error)
	TypeVersion(id string, version uint64) (*Configuration, error)
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
	// ConfigBuildErrors returns the configs this node persisted but could
	// not build into live objects (degraded — a node-local condition,
	// usually a missing ${VAR} secret). The raw config bytes are valid and
	// replicated cluster-wide; only this node's local construction failed,
	// so the answer is per-node and a healthy node returns none. Powers GET
	// /node/status, the queryable counterpart of the
	// committed_config_build_errors gauge.
	ConfigBuildErrors() []ConfigBuildError
}
