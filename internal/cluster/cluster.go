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
	TypeVersions(id string) ([]VersionInfo, error)
	TypeVersion(id string, version uint64) (*Configuration, error)
	// Leader returns the raft node ID this cluster believes is the current
	// leader, or 0 if no leader is known. Used by the /ready HTTP probe to
	// gate readiness on raft having elected a leader.
	Leader() uint64
	// AppliedIndex returns the highest log index that has been fully
	// applied to local application state. Used by the /ready HTTP probe to
	// gate readiness on this node having caught up.
	AppliedIndex() uint64
}
