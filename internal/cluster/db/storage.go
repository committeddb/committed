package db

import (
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

//counterfeiter:generate . Storage
type Storage interface {
	raft.Storage
	Close() error
	// Save persists raft state and entries durably. It does NOT apply the
	// entries to application state — that happens via ApplyCommitted, which
	// the raft Ready loop calls separately on rd.CommittedEntries.
	Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error
	// ApplyCommitted dispatches a single committed raft entry to the
	// per-entity handlers that update application state (BoltDB buckets,
	// time series, downstream channels). It is called by the raft Ready loop
	// on entries from rd.CommittedEntries, after Save has persisted them and
	// before n.node.Advance(). Implementations must be idempotent on
	// re-apply (e.g. by skipping entries with index <= AppliedIndex). An
	// error from ApplyCommitted is treated as fatal by raft.go because
	// continuing past a half-applied entry diverges the state machine.
	ApplyCommitted(entry raftpb.Entry) error
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
	CreateSnapshot(index uint64, confState *raftpb.ConfState) (raftpb.Snapshot, error)
	// RestoreSnapshot installs the metadata state carried by snap onto
	// this node, replacing current bbolt contents. Called from the
	// raft Ready loop when raft delivers a non-empty rd.Snapshot. The
	// permanent event log is NOT in the snapshot; a node whose event
	// log is behind the snapshot's metadata index is expected to
	// fatal-exit at the Ready loop's subsequent invariant check.
	RestoreSnapshot(snap raftpb.Snapshot) error
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
	Type(id string) (*cluster.Type, error)
	TimePoints(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error)
	Reader(id string) ProposalReader     // Gets current index by id cache. If id is not known, index is 0
	Position(id string) cluster.Position // Gets current index by id cache. If id is not known position is 0
	Node(id string) uint64               // Gets the node id that a worker is assigned to run on
	Database(id string) (cluster.Database, error)
	Databases() ([]*cluster.Configuration, error)
	Ingestables() ([]*cluster.Configuration, error)
	Syncables() ([]*cluster.Configuration, error)
	Types() ([]*cluster.Configuration, error)
	DatabaseVersions(id string) ([]cluster.VersionInfo, error)
	DatabaseVersion(id string, version uint64) (*cluster.Configuration, error)
	IngestableVersions(id string) ([]cluster.VersionInfo, error)
	IngestableVersion(id string, version uint64) (*cluster.Configuration, error)
	SyncableVersions(id string) ([]cluster.VersionInfo, error)
	SyncableVersion(id string, version uint64) (*cluster.Configuration, error)
	TypeVersions(id string) ([]cluster.VersionInfo, error)
	TypeVersion(id string, version uint64) (*cluster.Configuration, error)
}
