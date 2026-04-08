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
	// replay of already-applied committed entries is a no-op.
	AppliedIndex() uint64
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
}
