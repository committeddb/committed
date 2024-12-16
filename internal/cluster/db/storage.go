package db

import (
	"github.com/philborlin/committed/internal/cluster"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

//counterfeiter:generate . Storage
type Storage interface {
	raft.Storage
	Close() error
	Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error
	Type(id string) (*cluster.Type, error)
	Reader(id string) ProposalReader     // Gets current index by id cache. If id is not known, index is 0
	Position(id string) cluster.Position // Gets current index by id cache. If id is not known position is 0
	Node(id string) uint64               // Gets the node id that a worker is assigned to run on
	Database(id string) (cluster.Database, error)
	Databases() ([]*cluster.Configuration, error)
	Ingestables() ([]*cluster.Configuration, error)
	Syncables() ([]*cluster.Configuration, error)
	Types() ([]*cluster.Configuration, error)
}
