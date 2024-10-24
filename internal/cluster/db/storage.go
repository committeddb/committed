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
	Reader(id string) ProposalReader // Sets current index by id cache. If id is not know index is 0
	Database(id string) (cluster.Database, error)
	Syncables() ([]*cluster.Configuration, error)
}
