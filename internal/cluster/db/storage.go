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
	Reader(index uint64) ProposalReader // The index to start reading from
	Database(id string) (cluster.Database, error)
	Syncables() ([]*cluster.Configuration, error)
}
