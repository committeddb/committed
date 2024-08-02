package db

import (
	"github.com/philborlin/committed/internal/cluster"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type ProposalReader interface {
	Read() (*cluster.Proposal, error)
}

//counterfeiter:generate . Storage
type Storage interface {
	raft.Storage
	Close() error
	Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error
	Type(id string) (*cluster.Type, error)
	Reader(id string) ProposalReader
}
