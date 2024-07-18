package db

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

//counterfeiter:generate . Storage
type Storage interface {
	raft.Storage
	Close() error
	Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error
}
