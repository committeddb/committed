package db

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Transport interface {
	GetErrorC() chan error
	Start(stopC <-chan struct{}) error
	AddPeer(peer raft.Peer) error
	RemovePeer(id uint64)
	Send(msgs []raftpb.Message)
	Stop()
}
