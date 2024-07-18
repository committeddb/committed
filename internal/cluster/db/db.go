package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster/db/wal"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type DB struct {
	CommitC     <-chan []byte
	ErrorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
}

func New(id uint64, peers Peers, s Storage) *DB {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	commitC, errorC := newRaft(id, peers, s, proposeC, confChangeC)
	return &DB{CommitC: commitC, ErrorC: errorC, proposeC: proposeC, confChangeC: confChangeC}
}

func NewWithDefaultStorage(id uint64, peers Peers) (*DB, error) {
	s, err := wal.Open(".")
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %w", err)
	}

	return New(id, peers, s), nil
}

func (db *DB) Propose(b []byte) error {
	db.proposeC <- b
	return nil
}
