package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"fmt"
	"io"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Peers map[uint64]string

type DB struct {
	CommitC     <-chan []byte
	ErrorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	closer      io.Closer
}

func New(id uint64, peers Peers, s Storage) *DB {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	commitC, errorC, closer := NewRaft(id, rpeers, s, proposeC, confChangeC)
	return &DB{CommitC: commitC, ErrorC: errorC, proposeC: proposeC, confChangeC: confChangeC, closer: closer}
}

func NewWithDefaultStorage(id uint64, peers Peers) (*DB, error) {
	s, err := wal.Open("./data")
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %w", err)
	}

	return New(id, peers, s), nil
}

func (db *DB) Propose(p *cluster.LogProposal) error {
	bs, err := p.Marshal()
	if err != nil {
		return err
	}

	return db.propose(bs)
}

func (db *DB) propose(b []byte) error {
	db.proposeC <- b
	return nil
}

func (db *DB) Close() error {
	close(db.proposeC)
	return db.closer.Close()
}
