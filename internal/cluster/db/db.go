package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
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
	storage     Storage
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
	return &DB{CommitC: commitC, ErrorC: errorC, proposeC: proposeC, confChangeC: confChangeC, closer: closer, storage: s}
}

func (db *DB) Propose(p *cluster.Proposal) error {
	bs, err := p.Marshal()
	if err != nil {
		return err
	}

	db.proposeC <- bs

	return nil
}

func (db *DB) ProposeType(t *cluster.Type) error {
	typeEntity, err := cluster.NewUpsertTypeEntity(t)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{}
	p.Entities = append(p.Entities, typeEntity)

	return db.Propose(p)
}

func (db *DB) ProposeDeleteType(id string) error {
	deleteTypeEntity := cluster.NewDeleteTypeEntity(id)

	p := &cluster.Proposal{}
	p.Entities = append(p.Entities, deleteTypeEntity)

	return db.Propose(p)
}

func (db *DB) Type(id string) (*cluster.Type, error) {
	return db.storage.Type(id)
}

func (db *DB) Close() error {
	close(db.proposeC)
	return db.closer.Close()
}
