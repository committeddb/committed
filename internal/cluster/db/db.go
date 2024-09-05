package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
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
	ctx         context.Context
	cancelSyncs context.CancelFunc
	parser      Parser
}

func New(id uint64, peers Peers, s Storage, p Parser) *DB {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	ctx, cancelSyncs := context.WithCancel(context.Background())

	commitC, errorC, closer := NewRaft(id, rpeers, s, proposeC, confChangeC)
	return &DB{
		CommitC:     commitC,
		ErrorC:      errorC,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		closer:      closer,
		storage:     s,
		ctx:         ctx,
		cancelSyncs: cancelSyncs,
		parser:      p,
	}
}

func (db *DB) EatCommitC() {
	go func() {
		for {
			<-db.CommitC
		}
	}()
}

func (db *DB) Propose(p *cluster.Proposal) error {
	bs, err := p.Marshal()
	if err != nil {
		return err
	}

	// TODO Should we wrap this in a log level?
	// fmt.Printf("Proposing %v", p)

	db.proposeC <- bs

	// fmt.Println("...Proposal made")

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
	db.cancelSyncs()
	return db.closer.Close()
}

// The caller should run this on a separate go routine - or do we want to do this so close() can cancel all contexts?
func (db *DB) Sync(ctx context.Context, id string, s cluster.Syncable) error {
	go func() {
		r := db.storage.Reader(id)

		for {
			select {
			case <-db.ctx.Done():
				return
			default:
			}

			p, err := r.Read()
			if err == io.EOF {
				// TODO Figure out what to do - maybe do an exponential backoff to a certain point - maybe nothing?
				continue
			} else if err != nil {
				// TODO Handle error
				return
			}

			err = s.Sync(db.ctx, p)
			if err != nil {
				// TODO Handle error
				return
			}
		}
	}()

	return nil
}
