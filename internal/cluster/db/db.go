package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"fmt"
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
	raft        *Raft
	storage     Storage
	ctx         context.Context
	cancelSyncs context.CancelFunc
	parser      Parser
	leaderState *LeaderState
}

func New(id uint64, peers Peers, s Storage, p Parser, sync <-chan *SyncableWithID, ingest <-chan *IngestableWithID) *DB {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	ctx, cancelSyncs := context.WithCancel(context.Background())

	commitC, errorC, raft := NewRaft(id, rpeers, s, proposeC, confChangeC)

	db := &DB{
		CommitC:     commitC,
		ErrorC:      errorC,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		raft:        raft,
		storage:     s,
		ctx:         ctx,
		cancelSyncs: cancelSyncs,
		parser:      p,
		leaderState: raft.leaderState,
	}

	go db.listenForSyncables(sync)
	go db.listenForIngestables(ingest)

	return db
}

func (db *DB) listenForSyncables(sync <-chan *SyncableWithID) {
	for sync != nil {
		syncable := <-sync
		db.Sync(context.Background(), syncable.ID, syncable.Syncable)
	}
}

func (db *DB) listenForIngestables(ingest <-chan *IngestableWithID) {
	for ingest != nil {
		ingestable := <-ingest
		db.Ingest(context.Background(), ingestable.ID, ingestable.Ingestable)
	}
}

func (db *DB) EatCommitC() {
	go func() {
		for {
			<-db.CommitC
			fmt.Printf("[db.DB] Ate a commit\n")
		}
	}()
}

func (db *DB) Propose(p *cluster.Proposal) error {
	bs, err := p.Marshal()
	if err != nil {
		return err
	}

	// TODO Should we wrap this in a log level?
	fmt.Printf("[db.DB] Proposing %v", p)

	db.proposeC <- bs

	fmt.Println("[db.DB] ...Proposal made")

	return nil
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
	return db.raft.Close()
}

func (db *DB) Sync(ctx context.Context, id string, s cluster.Syncable) error {
	// TODO If transition to leader, start syncing, if transition off leader, stop syncing

	go func() {
		fmt.Printf("[db] Syncing %v\n", id)

		r := db.storage.Reader(id)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			i, p, err := r.Read()
			if err == io.EOF {
				// TODO Figure out what to do - maybe do an exponential backoff to a certain point - maybe nothing?
				continue
			} else if err != nil {
				// TODO Handle error
				fmt.Printf("[db.DB] read: %v\n", err)
				return
			}

			shouldSnapshot, err := s.Sync(db.ctx, p)
			if err != nil {
				// TODO Handle error
				fmt.Printf("[db.DB] sync: %v\n", err)
				return
			}

			if shouldSnapshot {
				err = db.proposeSyncableIndex(&cluster.SyncableIndex{ID: id, Index: i})
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] proposeSyncableIndex: %v\n", err)
					return
				}
			}
		}
	}()

	return nil
}

func (db *DB) proposeSyncableIndex(i *cluster.SyncableIndex) error {
	entity, err := cluster.NewUpsertSyncableIndexEntity(i)
	if err != nil {
		return err
	}

	err = db.Propose(&cluster.Proposal{Entities: []*cluster.Entity{entity}})
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) proposeIngestablePosition(p *cluster.IngestablePosition) error {
	entity, err := cluster.NewUpsertIngestablePositionEntity(p)
	if err != nil {
		return err
	}

	err = db.Propose(&cluster.Proposal{Entities: []*cluster.Entity{entity}})
	if err != nil {
		return err
	}

	return nil
}
