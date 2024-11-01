package testing

import (
	"context"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func CreateDB() *DB {
	return CreateDBWithStorage(NewMemoryStorage(), parser.New(), nil, nil)
}

func CreateDBWithStorage(s db.Storage, parser *parser.Parser, sync <-chan *db.SyncableWithID, ingest <-chan *db.IngestableWithID) *DB {
	id := uint64(1)
	url := fmt.Sprintf("http://127.0.0.1:%d", 12379)
	peers := make(db.Peers)
	peers[id] = url

	db := db.New(id, peers, s, parser, sync, ingest)
	return &DB{db, s, peers, id}
}

type DB struct {
	*db.DB
	storage db.Storage
	peers   db.Peers
	id      uint64
}

func (db *DB) Ents() ([]*cluster.Proposal, error) {
	fi, err := db.storage.FirstIndex()
	if err != nil {
		return nil, err
	}

	li, err := db.storage.LastIndex()
	if err != nil {
		return nil, err
	}

	ents, err := db.storage.Entries(fi, li+1, 10000)
	if err != nil {
		return nil, err
	}

	ps := db.entsToProposals(ents)

	return ps, nil
}

func (db *DB) entsToProposals(ents []raftpb.Entry) []*cluster.Proposal {
	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			_ = p.Unmarshal(e.Data)
			ps = append(ps, p)
		}
	}

	return ps
}

type MemorySyncable struct {
	proposals []*cluster.Proposal
	cancel    func()
	count     int
	// done        chan any
	doneAtCount int
}

func NewSyncable(doneAtCount int, cancel func()) *MemorySyncable {
	return &MemorySyncable{doneAtCount: doneAtCount, cancel: cancel}
	// return &MemorySyncable{doneAtCount: doneAtCount, cancel: cancel, done: make(chan any)}
}

func (ms *MemorySyncable) Init(ctx context.Context) error {
	return nil
}

func (ms *MemorySyncable) Sync(ctx context.Context, p *cluster.Proposal) error {
	fmt.Printf("syncing: %v\n", p)

	ms.count++
	ms.proposals = append(ms.proposals, p)
	if ms.doneAtCount == ms.count {
		ms.cancel()
		// ms.done <- ""
	}

	return nil
}

func (ms *MemorySyncable) Close() error {
	return nil
}
