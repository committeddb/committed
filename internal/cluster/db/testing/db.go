package testing

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
)

// testTickInterval makes single-node Raft elect itself in ~10ms instead of
// the default ~1s, eliminating the per-test startup tax.
const testTickInterval = 1 * time.Millisecond

func CreateDB() *DB {
	return CreateDBWithStorage(NewMemoryStorage(), parser.New(), nil, nil)
}

func CreateDBWithStorage(s db.Storage, parser *parser.Parser, sync <-chan *db.SyncableWithID, ingest <-chan *db.IngestableWithID) *DB {
	id := uint64(1)
	// An empty local-peer URL tells httptransport.HttpTransport to skip
	// binding a TCP listener, since single-node tests have no peers and
	// there is nothing to receive. This avoids the historical port-12379
	// collision when multiple test packages run in parallel under
	// `go test ./...`.
	peers := make(db.Peers)
	peers[id] = ""

	d := db.New(id, peers, s, parser, sync, ingest, db.WithTickInterval(testTickInterval))
	return &DB{d, s, peers, id}
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

	return db.entsToProposals(ents)
}

func (db *DB) entsToProposals(ents []raftpb.Entry) ([]*cluster.Proposal, error) {
	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(e.Data, db.storage); err != nil {
				return nil, err
			}
			ps = append(ps, p)
		}
	}

	return ps, nil
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

func (ms *MemorySyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	ms.count++
	ms.proposals = append(ms.proposals, p)
	if ms.doneAtCount == ms.count {
		ms.cancel()
	}

	shouldSnapshot := true
	for _, e := range p.Entities {
		if cluster.IsSystem(e.Type.ID) {
			shouldSnapshot = false
		}
	}

	return cluster.ShouldSnapshot(shouldSnapshot), nil
}

func (ms *MemorySyncable) Close() error {
	return nil
}
