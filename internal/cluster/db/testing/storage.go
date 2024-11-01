package testing

import (
	"io"
	"math"
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MemoryStorageSaveArgsForCall struct {
	st   raftpb.HardState
	ents []raftpb.Entry
	snap raftpb.Snapshot
}

type MemoryStorage struct {
	*raft.MemoryStorage
	saveArgsForCall []*MemoryStorageSaveArgsForCall
	indexes         map[string]uint64
}

func NewMemoryStorage() *MemoryStorage {
	indexes := make(map[string]uint64)
	return &MemoryStorage{raft.NewMemoryStorage(), nil, indexes}
}

func (ms *MemoryStorage) Close() error {
	return nil
}

func (ms *MemoryStorage) Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
	err := ms.Append(ents)
	if err != nil {
		return err
	}

	ms.maybeAppendArgsForCall(st, ents, snap)

	return ms.SetHardState(st)
}

func (ms *MemoryStorage) maybeAppendArgsForCall(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) {
	normalEntry := false
	for _, ent := range ents {
		if ent.Type == raftpb.EntryNormal {
			normalEntry = true
		}
	}

	if normalEntry {
		ms.saveArgsForCall = append(ms.saveArgsForCall, &MemoryStorageSaveArgsForCall{st, ents, snap})
	}
}

func (ms *MemoryStorage) SaveCallCount() int {
	return len(ms.saveArgsForCall)
}

func (ms *MemoryStorage) SaveArgsForCall(i int) (raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) {
	a := ms.saveArgsForCall[i]
	return a.st, a.ents, a.snap
}

func (ms *MemoryStorage) Type(id string) (*cluster.Type, error) {
	return nil, nil
}

func (ms *MemoryStorage) Reader(id string) db.ProposalReader {
	i, ok := ms.indexes[id]
	if !ok {
		i = 0
	}

	return &Reader{index: i, s: ms}
}

func (ms *MemoryStorage) Position(id string) cluster.Position {
	return nil
}

func (ms *MemoryStorage) Database(id string) (cluster.Database, error) {
	return nil, nil
}

func (ms *MemoryStorage) Ingestables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Syncables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Proposals() []*cluster.Proposal {
	fi, _ := ms.FirstIndex()
	li, _ := ms.LastIndex()
	ents, _ := ms.Entries(fi, li+1, math.MaxUint64)

	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			_ = p.Unmarshal(e.Data)

			if len(p.Entities) > 0 {
				ps = append(ps, p)
			}
		}
	}

	return ps
}

// TODO Pull this reader out and make it concrete instead of an interface
type Reader struct {
	sync.Mutex
	index uint64
	s     db.Storage
}

func (r *Reader) Read() (uint64, *cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	for {
		readIndex := r.index + 1

		li, err := r.s.LastIndex()
		if err != nil {
			return 0, nil, err
		}

		if readIndex > li {
			return 0, nil, io.EOF
		}

		ents, err := r.s.Entries(readIndex, readIndex+1, math.MaxUint)
		if err != nil {
			return 0, nil, err
		}

		ent := ents[0]

		r.index = readIndex

		if ent.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			_ = p.Unmarshal(ent.Data)

			if len(p.Entities) > 0 && !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
				return readIndex, p, nil
			}
		}
	}
}
