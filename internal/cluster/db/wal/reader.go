package wal

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Reader struct {
	sync.Mutex
	lastReadIndex uint64
	s             *Storage
}

// How to deal with tombstones?
func (r *Reader) Read() (*cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	for {
		bs, err := r.s.EntryLog.Read(r.lastReadIndex + 1)
		if err != nil {
			return nil, err
		}

		ent := &pb.Entry{}
		err = ent.Unmarshal(bs)
		if err != nil {
			return nil, err
		}

		r.lastReadIndex++

		if ent.Type == pb.EntryNormal {
			p := &cluster.Proposal{}
			p.Unmarshal(ent.Data)

			if len(p.Entities) > 0 {
				return p, nil
			}
		}
	}
}

// TODO Look up the lastReadIndex by id
func (s *Storage) Reader(id string) cluster.ProposalReader {
	return &Reader{lastReadIndex: 0, s: s}
}
