package wal

import (
	"io"
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Reader struct {
	sync.Mutex
	index uint64
	s     *Storage
}

func (r *Reader) Read() (uint64, *cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	for {
		readIndex := r.index + 1

		if readIndex > r.s.lastIndex {
			return 0, nil, io.EOF
		}

		bs, err := r.s.EntryLog.Read(readIndex)
		if err != nil {
			return 0, nil, err
		}

		ent := &pb.Entry{}
		err = ent.Unmarshal(bs)
		if err != nil {
			return 0, nil, err
		}

		r.index = readIndex

		if ent.Type == pb.EntryNormal {
			p := &cluster.Proposal{}
			err := p.Unmarshal(ent.Data)
			if err != nil {
				return 0, nil, err
			}

			if len(p.Entities) > 0 && !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
				return readIndex, p, nil
			}
		}
	}
}

func (s *Storage) Reader(id string) db.ProposalReader {
	i, err := s.getSyncableIndex(id)
	if err != nil {
		// TODO We should log this
		i = 0
	} else if id == "" {
		i = 0
	}

	return &Reader{index: i, s: s}
}
