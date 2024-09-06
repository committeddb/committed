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
			p.Unmarshal(ent.Data)

			if len(p.Entities) > 0 {
				return readIndex, p, nil
			}
		}
	}
}

func (s *Storage) Reader(index uint64) db.ProposalReader {
	return &Reader{index: index, s: s}
}
