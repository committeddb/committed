package db

import (
	"io"
	"math"

	"github.com/philborlin/committed/internal/cluster"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (db *DB) Proposals(n uint64, typeIDs ...string) ([]*cluster.Proposal, error) {
	last, err := db.storage.LastIndex()
	if err != nil {
		return nil, err
	}

	var ps []*cluster.Proposal
	for {
		candidates, err := db.readProposals(last, n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		ps = addCandidatesToProposals(candidates, ps, n, typeIDs...)

		// We found all the proposals or we read them all already
		if uint64(len(ps)) >= n || last < n {
			break
		}

		last = last - n
	}

	return ps, nil
}

func (db *DB) readProposals(last uint64, number uint64) ([]*cluster.Proposal, error) {
	first, err := db.storage.FirstIndex()
	if err != nil {
		return nil, err
	}

	if last <= first {
		return nil, io.EOF
	}

	max := last - first
	if max < number {
		number = max
	}

	f2 := last - number + 1

	es, err := db.storage.Entries(f2, last+1, math.MaxUint64)
	if err != nil {
		return nil, err
	}

	return entsToProposals(es), nil
}

func entsToProposals(ents []raftpb.Entry) []*cluster.Proposal {
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

func addCandidatesToProposals(candidates []*cluster.Proposal, ps []*cluster.Proposal, max uint64, typeIDs ...string) []*cluster.Proposal {
outer:
	for i := len(candidates) - 1; i >= 0; i-- {
		p := candidates[i]
		if uint64(len(ps)) >= max {
			break outer
		}

		for _, e := range p.Entities {
			if len(typeIDs) > 0 {
				for _, id := range typeIDs {
					if e.Type.ID == id {
						ps = append(ps, p)
						continue outer
					}
				}
			} else {
				if !cluster.IsSystem(e.Type.ID) {
					ps = append(ps, p)
					continue outer
				}
			}
		}
	}

	return ps
}
