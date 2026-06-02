package wal

import (
	"errors"
	"fmt"
	"io"
	"sync"

	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
)

// Reader streams committed proposals out of the permanent event log.
// Syncable workers construct one via Storage.Reader(id) and read until
// io.EOF; r.raftIndex tracks the last raft index actually returned, so
// the next Read() resumes at the next higher raft index in the log.
//
// Before Phase 2 this reader scanned the raft entry log, which worked
// only because the raft log was never compacted. Now it reads from
// EventLog — the permanent tier — which is the right shape for the CQRS
// bootstrap path ("new syncable reads from the start") and for operation
// after the raft log gets compacted.
//
// EventLog's internal sequence numbers (1..N, dense) are not the raft
// indices of the entries they store — raft indices can have gaps when a
// caller bypasses ApplyCommitted (tests do this), and they don't start
// at 1 on a node restored by rsync. Reader therefore maintains its own
// walSeq cursor and resolves it lazily on the first Read from the
// syncable's raft-index position.
type Reader struct {
	sync.Mutex
	raftIndex      uint64 // last raft index returned to caller
	walSeq         uint64 // wal seq to read next; 0 until resolved
	walSeqResolved bool
	s              *Storage
}

func (r *Reader) Read() (uint64, *cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	if !r.walSeqResolved {
		seq, err := r.resolveStartSeq()
		if err != nil {
			return 0, nil, err
		}
		r.walSeq = seq
		r.walSeqResolved = true
	}

	for {
		walLast, err := r.s.lastEventSeq()
		if err != nil {
			return 0, nil, err
		}
		if r.walSeq == 0 || r.walSeq > walLast {
			return 0, nil, io.EOF
		}

		bs, err := r.s.readEventAt(r.walSeq)
		if err != nil {
			return 0, nil, fmt.Errorf("event log read seq %d: %w", r.walSeq, err)
		}

		ent := &pb.Entry{}
		if err := ent.Unmarshal(bs); err != nil {
			return 0, nil, err
		}

		r.raftIndex = ent.Index
		r.walSeq++

		if ent.Type != pb.EntryNormal || ent.Data == nil {
			continue
		}

		p := &cluster.Proposal{}
		if err := p.Unmarshal(ent.Data, r.s); err != nil {
			return 0, nil, err
		}

		// Metadata proposals (syncable-index bumps, dead-letter records)
		// are internal bookkeeping with no topic and shouldn't reach
		// syncable projection code — a syncable would otherwise re-Sync
		// its own dead letters. Skip them and keep scanning.
		if len(p.Entities) > 0 {
			tid := p.Entities[0].Type.ID
			if !cluster.IsSyncableIndex(tid) && !cluster.IsSyncableDeadLetter(tid) &&
				!cluster.IsSyncableStuck(tid) && !cluster.IsSyncableSkipRequest(tid) {
				return ent.Index, p, nil
			}
		}
	}
}

// resolveStartSeq binary-searches the event log for the first wal seq
// whose entry's raft index is strictly greater than r.raftIndex. Returns
// 0 if the log is empty or every entry is at or below r.raftIndex (EOF).
//
// Binary search is safe because Phase 1 writes are strictly monotonic in
// raft index (ApplyCommitted gates writes on entry.Index >
// eventIndex.Load()), so the raft-index column of the event log is
// sorted.
func (r *Reader) resolveStartSeq() (uint64, error) {
	first, err := r.s.firstEventSeq()
	if err != nil {
		return 0, err
	}
	last, err := r.s.lastEventSeq()
	if err != nil {
		return 0, err
	}
	if first == 0 || last == 0 || last < first {
		return 0, nil
	}

	// Fast path: the common "fresh syncable" case starts at
	// r.raftIndex == 0 and wants seq = first.
	if r.raftIndex == 0 {
		return first, nil
	}

	lo, hi := first, last+1
	for lo < hi {
		mid := lo + (hi-lo)/2
		bs, err := r.s.readEventAt(mid)
		if err != nil {
			return 0, fmt.Errorf("event log read seq %d during resolve: %w", mid, err)
		}
		ent := &pb.Entry{}
		if err := ent.Unmarshal(bs); err != nil {
			return 0, err
		}
		if ent.Index > r.raftIndex {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo > last {
		return 0, nil
	}
	return lo, nil
}

// ErrProposalNotFound is returned by ProposalAt when no committed proposal
// exists at the requested raft index (it was never committed, has been
// scrubbed, or the entry there carries no proposal data).
var ErrProposalNotFound = errors.New("wal: no proposal at raft index")

// ProposalAt returns the committed proposal at raft index, read straight from
// the permanent event log. It binary-searches the log by raft index (the
// event log is strictly monotonic in raft index, so seq order == index
// order), so it is O(log n) and does not disturb any syncable's read cursor.
// Used by replay to re-drive a single dead-lettered proposal. Returns
// ErrProposalNotFound if the index isn't present or carries no proposal.
func (s *Storage) ProposalAt(index uint64) (*cluster.Proposal, error) {
	first, err := s.firstEventSeq()
	if err != nil {
		return nil, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return nil, err
	}
	if first == 0 || last == 0 || last < first {
		return nil, ErrProposalNotFound
	}

	lo, hi := first, last
	for lo <= hi {
		mid := lo + (hi-lo)/2
		bs, err := s.readEventAt(mid)
		if err != nil {
			return nil, fmt.Errorf("event log read seq %d: %w", mid, err)
		}
		ent := &pb.Entry{}
		if err := ent.Unmarshal(bs); err != nil {
			return nil, err
		}
		switch {
		case ent.Index == index:
			if ent.Type != pb.EntryNormal || ent.Data == nil {
				return nil, ErrProposalNotFound
			}
			p := &cluster.Proposal{}
			if err := p.Unmarshal(ent.Data, s); err != nil {
				return nil, err
			}
			return p, nil
		case ent.Index < index:
			lo = mid + 1
		default:
			if mid == first {
				return nil, ErrProposalNotFound
			}
			hi = mid - 1
		}
	}
	return nil, ErrProposalNotFound
}

func (s *Storage) Reader(id string) db.ProposalReader {
	i, err := s.getSyncableIndex(id)
	if err != nil {
		// TODO We should log this
		i = 0
	} else if id == "" {
		i = 0
	}

	return &Reader{raftIndex: i, s: s}
}
