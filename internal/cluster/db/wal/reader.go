package wal

import (
	"errors"
	"fmt"
	"io"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
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
	// lastGen is the scrub generation walSeq was resolved against. When the
	// storage's generation moves ahead of it, a scrub re-densified the wal
	// seqs and walSeq is stale, so we re-resolve from raftIndex (which is
	// never renumbered). See Storage.scrubGen.
	lastGen uint64
	s       *Storage
}

func (r *Reader) Read() (*cluster.Actual, error) {
	r.Lock()
	defer r.Unlock()

	// Hold eventMu.RLock for the whole read so a concurrent scrub swap can't
	// re-densify the seqs mid-scan, and so the generation check + resolve +
	// scan see one consistent log. Uses the *Locked accessors throughout to
	// avoid re-acquiring RLock (which would deadlock against a waiting swap).
	r.s.eventMu.RLock()
	defer r.s.eventMu.RUnlock()

	// If a scrub completed since we last resolved, our cached walSeq points
	// into the old (pre-densification) seq space. Re-resolve from raftIndex.
	gen := r.s.scrubGen.Load()
	if r.walSeqResolved && gen != r.lastGen {
		r.walSeqResolved = false
	}
	r.lastGen = gen

	if !r.walSeqResolved {
		seq, err := r.resolveStartSeqLocked()
		if err != nil {
			return nil, err
		}
		r.walSeq = seq
		r.walSeqResolved = true
	}

	for {
		walLast, err := r.s.lastEventSeqLocked()
		if err != nil {
			return nil, err
		}
		if r.walSeq == 0 || r.walSeq > walLast {
			return nil, io.EOF
		}

		bs, err := r.s.readEventAtLocked(r.walSeq)
		if err != nil {
			return nil, fmt.Errorf("event log read seq %d: %w", r.walSeq, err)
		}

		ent := &pb.Entry{}
		if err := ent.Unmarshal(bs); err != nil {
			return nil, err
		}

		r.raftIndex = ent.Index
		r.walSeq++

		if ent.Type != pb.EntryNormal || ent.Data == nil {
			continue
		}

		p := &cluster.Proposal{}
		if err := p.Unmarshal(ent.Data, r.s); err != nil {
			return nil, err
		}

		// Internal proposals — committed's own config (type / database /
		// syncable / ingestable) and coordination (syncable index +
		// dead-letters + stuck/skip, ingestable position, scrub, etc.) —
		// are not topic data and must NOT be projected into a syncable: a
		// syncable would otherwise re-Sync its own dead letters, and
		// committed's control plane would leak out of band into every
		// downstream sink. Skip them so a syncable sees only user-defined
		// topic data (ingested data included — it rides under user topic
		// types). Keep scanning.
		if len(p.Entities) > 0 {
			if !cluster.IsInternal(p.Entities[0].Type.ID) {
				return &cluster.Actual{Index: ent.Index, Entities: p.Entities}, nil
			}
		}
	}
}

// resolveStartSeqLocked binary-searches the event log for the first wal seq
// whose entry's raft index is strictly greater than r.raftIndex. Returns
// 0 if the log is empty or every entry is at or below r.raftIndex (EOF).
// Caller must hold r.s.eventMu (Read holds RLock), so it uses the lock-free
// accessors.
//
// Binary search is safe because the raft-index column of the event log is
// strictly ascending: appends gate on entry.Index > eventIndex.Load(), and a
// right-to-be-forgotten scrub only *removes* entries (it never reorders or
// renumbers them), so the column stays sorted — just sparse (gapped) after a
// scrub. Binary search tolerates the gaps; only the arithmetic fast-path
// would not, which is why this resolves by search, not by formula.
func (r *Reader) resolveStartSeqLocked() (uint64, error) {
	first, err := r.s.firstEventSeqLocked()
	if err != nil {
		return 0, err
	}
	last, err := r.s.lastEventSeqLocked()
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
		bs, err := r.s.readEventAtLocked(mid)
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

// ErrActualNotFound is returned by ActualAt when no committed Actual exists
// at the requested raft index (it was never committed, has been scrubbed, or
// the entry there carries no proposal data).
var ErrActualNotFound = errors.New("wal: no committed entry at raft index")

// ActualAt returns the committed Actual at raft index, read straight from the
// permanent event log. It binary-searches the log by raft index (the event log
// is strictly ascending in raft index — sparse after a scrub, but still sorted
// — so seq order == index order), so it is O(log n) and does not disturb any
// syncable's read cursor. Used by replay to re-drive a single dead-lettered
// Actual. Returns ErrActualNotFound if the index isn't present (never committed
// or scrubbed) or carries no proposal.
//
// Holds eventMu.RLock for the whole search so a concurrent scrub swap can't
// re-densify the seqs mid-search; uses the lock-free accessors throughout.
func (s *Storage) ActualAt(index uint64) (*cluster.Actual, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	first, err := s.firstEventSeqLocked()
	if err != nil {
		return nil, err
	}
	last, err := s.lastEventSeqLocked()
	if err != nil {
		return nil, err
	}
	if first == 0 || last == 0 || last < first {
		return nil, ErrActualNotFound
	}

	lo, hi := first, last
	for lo <= hi {
		mid := lo + (hi-lo)/2
		bs, err := s.readEventAtLocked(mid)
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
				return nil, ErrActualNotFound
			}
			p := &cluster.Proposal{}
			if err := p.Unmarshal(ent.Data, s); err != nil {
				return nil, err
			}
			return &cluster.Actual{Index: ent.Index, Entities: p.Entities}, nil
		case ent.Index < index:
			lo = mid + 1
		default:
			if mid == first {
				return nil, ErrActualNotFound
			}
			hi = mid - 1
		}
	}
	return nil, ErrActualNotFound
}

func (s *Storage) Reader(id string) db.ActualReader {
	i, err := s.getSyncableIndex(id)
	if err != nil {
		// TODO We should log this
		i = 0
	} else if id == "" {
		i = 0
	}

	return &Reader{raftIndex: i, s: s}
}
