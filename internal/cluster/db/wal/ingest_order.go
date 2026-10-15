package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The ingest order record, keyed by ingestable id: the IngestSeq and
// IngestSession of the last ACCEPTED sequenced proposal (see
// cluster.Proposal.IngestSeq). It is what lets an ingest worker keep many
// proposals in flight: apply accepts a sequenced proposal only if it is the
// next number and continues the last accepted session (or starts a new one),
// so a proposal dropped before commit (a leader change, a full raft buffer)
// can never be skipped by a later one that committed, and a stale session's
// straggler can never land behind its successor's re-emission.
//
// The record is never swept, not on an ingestable delete either: it only
// moves forward, so a deleted generation's straggler can never match a
// recreated one's numbering. A recreate's worker starts from it.
//
// Value encoding: [8-byte seq BE][8-byte session BE].

// ingestOrder is one ingestable's record.
type ingestOrder struct {
	seq     uint64
	session uint64
}

func decodeIngestOrder(raw []byte) ingestOrder {
	if len(raw) < 16 {
		return ingestOrder{}
	}
	return ingestOrder{seq: binary.BigEndian.Uint64(raw[:8]), session: binary.BigEndian.Uint64(raw[8:16])}
}

func (o ingestOrder) encode() []byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], o.seq)
	binary.BigEndian.PutUint64(buf[8:], o.session)
	return buf[:]
}

// accepts is the apply rule: the next number, in the same session or the
// start of a new one. A pure function of the record and the proposal, so
// every node decides alike.
func (o ingestOrder) accepts(p cluster.IngestOrder) bool {
	return p.Seq == o.seq+1 && (p.SessionStart || p.Session == o.session)
}

// IngestOrder returns the last accepted (seq, session) for id. The ingest
// worker numbers a new session's proposals from seq+1.
func (s *Storage) IngestOrder(id string) (seq, session uint64) {
	_ = s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(ingestOrderBucket); b != nil {
			o := decodeIngestOrder(b.Get([]byte(id)))
			seq, session = o.seq, o.session
		}
		return nil
	})
	return seq, session
}

// IngestRejectedTotal is the number of sequenced ingest proposals this node
// has rejected at apply since it opened.
func (s *Storage) IngestRejectedTotal() int64 { return s.ingestRejectedTotal.Load() }

// TakeIngestRejected reports whether the entry at index was a sequenced
// ingest proposal rejected at apply, and forgets it. The apply notifier
// calls it for every applied entry, so the set holds at most one Ready's
// worth of indexes.
func (s *Storage) TakeIngestRejected(index uint64) bool {
	_, ok := s.ingestRejected.LoadAndDelete(index)
	return ok
}

// orderVerdicts decides, for each entry of a batch, whether it is a
// sequenced ingest proposal to reject. Entries not yet in the event log are
// decided by the rule, against the stored records plus this batch's earlier
// acceptances; an entry already in the event log (a replay after a crash
// between the event append and the applied-index save) was decided when it
// was appended, and its stored form records the decision: a rejected entry
// was stored without its data.
func (s *Storage) orderVerdicts(entries []*pb.Entry) (map[uint64]bool, error) {
	var rejected map[uint64]bool
	overlay := map[string]ingestOrder{}
	record := func(id string) (ingestOrder, error) {
		if o, ok := overlay[id]; ok {
			return o, nil
		}
		var o ingestOrder
		err := s.view(func(tx *bolt.Tx) error {
			b := tx.Bucket(ingestOrderBucket)
			if b == nil {
				return ErrBucketMissing
			}
			o = decodeIngestOrder(b.Get([]byte(id)))
			return nil
		})
		return o, err
	}
	for _, e := range entries {
		if e.GetType() != pb.EntryNormal || e.Data == nil {
			continue
		}
		po, err := cluster.IngestOrderFromProposal(e.Data)
		if err != nil {
			// Not decodable here; applyCommitted's full decode reports it.
			continue
		}
		if po.Seq == 0 || po.IngestableID == "" {
			continue
		}
		o, err := record(po.IngestableID)
		if err != nil {
			return nil, err
		}
		var reject bool
		if e.GetIndex() <= s.eventIndex.Load() {
			reject, err = s.storedWithoutData(e.GetIndex())
			if err != nil {
				return nil, err
			}
		} else {
			reject = !o.accepts(po)
		}
		if reject {
			if rejected == nil {
				rejected = map[uint64]bool{}
			}
			rejected[e.GetIndex()] = true
			continue
		}
		if po.Seq > o.seq {
			overlay[po.IngestableID] = ingestOrder{seq: po.Seq, session: po.Session}
		}
	}
	return rejected, nil
}

// storedWithoutData reports whether the event log holds the entry at index
// without its data: the stored form of a rejected sequenced proposal. An
// entry missing from the log was not rejected (only data an accepted entry
// carried can have been scrubbed since).
func (s *Storage) storedWithoutData(index uint64) (bool, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	cursor := s.eventLog.entries.NewEntryCursor(index)
	defer func() { _ = cursor.Close() }()
	stored, err := exactEntry(cursor, index)
	if err != nil {
		if err == ErrActualNotFound {
			return false, nil
		}
		return false, fmt.Errorf("[wal.ingest-order] read event %d: %w", index, err)
	}
	return stored.Data == nil, nil
}

// withoutData is the stored form of a rejected entry: same index, term and
// type, no data. Every event-log reader skips an entry without data, as it
// skips raft's empty leader entries.
func withoutData(e *pb.Entry) *pb.Entry {
	return &pb.Entry{Term: e.Term, Index: e.Index, Type: e.Type}
}

// noteIngestRejected logs and remembers a rejected entry for the apply
// notifier.
func (s *Storage) noteIngestRejected(e *pb.Entry) {
	s.ingestRejected.Store(e.GetIndex(), struct{}{})
	s.ingestRejectedTotal.Add(1)
	po, _ := cluster.IngestOrderFromProposal(e.Data)
	s.logger.Info("ingest: rejected an out-of-order proposal at apply; stored as an empty entry, the worker restarts from its durable position",
		zap.String("ingestable", po.IngestableID),
		zap.Uint64("index", e.GetIndex()),
		zap.Uint64("ingestSeq", po.Seq),
		zap.Uint64("ingestSession", po.Session),
		zap.Bool("sessionStart", po.SessionStart))
}

// advanceIngestOrder records an accepted sequenced proposal, within the
// caller's bbolt transaction. Monotonic, so a replayed entry is a no-op.
func advanceIngestOrder(tx *bolt.Tx, id string, seq, session uint64) error {
	if id == "" || seq == 0 {
		return nil
	}
	b := tx.Bucket(ingestOrderBucket)
	if b == nil {
		return ErrBucketMissing
	}
	if decodeIngestOrder(b.Get([]byte(id))).seq >= seq {
		return nil
	}
	if err := b.Put([]byte(id), ingestOrder{seq: seq, session: session}.encode()); err != nil {
		return fmt.Errorf("[wal.ingest-order] put: %w", err)
	}
	return nil
}
