package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// IngestSourceSeqHighwater returns the highest applied source sequence
// for the given ingestable id, or 0 if the id has no recorded highwater
// (never ingested, or pre-feature). Read by the ingest worker before
// proposing so it can skip re-emitted proposals (effectively-once
// ingest); read-through to the committed bucket state so a node that
// has just become leader sees the value its apply path advanced.
func (s *Storage) IngestSourceSeqHighwater(id string) uint64 {
	var hw uint64
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestSourceSeqBucket)
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(id))
		if len(raw) == 8 {
			hw = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	return hw
}

// advanceIngestSourceSeq raises the highwater for id to seq when seq is
// greater than the stored value (monotonic max). Called from
// ApplyCommitted for every applied ingest proposal, BEFORE
// saveAppliedIndex, so a crash that replays the entry re-applies the max
// idempotently. The max guard also makes out-of-order or duplicate
// applies (which shouldn't happen, but defense in depth) harmless.
//
// Runs in its own bbolt transaction. That is not atomic with the
// entity write, but the operation is idempotent (max) and gated by the
// same appliedIndex replay barrier as the entity write, so a partial
// crash re-runs the whole apply and converges to the same state.
func (s *Storage) advanceIngestSourceSeq(id string, seq uint64) error {
	if id == "" || seq == 0 {
		return nil
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestSourceSeqBucket)
		if b == nil {
			return ErrBucketMissing
		}
		// Invariant: the highwater tracks a live ingestable only. An ingest proposal
		// can apply after the config was deleted (an in-flight proposal committing
		// post-delete, or an old proposal replayed from the log before a same-id
		// recreate). Without this guard it re-establishes an orphaned highwater that
		// makes a same-id recreate's re-emitted CDC proposals get dropped pre-raft
		// (SourceSeq <= highwater — see IngestSourceSeqHighwater's caller). So if the
		// config is gone, drop the advance and reap any lingering value. Deterministic:
		// config existence is replicated state applied in identical log order.
		if !configExists(tx, ingestableBucket, []byte(id)) {
			return b.Delete([]byte(id))
		}
		if raw := b.Get([]byte(id)); len(raw) == 8 {
			if cur := binary.BigEndian.Uint64(raw); seq <= cur {
				return nil
			}
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], seq)
		if err := b.Put([]byte(id), buf[:]); err != nil {
			return fmt.Errorf("[wal.ingest-source-seq] put: %w", err)
		}
		return nil
	})
}
