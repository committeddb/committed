package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// The ingest dedup record, keyed by ingestable id: the source-transaction
// identity of the LAST applied ingest proposal plus the highest SourceSeq
// applied within that transaction. Dedup is TRANSACTION-scoped: a re-emitted
// part of the same source transaction (same txnID, seq at or below the
// highwater) is a duplicate; a proposal from a DIFFERENT transaction is
// never compared against another transaction's coordinates — which is what
// makes post-failover coordinates (a different binlog lineage encoding
// below the old highwater) and the compressed-transaction sub-inflation
// harmless instead of silent-loss holes.
//
// Value encoding: [8-byte seq BE][txnID bytes]. A value of exactly 8 bytes
// decodes as txnID "" — both the pre-upgrade legacy format and the regime
// for proposals that carry no transaction identity. txnID "" selects the
// LEGACY SCALAR semantics (global monotonic highwater), so replaying a
// pre-upgrade log reproduces pre-upgrade behavior bit-for-bit, and an old
// binary reading a v2 value sees len != 8 and reports 0 ("dedup nothing") —
// a safe degradation (possible duplicates on idempotent keyed sinks during
// a mixed-version window, never loss).

// decodeSourceDedup splits a stored dedup value; nil/short input decodes as
// (0, "").
func decodeSourceDedup(raw []byte) (seq uint64, txnID string) {
	if len(raw) < 8 {
		return 0, ""
	}
	return binary.BigEndian.Uint64(raw[:8]), string(raw[8:])
}

func encodeSourceDedup(seq uint64, txnID string) []byte {
	buf := make([]byte, 8+len(txnID))
	binary.BigEndian.PutUint64(buf[:8], seq)
	copy(buf[8:], txnID)
	return buf
}

// IngestSourceDedup returns the transaction-scoped dedup record for id: the
// last applied source-transaction identity ("" = legacy scalar regime) and
// the seq highwater within it. Read by the ingest worker before proposing —
// read-through to the committed bucket state so a node that has just become
// leader sees the value its apply path advanced.
func (s *Storage) IngestSourceDedup(id string) (txnID string, seq uint64) {
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestSourceSeqBucket)
		if b == nil {
			return nil
		}
		seq, txnID = decodeSourceDedup(b.Get([]byte(id)))
		return nil
	})
	return txnID, seq
}

// IngestSourceSeqHighwater returns the seq half of the dedup record — the
// highest applied source sequence within the LAST applied transaction (or
// the global highwater while the record is in the legacy scalar regime).
// Test convenience only (dedup assertions predating the record); production
// consumes IngestSourceDedup, which carries the transaction scope — note
// this value can go DOWN across a transaction change, so it must never be
// surfaced as a monotonic gauge.
func (s *Storage) IngestSourceSeqHighwater(id string) uint64 {
	_, seq := s.IngestSourceDedup(id)
	return seq
}

// advanceIngestSourceSeq folds one applied ingest proposal into the dedup
// record. Called from ApplyCommitted for every applied ingest proposal,
// BEFORE saveAppliedIndex, so a crash that replays the entry re-applies
// idempotently. Semantics:
//
//   - same transaction as stored (txnID equal, including ""=="" for the
//     legacy/no-identity regime): monotonic max — the within-transaction
//     part highwater.
//   - different transaction: OVERWRITE (seq, txnID) — the record resets to
//     the new transaction. A later transaction's coordinates are never
//     compared against an earlier one's, so an inflated highwater cannot
//     swallow a following transaction, and a new lineage's low coordinates
//     cannot be shadowed by the old lineage's high ones.
//
// Deterministic: a pure fold of (stored record, proposal) applied in log
// order — every node computes the same record.
func (s *Storage) advanceIngestSourceSeq(id string, seq uint64, txnID string) error {
	if id == "" || seq == 0 {
		return nil
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestSourceSeqBucket)
		if b == nil {
			return ErrBucketMissing
		}
		// Invariant: the record tracks a live ingestable only. An ingest proposal
		// can apply after the config was deleted (an in-flight proposal committing
		// post-delete, or an old proposal replayed from the log before a same-id
		// recreate). Without this guard it re-establishes an orphaned record that
		// makes a same-id recreate's re-emitted CDC proposals get dropped pre-raft.
		// So if the config is gone, drop the advance and reap any lingering value.
		// Deterministic: config existence is replicated state applied in identical
		// log order.
		if !configExists(tx, ingestableBucket, []byte(id)) {
			return b.Delete([]byte(id))
		}
		curSeq, curTxn := decodeSourceDedup(b.Get([]byte(id)))
		if txnID == curTxn && seq <= curSeq {
			return nil
		}
		if txnID == curTxn {
			// Same transaction: advance the part highwater in place.
			if err := b.Put([]byte(id), encodeSourceDedup(seq, txnID)); err != nil {
				return fmt.Errorf("[wal.ingest-source-seq] put: %w", err)
			}
			return nil
		}
		// New transaction: reset the record to it.
		if err := b.Put([]byte(id), encodeSourceDedup(seq, txnID)); err != nil {
			return fmt.Errorf("[wal.ingest-source-seq] put: %w", err)
		}
		return nil
	})
}
