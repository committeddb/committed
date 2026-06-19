package wal

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// Event-log tombstones drive the RTBF scrubber. When a user-defined (topic)
// delete entity applies, we record the delete's raft index against its
// (type, key) here. A later Scrub command's background rewrite consults this
// to physically remove the subject's PII from the permanent event log.
//
// Why store the full set of delete indices (not just the latest):
//
//   - The scrubber removes an upsert at event-log index I for (type, key) only
//     when there is a delete D for that same (type, key) with I < D <= B, where
//     B is the Scrub command's freeze line. This preserves data legitimately
//     re-created after a delete (an upsert after the last delete <= B survives),
//     and it deletes exactly what a downstream syncable would have deleted.
//   - The rewrite runs on a background goroutine, so the bucket may have
//     accumulated deletes with index > B by the time the worker reads it.
//     Keeping every delete index and filtering to <= B makes the selection a
//     pure function of (stored indices, B) — identical on every replica
//     regardless of when each node's worker happens to run. A single "latest"
//     value would be timing-dependent and could differ across nodes.
//
// Encoding: the bucket key is `typeID + 0x00 + key`; the value is the delete
// indices as concatenated big-endian uint64s, kept ascending. Deletes for one
// (type, key) apply in raft-index order, so appending preserves the ordering.

// tombstoneKey builds the bucket key for a (type, key) pair. The 0x00 separator
// is unambiguous because typeIDs are UUID strings (no NUL bytes); the key bytes
// follow verbatim.
func tombstoneKey(typeID string, key []byte) []byte {
	out := make([]byte, 0, len(typeID)+1+len(key))
	out = append(out, typeID...)
	out = append(out, 0)
	out = append(out, key...)
	return out
}

// isUserDefinedType reports whether id is a user-defined (topic) type — i.e.
// not a built-in committed type. Only user-defined entities carry application
// PII that RTBF scrubs; built-in config and coordination entities are never
// tombstoned. It is the negation of cluster.IsInternal — the single source of
// truth for the user-vs-internal line (the systemTypes registry) — so it stays
// correct as built-ins are added without editing this predicate.
func isUserDefinedType(id string) bool {
	return !cluster.IsInternal(id)
}

// recordEventTombstone appends deleteIndex to the (type, key)'s tombstone list.
// Called from ApplyCommitted for every applied user-defined delete entity, in a
// short bbolt transaction (same shape as the other apply-path bucket writes).
// Deterministic: every replica applies the same committed deletes in the same
// order, so the stored lists are byte-identical.
func (s *Storage) recordEventTombstone(typeID string, key []byte, deleteIndex uint64) error {
	tk := tombstoneKey(typeID, key)
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(eventTombstoneBucket)
		if b == nil {
			return ErrBucketMissing
		}
		existing := b.Get(tk)
		// Dedupe the only repeat that can occur: the same (type, key) deleted
		// twice within one entry (identical raft index). Across entries the
		// index strictly increases, so the list stays ascending by appending.
		if n := len(existing); n >= 8 && binary.BigEndian.Uint64(existing[n-8:]) == deleteIndex {
			return nil
		}
		// bbolt values are only valid for the transaction's lifetime and must
		// not be aliased into the new value — copy before appending.
		out := make([]byte, 0, len(existing)+8)
		out = append(out, existing...)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], deleteIndex)
		out = append(out, buf[:]...)
		return b.Put(tk, out)
	})
}

// tombstoneSelections scans the tombstone bucket and returns, per
// `typeID + 0x00 + key`, the maximum delete index that is <= bound (0 if a
// (type, key) has no delete within the freeze line). The scrubber removes an
// upsert at event-log index I when I < selection[key] for its (type, key) —
// see the package comment above for why "max delete index <= bound" is the
// correct, determinism-safe predicate. Keys with only deletes > bound are
// omitted (they have nothing to remove yet).
func (s *Storage) tombstoneSelections(bound uint64) (map[string]uint64, error) {
	sel := make(map[string]uint64)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(eventTombstoneBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.ForEach(func(k, v []byte) error {
			var maxLE uint64
			for off := 0; off+8 <= len(v); off += 8 {
				d := binary.BigEndian.Uint64(v[off : off+8])
				if d <= bound && d > maxLE {
					maxLE = d
				}
			}
			if maxLE > 0 {
				sel[string(k)] = maxLE
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return sel, nil
}
