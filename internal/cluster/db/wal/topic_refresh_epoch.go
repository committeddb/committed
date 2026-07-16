package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// TopicRefreshEpoch returns the highest refresh generation ever committed for a
// topic, or 0 if the topic has never carried a generation-stamped entity. The
// ingest worker reads it at snapshot start as the epoch FLOOR: on a
// delete-and-recreate (same id or a different id producing the same topic) the
// per-id position — and the epoch inside it — is cleared, but the sink still
// holds rows stamped up to this value, so the recreate's snapshot must stamp
// ABOVE it and its closing refresh-boundary marker must sweep them.
//
// Keyed by TOPIC, not ingestable id, and deliberately NOT cleared by
// DeleteIngestable: the generation lives on the topic's sink, whose lifecycle
// outlives any single ingestable incarnation. (A topic has exactly one
// ingestable at a time — enforced at POST — so there is a single writer.) This
// is the intentional inverse of ingestSourceSeq, which is id-scoped; see
// IngestSourceSeqHighwater.
func (s *Storage) TopicRefreshEpoch(topic string) uint64 {
	var hw uint64
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicRefreshEpochBucket)
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(topic))
		if len(raw) == 8 {
			hw = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	return hw
}

// bumpTopicRefreshEpoch raises the highwater for topic to gen when gen is greater
// than the stored value (monotonic max). Called from ApplyCommitted for every
// applied entity carrying a non-zero Generation — data rows AND refresh-boundary
// markers — so the floor tracks the highest generation ever STAMPED, not just the
// highest one MARKED. That closes the crash-mid-snapshot edge: rows stamped at an
// epoch whose marker never committed still raise the floor, so a later recreate
// stamps above them.
//
// Deterministic (derived only from the committed entity) and gated by the same
// appliedIndex replay barrier as the entity write, so a crash that replays the
// entry re-applies the idempotent max. Runs in its own bbolt transaction; the max
// guard makes a partial-crash re-run converge.
func (s *Storage) bumpTopicRefreshEpoch(topic string, gen uint64) error {
	if topic == "" || gen == 0 {
		return nil
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicRefreshEpochBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if raw := b.Get([]byte(topic)); len(raw) == 8 {
			if cur := binary.BigEndian.Uint64(raw); gen <= cur {
				return nil
			}
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], gen)
		if err := b.Put([]byte(topic), buf[:]); err != nil {
			return fmt.Errorf("[wal.topic-refresh-epoch] put: %w", err)
		}
		return nil
	})
}
