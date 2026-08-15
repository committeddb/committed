package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// maxDeadLetterPageLimit caps how many records a single
// SyncableDeadLetters query returns, regardless of the caller's
// requested limit. Dead letters are rare in healthy operation, so a
// page of 1000 is generous; the cap exists to bound the response of a
// query against a syncable that has been failing every proposal.
const maxDeadLetterPageLimit = 1000

// handleSyncableDeadLetter applies a committed SyncableDeadLetter entity:
// it records the dead-letter pointer in bbolt so it can be queried later.
// The record is content-deterministic (the leader stamped the timestamp,
// kind, and message at propose time), so every replica stores identical
// bytes and the records ride along in snapshots.
//
// Dead letters are append-only in practice — keyed by the failed
// proposal's raft index, a re-propose after a crash-replay simply
// overwrites the same key with a fresh timestamp rather than duplicating
// the row.
func (s *Storage) handleSyncableDeadLetter(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		id, index, ok := cluster.DecodeSyncableDeadLetterKey(e.Key)
		if !ok {
			return fmt.Errorf("[wal.dead-letter] malformed delete key (%d bytes)", len(e.Key))
		}
		return s.deleteSyncableDeadLetter(id, index)
	}
	d := &cluster.SyncableDeadLetter{}
	if err := d.Unmarshal(e.Data); err != nil {
		return err
	}
	return s.saveSyncableDeadLetter(d)
}

// deleteSyncableDeadLetter removes the record at a specific raft index from a
// syncable's nested bucket (used by replay after a successful re-sync). A
// missing id or key is a no-op.
func (s *Storage) deleteSyncableDeadLetter(id string, index uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(id))
		if sub == nil {
			return nil
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], index)
		return sub.Delete(key[:])
	})
}

func (s *Storage) saveSyncableDeadLetter(d *cluster.SyncableDeadLetter) error {
	return s.update(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		// Invariant: a dead-letter persists only while its syncable config exists.
		// The sync worker (or an operator's manual dead-letter) proposes these, and
		// DeleteSyncable does not drain the worker first, so a dead-letter can commit
		// AFTER the config was deleted — or an old one can replay from the log before
		// a same-id recreate. Without this guard that write re-establishes an orphaned
		// sub-bucket a same-id recreate consults (HasSyncableDeadLetter) and then
		// silently skips those indices. So if the config is gone, drop the stale bump
		// and reap any lingering sub-bucket. Deterministic: config existence is
		// replicated state applied in identical log order on every node.
		if !configExists(tx, syncableBucket, []byte(d.ID)) {
			if top.Bucket([]byte(d.ID)) != nil {
				return top.DeleteBucket([]byte(d.ID))
			}
			return nil
		}
		// One nested sub-bucket per syncable id keeps the query a tight
		// per-syncable cursor scan and the keyspace naturally partitioned.
		sub, err := top.CreateBucketIfNotExists([]byte(d.ID))
		if err != nil {
			return fmt.Errorf("[wal.dead-letter] create sub-bucket: %w", err)
		}
		bs, err := d.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.dead-letter] marshal: %w", err)
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], d.Index)
		if err := sub.Put(key[:], bs); err != nil {
			return fmt.Errorf("[wal.dead-letter] put: %w", err)
		}
		return nil
	})
}

// HasSyncableDeadLetter reports whether a dead-letter record exists for the
// proposal at raft index in syncable id. The sync worker consults it before
// (re-)processing a proposal so a skip — permanent or operator "manual" —
// survives restart: a proposal already given up on is excluded rather than
// re-attempted. An unknown syncable id returns false.
func (s *Storage) HasSyncableDeadLetter(id string, index uint64) (bool, error) {
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(id))
		if sub == nil {
			return nil
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], index)
		found = sub.Get(key[:]) != nil
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// SyncableDeadLetterStats returns how many UNACKNOWLEDGED proposals syncable
// id has dead-lettered (permanently or manually skipped), how many records an
// operator has acknowledged as resolved out-of-band, and the raft index of
// the most recent record of either state (0 when none). This is the
// status-surface summary: a sink can be caught up AND have skipped rows, and
// without the count that state reads as fully green — the honest "complete"
// check is caughtUp && count==0 && workerState=="running". Acknowledged
// records leave `count` (the whole point of the verb: a superseded skip must
// not read permanently red) but stay listable on SyncableDeadLetters as the
// audit trail. A successful replay deletes its record and so leaves both
// numbers. An unknown id returns zeros.
func (s *Storage) SyncableDeadLetterStats(id string) (count, acknowledged, last uint64, err error) {
	err = s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(id))
		if sub == nil {
			return nil
		}
		c := sub.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			d := cluster.SyncableDeadLetter{}
			if err := d.Unmarshal(v); err != nil {
				return fmt.Errorf("[wal.dead-letter] unmarshal id=%s index=%d: %w", id, binary.BigEndian.Uint64(k), err)
			}
			if d.Acknowledged {
				acknowledged++
			} else {
				count++
			}
		}
		if k, _ := c.Last(); k != nil {
			last = binary.BigEndian.Uint64(k)
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	return count, acknowledged, last, nil
}

// SyncableDeadLetterAt returns the record for one (id, index), with ok=false
// when no dead letter exists there — the acknowledge verb's point read.
func (s *Storage) SyncableDeadLetterAt(id string, index uint64) (cluster.SyncableDeadLetter, bool, error) {
	var d cluster.SyncableDeadLetter
	var ok bool
	err := s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(id))
		if sub == nil {
			return nil
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], index)
		v := sub.Get(key[:])
		if v == nil {
			return nil
		}
		if err := d.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.dead-letter] unmarshal id=%s index=%d: %w", id, index, err)
		}
		ok = true
		return nil
	})
	return d, ok, err
}

// SyncableDeadLetters returns the dead-letter records for a syncable in
// ascending raft-index order. `since` is an EXCLUSIVE cursor: only
// records with raft index strictly greater than `since` are returned, so
// paging forward is "pass the last index you saw." `limit` bounds the
// page (clamped to [1, maxDeadLetterPageLimit]). An unknown syncable id
// returns an empty slice, not an error.
func (s *Storage) SyncableDeadLetters(id string, since uint64, limit int) ([]cluster.SyncableDeadLetter, error) {
	if limit <= 0 || limit > maxDeadLetterPageLimit {
		limit = maxDeadLetterPageLimit
	}

	var out []cluster.SyncableDeadLetter
	err := s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(syncableDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(id))
		if sub == nil {
			// No failures ever recorded for this syncable.
			return nil
		}

		c := sub.Cursor()
		// Seek to the first key strictly greater than `since`. Keys are
		// big-endian raft indices, so byte order == numeric order.
		var seek [8]byte
		binary.BigEndian.PutUint64(seek[:], since)
		k, v := c.Seek(seek[:])
		if k != nil && binary.BigEndian.Uint64(k) == since {
			// Seek lands on `since` itself when present; skip it so the
			// cursor stays exclusive.
			k, v = c.Next()
		}
		for ; k != nil && len(out) < limit; k, v = c.Next() {
			d := cluster.SyncableDeadLetter{}
			if err := d.Unmarshal(v); err != nil {
				return fmt.Errorf("[wal.dead-letter] unmarshal id=%s index=%d: %w", id, binary.BigEndian.Uint64(k), err)
			}
			out = append(out, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
