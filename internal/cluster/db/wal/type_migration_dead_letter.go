package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleTypeMigrationDeadLetter applies a committed TypeMigrationDeadLetter
// entity: it records the migration-failure pointer in bbolt so it can be
// queried later (GET /type/{id}/migration-errors). Same contract as
// handleSyncableDeadLetter: the record is content-deterministic (the leader
// stamped the timestamp and message at propose time), so every replica
// stores identical bytes, and re-keyed by the failed proposal's raft index a
// re-propose simply overwrites the same row. Two always-current syncables
// tripping over the same proposal converge on one record per (type, index).
func (s *Storage) handleTypeMigrationDeadLetter(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		typeID, index, ok := cluster.DecodeTypeMigrationDeadLetterKey(e.Key)
		if !ok {
			return fmt.Errorf("[wal.type-migration-dead-letter] malformed delete key (%d bytes)", len(e.Key))
		}
		return s.deleteTypeMigrationDeadLetter(typeID, index)
	}
	d := &cluster.TypeMigrationDeadLetter{}
	if err := d.Unmarshal(e.Data); err != nil {
		return err
	}
	return s.saveTypeMigrationDeadLetter(d)
}

// deleteTypeMigrationDeadLetter removes the record at a specific raft index
// from a type's nested bucket (used by migration retry after the fixed
// program succeeds). A missing type id or key is a no-op.
func (s *Storage) deleteTypeMigrationDeadLetter(typeID string, index uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		top := tx.Bucket(typeMigrationDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(typeID))
		if sub == nil {
			return nil
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], index)
		return sub.Delete(key[:])
	})
}

func (s *Storage) saveTypeMigrationDeadLetter(d *cluster.TypeMigrationDeadLetter) error {
	return s.update(func(tx *bolt.Tx) error {
		top := tx.Bucket(typeMigrationDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		// Invariant: a migration dead-letter persists only while its type config
		// exists. An always-current syncable proposes these on a migration failure,
		// and the write can commit AFTER the type was deleted (ProposeDeleteType) — or
		// replay from the log before a same-id recreate. Without this guard that write
		// re-establishes an orphaned sub-bucket a same-id type recreate consults
		// (HasTypeMigrationDeadLetter) and then silently skips those indices. So if the
		// config is gone, drop the stale bump and reap any lingering sub-bucket.
		// Deterministic: config existence is replicated state applied in identical log
		// order on every node. Mirrors saveSyncableDeadLetter.
		if !configExists(tx, typeBucket, []byte(d.TypeID)) {
			if top.Bucket([]byte(d.TypeID)) != nil {
				return top.DeleteBucket([]byte(d.TypeID))
			}
			return nil
		}
		// One nested sub-bucket per type id keeps the query a tight
		// per-type cursor scan and the keyspace naturally partitioned.
		sub, err := top.CreateBucketIfNotExists([]byte(d.TypeID))
		if err != nil {
			return fmt.Errorf("[wal.type-migration-dead-letter] create sub-bucket: %w", err)
		}
		bs, err := d.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.type-migration-dead-letter] marshal: %w", err)
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], d.Index)
		if err := sub.Put(key[:], bs); err != nil {
			return fmt.Errorf("[wal.type-migration-dead-letter] put: %w", err)
		}
		return nil
	})
}

// HasTypeMigrationDeadLetter reports whether a migration dead-letter record
// exists for the proposal at raft index in type id. Migration retry consults
// it so a retry targets only a recorded failure. An unknown type id returns
// false.
func (s *Storage) HasTypeMigrationDeadLetter(typeID string, index uint64) (bool, error) {
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(typeMigrationDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(typeID))
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

// TypeMigrationDeadLetters returns the migration-failure records for a type
// in ascending raft-index order. `since` is an EXCLUSIVE cursor: only
// records with raft index strictly greater than `since` are returned, so
// paging forward is "pass the last index you saw." `limit` bounds the page
// (clamped to [1, maxDeadLetterPageLimit]). An unknown type id returns an
// empty slice, not an error.
func (s *Storage) TypeMigrationDeadLetters(typeID string, since uint64, limit int) ([]cluster.TypeMigrationDeadLetter, error) {
	if limit <= 0 || limit > maxDeadLetterPageLimit {
		limit = maxDeadLetterPageLimit
	}

	var out []cluster.TypeMigrationDeadLetter
	err := s.view(func(tx *bolt.Tx) error {
		top := tx.Bucket(typeMigrationDeadLetterBucket)
		if top == nil {
			return ErrBucketMissing
		}
		sub := top.Bucket([]byte(typeID))
		if sub == nil {
			// No migration failures ever recorded for this type.
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
			d := cluster.TypeMigrationDeadLetter{}
			if err := d.Unmarshal(v); err != nil {
				return fmt.Errorf("[wal.type-migration-dead-letter] unmarshal type=%s index=%d: %w", typeID, binary.BigEndian.Uint64(k), err)
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
