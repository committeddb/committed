package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleSyncableStuck applies a committed SyncableStuck entity: a delete
// clears the syncable's stuck record, an upsert replaces it. One entry per
// syncable id (last-writer-wins) — the worker publishes "blocked on index N"
// and deletes it on progress.
func (s *Storage) handleSyncableStuck(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteKeyed(syncableStuckBucket, e.Key)
	}
	st := &cluster.SyncableStuck{}
	if err := st.Unmarshal(e.Data); err != nil {
		return err
	}
	return s.putKeyed(syncableStuckBucket, []byte(st.ID), e.Data)
}

// SyncableStuck returns the syncable's current stuck record, or ok=false if
// it isn't blocked. Replicated, so any node answers identically.
func (s *Storage) SyncableStuck(id string) (cluster.SyncableStuck, bool, error) {
	var (
		out cluster.SyncableStuck
		ok  bool
	)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableStuckBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if err := out.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.stuck] unmarshal id=%s: %w", id, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return cluster.SyncableStuck{}, false, err
	}
	return out, ok, nil
}

// handleSyncableSkipRequest applies a committed SyncableSkipRequest entity:
// a delete clears the request, an upsert records it. One entry per syncable
// id — the endpoint proposes it, the worker deletes it once honored or stale.
func (s *Storage) handleSyncableSkipRequest(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteKeyed(syncableSkipRequestBucket, e.Key)
	}
	r := &cluster.SyncableSkipRequest{}
	if err := r.Unmarshal(e.Data); err != nil {
		return err
	}
	return s.putKeyed(syncableSkipRequestBucket, []byte(r.ID), e.Data)
}

// SyncableSkipRequest returns the pending operator skip request for the
// syncable, or ok=false if none. The worker reads it while blocked and
// honors it only when its Index matches the proposal currently wedged.
func (s *Storage) SyncableSkipRequest(id string) (cluster.SyncableSkipRequest, bool, error) {
	var (
		out cluster.SyncableSkipRequest
		ok  bool
	)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableSkipRequestBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if err := out.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.stuck] unmarshal skip-request id=%s: %w", id, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return cluster.SyncableSkipRequest{}, false, err
	}
	return out, ok, nil
}

// putKeyed writes value under key in the named flat bucket.
func (s *Storage) putKeyed(bucket, key, value []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.Put(key, value)
	})
}

// deleteKeyed removes key from the named flat bucket (no-op if absent).
func (s *Storage) deleteKeyed(bucket, key []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.Delete(key)
	})
}
