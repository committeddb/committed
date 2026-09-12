package wal

import (
	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleSyncableRematerialization applies the in-progress re-materialization
// record: upsert when the verb is accepted, delete when the sweep completes.
// Guarded on the syncable's config still existing (the write-guard half of
// the delete-safety contract); the sweep half rides sweepSyncableSiblingState
// — a deleted syncable's remat record must not linger for a same-id recreate
// to resume a replay nobody asked for.
func (s *Storage) handleSyncableRematerialization(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		return s.update(func(tx *bolt.Tx) error {
			b := tx.Bucket(syncableRematerializationBucket)
			if b == nil {
				return ErrBucketMissing
			}
			return b.Delete(e.Key)
		})
	}
	return s.putConfigGuardedKeyed(syncableBucket, syncableRematerializationBucket, e.Key, e.Data)
}

// SyncableRematerialization implements the db.Storage read: the in-progress
// record for the syncable, or (nil, false) when none exists.
func (s *Storage) SyncableRematerialization(id string) (*cluster.SyncableRematerialization, bool) {
	var raw []byte
	_ = s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(syncableRematerializationBucket); b != nil {
			if v := b.Get([]byte(id)); v != nil {
				raw = append([]byte{}, v...)
			}
		}
		return nil
	})
	if raw == nil {
		return nil, false
	}
	r := &cluster.SyncableRematerialization{}
	if err := r.Unmarshal(raw); err != nil {
		return nil, false
	}
	return r, true
}
