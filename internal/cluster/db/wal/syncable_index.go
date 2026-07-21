package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

func (s *Storage) handleSyncableIndex(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		return s.deleteSyncableIndex(e.Key)
	} else {
		t := &cluster.SyncableIndex{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveSyncableIndex(t)
	}
}

func (s *Storage) saveSyncableIndex(t *cluster.SyncableIndex) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}

		// Invariant: a checkpoint persists only while its syncable config exists.
		// A worker submits its checkpoint via an async consensus bump
		// (proposeSyncableIndex) that can commit AFTER the config was deleted;
		// without this guard the Put would re-establish an orphaned SyncableIndex
		// (config gone, checkpoint lingering) that a later same-id recreate resumes
		// from, silently skipping history. So if the config is gone, drop the stale
		// bump — and clear any lingering value, reaping an orphan rather than
		// creating one. Deterministic: config existence is replicated state applied
		// in identical log order on every node. (Rebuild keeps the config, so its
		// separate worker-drain fix is unaffected — the config is present here and
		// the Put proceeds.)
		if !configExists(tx, syncableBucket, []byte(t.ID)) {
			return b.Delete([]byte(t.ID))
		}

		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable-index] marshal: %w", err)
		}

		err = b.Put([]byte(t.ID), bs)
		if err != nil {
			return fmt.Errorf("[wal.syncable-index] put: %w", err)
		}

		return nil
	})
}

func (s *Storage) deleteSyncableIndex(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		err := b.Delete(id)
		if err != nil {
			return err
		}

		return nil
	})
}

// GetSyncableIndex returns the highest raft entry index that the named
// syncable's worker has finished syncing. Tests use this as a barrier:
// once it has advanced past the persisted lastIndex, the sync worker is
// caught up and follow-up reads against the destination database don't
// race with in-flight INSERTs.
func (s *Storage) GetSyncableIndex(id string) (uint64, error) {
	return s.getSyncableIndex(id)
}

func (s *Storage) getSyncableIndex(id string) (uint64, error) {
	index := &cluster.SyncableIndex{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}

		bs := b.Get([]byte(id))

		err := index.Unmarshal(bs)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return index.Index, nil
}
