package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleSyncableIndex(e *cluster.Entity) error {
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
