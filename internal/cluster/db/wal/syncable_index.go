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
		// fmt.Printf("[wal.syncable-index] index: %#v\n", t)
		return s.saveSyncableIndex(t)
	}
}

func (s *Storage) saveSyncableIndex(t *cluster.SyncableIndex) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
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
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
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

func (s *Storage) getSyncableIndex(id string) (uint64, error) {
	index := &cluster.SyncableIndex{}
	err := s.typeStorage.View(func(tx *bolt.Tx) error {
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
