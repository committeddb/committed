package wal

import (
	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleSyncable(e *cluster.Entity) error {
	// fmt.Printf("[wal] saving database...\n")
	if e.IsDelete() {
		return s.deleteSyncable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveSyncable(t)
		// fmt.Printf("[wal] ... database saved\n")
	}
}

func (s *Storage) saveSyncable(t *cluster.Configuration) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return err
		}

		_, _, err = s.parser.ParseSyncable(t.MimeType, t.Data, s)
		if err != nil {
			return err
		}

		err = b.Put([]byte(t.ID), bs)
		if err != nil {
			return err
		}

		return nil
	})
}

func (s *Storage) deleteSyncable(id []byte) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
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

func (s *Storage) Syncables() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}

		err := b.ForEach(func(k, v []byte) error {
			cfg := &cluster.Configuration{}
			err := cfg.Unmarshal(v)
			if err != nil {
				return err
			}

			cfgs = append(cfgs, cfg)

			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return cfgs, nil
}
