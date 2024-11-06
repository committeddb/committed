package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleType(e *cluster.Entity) error {
	// fmt.Printf("[wal.type] saving type...\n")
	if e.IsDelete() {
		return s.deleteType(e.Key)
	} else {
		t := &cluster.Type{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveType(t)
		// fmt.Printf("[wal.type] ... type saved\n")
	}
}

func (s *Storage) saveType(t *cluster.Type) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return err
		}
		return b.Put([]byte(t.ID), bs)
	})
}

func (s *Storage) deleteType(id []byte) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.Delete(id)
	})
}

func (s *Storage) Type(id string) (*cluster.Type, error) {
	t := &cluster.Type{}

	return t, s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs := b.Get([]byte(id))
		if bs == nil {
			return fmt.Errorf("%w: %s", ErrTypeMissing, id)
		}
		return t.Unmarshal(bs)
	})
}

func (s *Storage) Types() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}

		err := b.ForEach(func(k, v []byte) error {
			tipe := &cluster.Type{}
			err := tipe.Unmarshal(v)
			if err != nil {
				return err
			}

			cfg := &cluster.Configuration{
				ID:       tipe.ID,
				MimeType: "text/toml",
				Data:     []byte(fmt.Sprintf("[type]\nname = \"%s\"", tipe.Name)),
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
