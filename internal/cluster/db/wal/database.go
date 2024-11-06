package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleDatabase(e *cluster.Entity) error {
	fmt.Printf("[wal.database] saving database...\n")
	if e.IsDelete() {
		return s.deleteDatabase(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		fmt.Printf("[wal.database] ... database saved\n")
		return s.saveDatabase(t)
	}
}

func (s *Storage) saveDatabase(t *cluster.Configuration) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.database] marshal: %w", err)
		}

		name, db, err := s.parser.ParseDatabase(t.MimeType, t.Data)
		if err != nil {
			return fmt.Errorf("[wal.database] parseDatabase: %w", err)
		}

		fmt.Printf("[wal.database] Saved database: %s\n", name)
		err = b.Put([]byte(t.ID), bs)
		if err != nil {
			return fmt.Errorf("[wal.database] put: %w", err)
		}

		s.databases[t.ID] = db

		return nil
	})
}

func (s *Storage) deleteDatabase(id []byte) error {
	return s.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}
		err := b.Delete(id)
		if err != nil {
			return err
		}

		s.databases[string(id)] = nil
		return nil
	})
}

func (s *Storage) loadDatabases() error {
	return s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}

		err := b.ForEach(func(k, v []byte) error {
			cfg := &cluster.Configuration{}
			err := cfg.Unmarshal(v)
			if err != nil {
				return err
			}

			_, db, err := s.parser.ParseDatabase(cfg.MimeType, cfg.Data)
			if err != nil {
				return err
			}

			s.databases[cfg.ID] = db

			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
}

func (s *Storage) Database(id string) (cluster.Database, error) {
	db, ok := s.databases[id]
	if !ok {
		return nil, ErrDatabaseMissing
	}

	return db, nil
}

func (s *Storage) Databases() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
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
