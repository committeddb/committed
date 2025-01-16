package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleIngestable(e *cluster.Entity) error {
	// fmt.Printf("[wal.ingestable] saving ingestable...\n")
	if e.IsDelete() {
		return s.deleteIngestable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveIngestable(t)
		// fmt.Printf("[wal.ingestable] ... ingestable saved\n")
	}
}

func (s *Storage) saveIngestable(t *cluster.Configuration) error {
	return s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.ingestable] marshal: %w", err)
		}

		_, ingestable, err := s.parser.ParseIngestable(t.MimeType, t.Data)
		if err != nil {
			return fmt.Errorf("[wal.ingestable] parseIngestable: %w", err)
		}

		err = b.Put([]byte(t.ID), bs)
		if err != nil {
			return fmt.Errorf("[wal.ingestable] put: %w", err)
		}

		if s.ingest != nil {
			s.ingest <- &db.IngestableWithID{ID: t.ID, Ingestable: ingestable}
		}

		return nil
	})
}

func (s *Storage) deleteIngestable(id []byte) error {
	return s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
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

func (s *Storage) Ingestables() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
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
