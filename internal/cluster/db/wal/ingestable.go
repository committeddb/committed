package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleIngestable(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteIngestable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveIngestable(t)
	}
}

// saveIngestable persists an ingestable Configuration as a new version in
// bbolt and then notifies the consumer channel. See saveSyncable for the
// rationale on why the channel send happens outside the bbolt Update closure.
func (s *Storage) saveIngestable(t *cluster.Configuration) error {
	var ingestable cluster.Ingestable
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.ingestable] marshal: %w", err)
		}

		_, parsed, err := s.parser.ParseIngestable(t.MimeType, t.Data)
		if err != nil {
			return fmt.Errorf("[wal.ingestable] parseIngestable: %w", err)
		}
		ingestable = parsed

		if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.ingestable] putVersioned: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if s.ingest != nil {
		s.ingest <- &db.IngestableWithID{ID: t.ID, Ingestable: ingestable}
	}

	return nil
}

func (s *Storage) deleteIngestable(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := deleteVersioned(b, id); err != nil {
			return err
		}

		s.databases[string(id)] = nil
		return nil
	})
}

func (s *Storage) Ingestables() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}

		return forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				return err
			}
			cfgs = append(cfgs, cfg)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return cfgs, nil
}

func (s *Storage) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	var versions []cluster.VersionInfo
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var err error
		versions, err = listVersions(b, []byte(id))
		return err
	})
	return versions, err
}

func (s *Storage) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	cfg := &cluster.Configuration{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		data, err := getVersion(b, []byte(id), version)
		if err != nil {
			return err
		}
		return cfg.Unmarshal(data)
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
