package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

func (s *Storage) handleDatabase(e *cluster.Entity) error {
	s.logger.Debug("saving database", zap.String("key", string(e.Key)))
	if e.IsDelete() {
		return s.deleteDatabase(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveDatabase(t)
	}
}

func (s *Storage) saveDatabase(t *cluster.Configuration) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.database] marshal: %w", err)
		}

		// Deterministic state-machine write FIRST: persist the raw config
		// bytes. This depends only on Marshal (not on the node-local parse
		// below), so every replica converges on the same config bucket
		// state regardless of whether THIS node can build the live
		// connection.
		if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.database] putVersioned: %w", err)
		}

		// Node-local construction: build the live Database. ParseDatabase
		// interpolates ${VAR} secrets against this node's environment, so
		// it can fail for node-local reasons a follower's environment
		// differs in. Degrade — record + log, leave the database uncached
		// — instead of returning an error, which the apply path treats as
		// fatal and would crash the node (and, for a freshly-rolled secret
		// the proposing node has but others don't, every follower at once).
		name, db, err := s.parser.ParseDatabase(t.MimeType, t.Data)
		if err != nil {
			s.recordConfigError("database", t.ID, err)
			s.logger.Error("database config persisted but could not be built on this node (degraded); fix the environment and the config will build on next restart",
				zap.String("id", t.ID), zap.Error(err))
			return nil
		}
		s.clearConfigError("database", t.ID)

		s.logger.Debug("database saved", zap.String("id", t.ID), zap.String("name", name))
		s.databases[t.ID] = db

		return nil
	})
}

func (s *Storage) deleteDatabase(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
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

func (s *Storage) loadDatabases() error {
	return s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}

		return forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				return err // genuine corruption — stays fatal
			}

			_, db, err := s.parser.ParseDatabase(cfg.MimeType, cfg.Data)
			if err != nil {
				// Node-local build failure (e.g. missing ${VAR} secret).
				// Degrade: skip caching the connection and keep going,
				// rather than failing startup. Dependent syncables/
				// ingestables will surface connection errors; the operator
				// fixes the env and a restart builds it.
				s.recordConfigError("database", cfg.ID, err)
				s.logger.Error("database config could not be built on this node at startup (degraded)",
					zap.String("id", cfg.ID), zap.Error(err))
				return nil
			}

			s.clearConfigError("database", cfg.ID)
			s.databases[cfg.ID] = db
			return nil
		})
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

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
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

func (s *Storage) DatabaseVersions(id string) ([]cluster.VersionInfo, error) {
	var versions []cluster.VersionInfo
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var err error
		versions, err = listVersions(b, []byte(id))
		return err
	})
	return versions, err
}

func (s *Storage) DatabaseVersion(id string, version uint64) (*cluster.Configuration, error) {
	cfg := &cluster.Configuration{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
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
