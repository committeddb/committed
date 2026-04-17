package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/migration"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func (s *Storage) handleSyncable(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteSyncable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveSyncable(t)
	}
}

// saveSyncable persists a syncable Configuration as a new version in bbolt
// and then notifies the consumer channel. The channel send happens AFTER the
// bbolt Update returns successfully — it must not happen inside the closure,
// because:
//
//  1. The unbuffered channel send would block while holding the bbolt
//     writer lock, blocking all other writes against any bucket.
//  2. If the tx commit failed *after* the send, the consumer would have
//     been notified about state that doesn't exist on disk.
//  3. The consumer (db.listenForSyncables) calls db.Sync, which can re-enter
//     the proposal path; that would deadlock under the writer lock.
func (s *Storage) saveSyncable(t *cluster.Configuration) error {
	var syncable cluster.Syncable
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable] marshal: %w", err)
		}

		_, parsed, parsedMode, err := s.parser.ParseSyncable(t.MimeType, t.Data, s)
		if err != nil {
			return fmt.Errorf("[wal.syncable] parseSyncable: %w", err)
		}
		// ModeAlwaysCurrent decorates the user syncable with a
		// migration wrapper so the worker loop stays oblivious to
		// version-upgrade concerns. ModeAsStored hands the syncable
		// through untouched. The wrapper lives in the migration
		// package next to the chain it uses.
		if parsedMode == cluster.ModeAlwaysCurrent {
			parsed = migration.Wrap(parsed, s)
		}
		syncable = parsed

		if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.syncable] putVersioned: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if s.sync != nil {
		s.logger.Debug("sending syncable to channel", zap.String("id", t.ID))
		s.sync <- &db.SyncableWithID{ID: t.ID, Syncable: syncable}
	}

	return nil
}

func (s *Storage) deleteSyncable(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return deleteVersioned(b, id)
	})
}

func (s *Storage) Syncables() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
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

func (s *Storage) SyncableVersions(id string) ([]cluster.VersionInfo, error) {
	var versions []cluster.VersionInfo
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var err error
		versions, err = listVersions(b, []byte(id))
		return err
	})
	return versions, err
}

func (s *Storage) SyncableVersion(id string, version uint64) (*cluster.Configuration, error) {
	cfg := &cluster.Configuration{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
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
