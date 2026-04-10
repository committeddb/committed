package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
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

// saveSyncable persists a syncable Configuration to bbolt and then notifies
// the consumer channel. The channel send happens AFTER the bbolt Update
// returns successfully — it must not happen inside the closure, because:
//
//  1. The unbuffered channel send would block while holding the bbolt
//     writer lock, blocking all other writes against any bucket.
//  2. If the tx commit failed *after* the send, the consumer would have
//     been notified about state that doesn't exist on disk.
//  3. The consumer (db.listenForSyncables) calls db.Sync, which can re-enter
//     the proposal path; that would deadlock under the writer lock.
//
// Idempotency on restart depends on the persisted appliedIndex (see
// wal.Storage.ApplyCommitted). Without it, replay would re-spawn a worker
// for the same ID — which is a separate bug tracked in
// .claude-scratch/tickets/worker-registry-by-id.md.
func (s *Storage) saveSyncable(t *cluster.Configuration) error {
	var syncable cluster.Syncable
	err := s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable] marshal: %w", err)
		}

		_, parsed, err := s.parser.ParseSyncable(t.MimeType, t.Data, s)
		if err != nil {
			return fmt.Errorf("[wal.syncable] parseSyncable: %w", err)
		}
		syncable = parsed

		if err := b.Put([]byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.syncable] put: %w", err)
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
	return s.keyValueStorage.Update(func(tx *bolt.Tx) error {
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

	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
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
