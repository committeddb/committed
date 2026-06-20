package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/migration"
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
	var built bool
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable] marshal: %w", err)
		}

		// Deterministic state-machine write FIRST: persist the raw config
		// bytes so every replica converges, then attempt the node-local
		// build (which can fail on a missing ${VAR} secret).
		if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.syncable] putVersioned: %w", err)
		}

		_, parsed, parsedMode, err := s.parser.ParseSyncable(t.MimeType, t.Data, s)
		if err != nil {
			// Degrade rather than fail the apply (which would crash the
			// node). The config is persisted; no worker is started until
			// the build succeeds.
			s.recordConfigError("syncable", t.ID, err)
			s.logger.Error("syncable config persisted but could not be built on this node (degraded); fix the environment and the config will build on next restart",
				zap.String("id", t.ID), zap.Error(err))
			return nil
		}
		s.clearConfigError("syncable", t.ID)
		// ModeAlwaysCurrent decorates the user syncable with a
		// migration wrapper so the worker loop stays oblivious to
		// version-upgrade concerns. ModeAsStored hands the syncable
		// through untouched. The wrapper lives in the migration
		// package next to the chain it uses.
		if parsedMode == cluster.ModeAlwaysCurrent {
			parsed = migration.Wrap(parsed, s, s.metrics)
		}
		syncable = parsed
		built = true

		return nil
	})
	if err != nil {
		return err
	}

	if built && s.sync != nil {
		s.logger.Debug("sending syncable to channel", zap.String("id", t.ID))
		s.sync <- &db.SyncableWithID{ID: t.ID, Syncable: syncable}
	}

	return nil
}

// deleteSyncable removes a syncable's persisted config bytes and then notifies
// the DB layer so it can cancel the worker and (on the owner) tear down the
// destination table. The channel send happens AFTER the bbolt Update returns,
// for the same three reasons saveSyncable documents (no send under the writer
// lock, no notify before the commit is durable, no re-entrant deadlock).
//
// The DB layer does the teardown, not this wal layer (the wal layer must not
// touch the destination DB), and it reuses the worker's already-built syncable
// handle rather than re-parsing here — so the delete signal carries only the
// ID.
func (s *Storage) deleteSyncable(id []byte) error {
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return deleteVersioned(b, id)
	})
	if err != nil {
		return err
	}

	if s.sync != nil {
		s.logger.Debug("sending syncable delete to channel", zap.String("id", string(id)))
		s.sync <- &db.SyncableWithID{ID: string(id), Delete: true}
	}

	return nil
}

// RestoreSyncableWorkers walks the syncable bucket and re-sends each persisted
// syncable to the supervisor's sync channel so a restarted node spawns workers
// for them. It is the syncable twin of RestoreIngestableWorkers.
//
// Why this is needed: on a clean restart, ApplyCommitted's idempotency guard
// (entry.Index <= appliedIndex) means handleSyncable is NOT re-called, so the
// only thing that ever sends on s.sync (saveSyncable, on the apply path) does
// not fire. Without an explicit restore, a previously-configured syncable's
// worker never respawns and it silently stops syncing until the config is
// re-applied.
//
// ORDERING CONTRACT (identical to RestoreIngestableWorkers): the caller MUST
// have registered the syncable sub-parsers (Parser.AddSyncableParser "sql" /
// "http") AND started the channel consumer (db.New's listenForSyncables drains
// s.sync) before calling this. Open deliberately does NOT auto-spawn it: when
// the ingestable side did, the goroutine raced the caller's parser
// registration and, on a loaded machine, usually lost — every syncable then
// failed ParseSyncable with "cannot parse syncable of type: sql", was logged
// as a (silent, under the default Nop logger) degraded parse, and skipped, so
// the restarted node never resumed syncing. Run it once setup is complete
// instead (cmd/node spawns `go s.RestoreSyncableWorkers()` after the parsers
// are wired). It also races the apply path: a config re-applied on restart
// (handleSyncable) re-sends the same syncable, but db.Sync's replace-by-id
// collapses the duplicate to a single worker, so last-writer-wins.
//
// Errors here are warnings, not fatals: a corrupted single config shouldn't
// stop the rest from running. The dialect will surface a real connection or
// schema error in its own retry loop later.
func (s *Storage) RestoreSyncableWorkers() {
	if s.sync == nil {
		return
	}
	cfgs, err := s.Syncables()
	if err != nil {
		s.logger.Warn("restoreSyncableWorkers: list syncables", zap.Error(err))
		return
	}
	for _, cfg := range cfgs {
		_, syncable, mode, err := s.parser.ParseSyncable(cfg.MimeType, cfg.Data, s)
		if err != nil {
			// Degraded: record so the build-errors gauge reflects the
			// build path too (validateConfigSecrets uses the cheaper
			// Validate; a config can pass that but fail the full parse).
			s.recordConfigError("syncable", cfg.ID, err)
			s.logger.Warn("restoreSyncableWorkers: parse (degraded)",
				zap.String("id", cfg.ID), zap.Error(err))
			continue
		}
		s.clearConfigError("syncable", cfg.ID)
		// Mirror saveSyncable: ModeAlwaysCurrent decorates the syncable
		// with the migration wrapper so the worker loop stays oblivious
		// to version-upgrade concerns.
		if mode == cluster.ModeAlwaysCurrent {
			syncable = migration.Wrap(syncable, s, s.metrics)
		}
		s.sync <- &db.SyncableWithID{ID: cfg.ID, Syncable: syncable}
	}
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
