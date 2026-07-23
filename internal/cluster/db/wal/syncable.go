package wal

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/migration"
)

func (s *Storage) handleSyncable(e *cluster.Entity, raftIndex uint64) error {
	if e.IsDelete() {
		return s.deleteSyncable(e.Key, e.KeepData)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveSyncable(t, raftIndex)
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
func (s *Storage) saveSyncable(t *cluster.Configuration, raftIndex uint64) error {
	var syncable cluster.Syncable
	var built bool
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}

		// Replay guard (config-version-replay): ApplyCommittedBatch can replay a
		// whole Ready on a crash-window restart. A versioned apply whose entry
		// index already produced a version is a replay — skip it, or the last+1
		// allocator appends a phantom version, diverging history across replicas.
		// The set below rides this same atomic tx, so a failure rolls both back.
		if versionedLastIndex(b, []byte(t.ID)) >= raftIndex {
			return nil
		}
		if err := setVersionedLastIndex(b, []byte(t.ID), raftIndex); err != nil {
			return err
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable] marshal: %w", err)
		}

		// Deterministic state-machine write FIRST: persist the raw config
		// bytes so every replica converges, then attempt the node-local
		// build (which can fail on a missing ${VAR} secret).
		//
		// Skip the version APPEND on a byte-identical replay: a crash-apply-window
		// entry is re-delivered (entity fsynced, applied-index not), and appending
		// again would duplicate the version on the replaying node — diverging its
		// version history and rollback-by-number from nodes that didn't crash
		// there. Mirrors saveType. The node-local build below still runs, so a
		// replay re-establishes the worker.
		if existing, gerr := getVersioned(b, []byte(t.ID)); gerr != nil || !bytes.Equal(existing, bs) {
			if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
				return fmt.Errorf("[wal.syncable] putVersioned: %w", err)
			}
		}

		_, parsed, parsedMode, err := s.parser.ParseSyncable(t.MimeType, t.Data, s)
		if err != nil {
			// Degrade rather than fail the apply (which would crash the
			// node). The config is persisted; no worker is started until
			// the build succeeds.
			s.recordConfigError("syncable", t.ID, configErrBuild, err)
			s.logger.Error("syncable config persisted but could not be built on this node (degraded); fix the environment and the config will build on next restart",
				zap.String("id", t.ID), zap.Error(err))
			return nil
		}
		s.clearConfigError("syncable", t.ID, configErrBuild)
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
		select {
		case s.sync <- &db.SyncableWithID{ID: t.ID, Syncable: syncable}:
		case <-s.closeC:
			s.logger.Debug("storage closing; dropped syncable notification (reconcile re-emits on next start)", zap.String("id", t.ID))
		}
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
// handle rather than re-parsing here — so the delete signal carries the ID and
// the entity-borne keepData intent (deterministic on every node; see
// cluster.NewDeleteSyncableEntities).
func (s *Storage) deleteSyncable(id []byte, keepData bool) error {
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := deleteVersioned(b, id); err != nil {
			return err
		}
		// Sweep the per-syncable-id state kept outside the config sub-bucket and not
		// carried as its own delete-bundle tombstone (dead-letters, stuck, skip) so a
		// same-id recreate starts clean. Same tx as the config delete → atomic.
		return sweepSyncableSiblingState(tx, id)
	})
	if err != nil {
		return err
	}
	// The config is gone; its degraded-config record must not outlive it
	// (nothing re-checks a deleted id, so the gauge would overcount forever).
	s.clearConfigError("syncable", string(id), configErrBuild)

	if s.sync != nil {
		s.logger.Debug("sending syncable delete to channel", zap.String("id", string(id)))
		select {
		case s.sync <- &db.SyncableWithID{ID: string(id), Delete: true, KeepData: keepData}:
		case <-s.closeC:
			s.logger.Debug("storage closing; dropped syncable delete notification (reconcile re-emits on next start)", zap.String("id", string(id)))
		}
	}

	return nil
}

// RequestSyncReconcile asks the db-layer listener to converge the running
// sync workers to the CURRENT syncable config set. It replaces the old
// RestoreSyncableWorkers, which listed and parsed configs on ITS CALLER'S
// goroutine and then sent the results — a stale snapshot that raced the apply
// path (resurrecting a just-deleted worker, rolling an updated one back to a
// superseded version, and corrupting the config-error gauge through
// same-strength stale records). The reconcile message instead carries a
// closure the LISTENER executes at dequeue time, serialized with the apply
// path's own channel events, so the list+parse can never observe anything
// older than every event already delivered; and the db layer cancels workers
// whose id the fresh list lacks — closing the compacted-delete hole, where a
// delete that arrived inside an InstallSnapshot has no apply event at all.
//
// Why a restart needs this at all: on a clean restart, ApplyCommitted's
// idempotency guard (entry.Index <= appliedIndex) means handleSyncable is NOT
// re-called, so the only thing that ever sends worker events on s.sync
// (saveSyncable, on the apply path) does not fire; without a reconcile a
// previously-configured syncable's worker never respawns.
//
// ORDERING CONTRACT (identical to RequestIngestReconcile): the caller MUST
// have registered the syncable sub-parsers (Parser.AddSyncableParser "sql" /
// "http") AND started the channel consumer (db.New's listenForSyncables
// drains s.sync) before calling this — the closure parses with whatever
// sub-parsers exist when the LISTENER runs it.
func (s *Storage) RequestSyncReconcile() {
	if s.sync == nil {
		return
	}
	// This runs on a detached goroutine (cmd/node startup, refreshAfterRestore)
	// that is NOT synchronized with db.Close's channel drain, so a bare send can
	// outlive the listener and strand forever. Escape on closeC — a reconcile
	// dropped at shutdown is redundant (the next start reconciles from scratch).
	select {
	case s.sync <- &db.SyncableWithID{ReconcileList: s.reconcileSyncableList}:
	case <-s.closeC:
		s.logger.Debug("storage closing; skipped sync reconcile request")
	}
}

// reconcileSyncableList is the reconcile closure body: list + parse the
// CURRENT config set (recording/clearing build-evidence config errors), and
// sweep degraded records for ids no longer in the bucket — nothing re-checks
// a deleted id, so a record surviving its config would overcount the gauge
// forever (the compacted-delete stale-record hole).
func (s *Storage) reconcileSyncableList() ([]*db.SyncableWithID, error) {
	raws, present, err := s.listRawConfigs(syncableBucket)
	if err != nil {
		return nil, err
	}
	out := make([]*db.SyncableWithID, 0, len(raws))
	for _, r := range raws {
		// A degraded config (undecodable, or unparseable on this node) is
		// returned with a nil Syncable: it is PRESENT so its worker is kept
		// (not cancelled as a phantom delete), just not reconfigured.
		if r.decodeErr != nil {
			s.recordConfigError("syncable", r.id, configErrBuild, r.decodeErr)
			s.logger.Warn("sync reconcile: undecodable config (degraded — kept)",
				zap.String("id", r.id), zap.Error(r.decodeErr))
			out = append(out, &db.SyncableWithID{ID: r.id})
			continue
		}
		_, syncable, mode, perr := s.parser.ParseSyncable(r.cfg.MimeType, r.cfg.Data, s)
		if perr != nil {
			s.recordConfigError("syncable", r.id, configErrBuild, perr)
			s.logger.Warn("sync reconcile: parse (degraded — kept)",
				zap.String("id", r.id), zap.Error(perr))
			out = append(out, &db.SyncableWithID{ID: r.id})
			continue
		}
		s.clearConfigError("syncable", r.id, configErrBuild)
		// Mirror saveSyncable: ModeAlwaysCurrent decorates the syncable
		// with the migration wrapper so the worker loop stays oblivious
		// to version-upgrade concerns.
		if mode == cluster.ModeAlwaysCurrent {
			syncable = migration.Wrap(syncable, s, s.metrics)
		}
		out = append(out, &db.SyncableWithID{ID: r.id, Syncable: syncable})
	}
	s.sweepConfigErrorsExcept("syncable", present)
	return out, nil
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
