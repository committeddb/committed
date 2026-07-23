package wal

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

func (s *Storage) handleIngestable(e *cluster.Entity, raftIndex uint64) error {
	if e.IsDelete() {
		return s.deleteIngestable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveIngestable(t, raftIndex)
	}
}

// saveIngestable persists an ingestable Configuration as a new version in
// bbolt and then notifies the consumer channel. See saveSyncable for the
// rationale on why the channel send happens outside the bbolt Update closure.
func (s *Storage) saveIngestable(t *cluster.Configuration, raftIndex uint64) error {
	var ingestable cluster.Ingestable
	var built bool
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
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
			return fmt.Errorf("[wal.ingestable] marshal: %w", err)
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
				return fmt.Errorf("[wal.ingestable] putVersioned: %w", err)
			}
		}

		_, parsed, err := s.parser.ParseIngestable(t.MimeType, t.Data)
		if err != nil {
			// Degrade rather than fail the apply (which would crash the
			// node). The config is persisted; no worker is started until
			// the build succeeds.
			s.recordConfigError("ingestable", t.ID, configErrBuild, err)
			s.logger.Error("ingestable config persisted but could not be built on this node (degraded); fix the environment and the config will build on next restart",
				zap.String("id", t.ID), zap.Error(err))
			return nil
		}
		s.clearConfigError("ingestable", t.ID, configErrBuild)
		ingestable = parsed
		built = true

		return nil
	})
	if err != nil {
		return err
	}

	if built && s.ingest != nil {
		select {
		case s.ingest <- &db.IngestableWithID{ID: t.ID, Ingestable: ingestable}:
		case <-s.closeC:
			s.logger.Debug("storage closing; dropped ingestable notification (reconcile re-emits on next start)", zap.String("id", t.ID))
		}
	}

	return nil
}

func (s *Storage) deleteIngestable(id []byte) error {
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		// NB: unlike deleteDatabase there is no in-memory map to clear —
		// ingestables aren't cached on the Storage (they're handed to the
		// supervisor via the ingest channel). The previous
		// `s.databases[id] = nil` here was a copy-paste leftover from
		// deleteDatabase that could nil out a live database connection if
		// an ingestable and database happened to share an id.
		if err := deleteVersioned(b, id); err != nil {
			return err
		}
		// Sweep the per-ingestable-id source-seq highwater (kept outside the config
		// sub-bucket and not a delete-bundle tombstone) so a same-id recreate's
		// re-emitted CDC proposals aren't dropped pre-raft. Same tx → atomic. The
		// topic refresh-epoch is deliberately NOT swept (topic-keyed, must survive).
		return sweepIngestableSiblingState(tx, id)
	})
	if err != nil {
		return err
	}
	// The config is gone; its degraded-config record must not outlive it
	// (nothing re-checks a deleted id, so the gauge would overcount forever).
	s.clearConfigError("ingestable", string(id), configErrBuild)

	// Signal the DB layer to cancel the worker and, on the owner, tear down the
	// source-side replication resources (drop the Postgres slot + publication) so
	// an orphaned slot can't pin the source's WAL. Mirrors deleteSyncable; the DB
	// layer reuses the worker's already-built ingestable handle for the teardown,
	// so the signal carries only the ID.
	if s.ingest != nil {
		s.logger.Debug("sending ingestable delete to channel", zap.String("id", string(id)))
		select {
		case s.ingest <- &db.IngestableWithID{ID: string(id), Delete: true}:
		case <-s.closeC:
			s.logger.Debug("storage closing; dropped ingestable delete notification (reconcile re-emits on next start)", zap.String("id", string(id)))
		}
	}

	return nil
}

// RequestIngestReconcile is the ingest twin of RequestSyncReconcile: the
// listener converges running ingest workers to the CURRENT config set via the
// closure below, executed at dequeue time. See RequestSyncReconcile for the
// staleness rationale and the ORDERING CONTRACT (sub-parsers registered and
// db.New's listener draining s.ingest before this is called).
func (s *Storage) RequestIngestReconcile() {
	if s.ingest == nil {
		return
	}
	// Detached-goroutine sender; escape on closeC so it can't strand past
	// shutdown. See RequestSyncReconcile.
	select {
	case s.ingest <- &db.IngestableWithID{ReconcileList: s.reconcileIngestableList}:
	case <-s.closeC:
		s.logger.Debug("storage closing; skipped ingest reconcile request")
	}
}

// reconcileIngestableList: see reconcileSyncableList.
func (s *Storage) reconcileIngestableList() ([]*db.IngestableWithID, error) {
	raws, present, err := s.listRawConfigs(ingestableBucket)
	if err != nil {
		return nil, err
	}
	out := make([]*db.IngestableWithID, 0, len(raws))
	for _, r := range raws {
		// Degraded → nil Ingestable → present-but-kept (see reconcileSyncableList).
		if r.decodeErr != nil {
			s.recordConfigError("ingestable", r.id, configErrBuild, r.decodeErr)
			s.logger.Warn("ingest reconcile: undecodable config (degraded — kept)",
				zap.String("id", r.id), zap.Error(r.decodeErr))
			out = append(out, &db.IngestableWithID{ID: r.id})
			continue
		}
		_, ingestable, perr := s.parser.ParseIngestable(r.cfg.MimeType, r.cfg.Data)
		if perr != nil {
			s.recordConfigError("ingestable", r.id, configErrBuild, perr)
			s.logger.Warn("ingest reconcile: parse (degraded — kept)",
				zap.String("id", r.id), zap.Error(perr))
			out = append(out, &db.IngestableWithID{ID: r.id})
			continue
		}
		s.clearConfigError("ingestable", r.id, configErrBuild)
		out = append(out, &db.IngestableWithID{ID: r.id, Ingestable: ingestable})
	}
	s.sweepConfigErrorsExcept("ingestable", present)
	return out, nil
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
