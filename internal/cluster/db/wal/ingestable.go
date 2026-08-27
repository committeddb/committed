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
// bbolt and then queues the node-local BUILD for the listener — see
// saveSyncable: ParseIngestable reaches the SOURCE database (Preflight
// connects), and the apply-liveness invariant is the same — appliedIndex
// never waits on external-DB state, and no external I/O runs under the
// bbolt writer lock.
func (s *Storage) saveIngestable(t *cluster.Configuration, raftIndex uint64) error {
	var replayed bool
	var clearedWorkerState bool
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
			replayed = true
			return nil
		}
		if err := setVersionedLastIndex(b, []byte(t.ID), raftIndex); err != nil {
			return err
		}

		// A new config version is the operator's fix — clear any parked record for
		// this ingestable ATOMICALLY with the version write (mirrors saveSyncable).
		// A bare respawn replays the same version (deduped above) and never reaches
		// here, so a park survives leadership change / restart.
		if cleared, derr := deleteKeyedTx(tx, ingestableStuckBucket, []byte(t.ID)); derr != nil {
			return derr
		} else if cleared {
			clearedWorkerState = true
		}

		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.ingestable] marshal: %w", err)
		}

		// Deterministic state-machine write FIRST: persist the raw config
		// bytes so every replica converges. The node-local build (which
		// can fail on a missing ${VAR} secret) happens later, on the
		// listener, off this path.
		//
		// Skip the version APPEND on a byte-identical replay: a crash-apply-window
		// entry is re-delivered (entity fsynced, applied-index not), and appending
		// again would duplicate the version on the replaying node — diverging its
		// version history and rollback-by-number from nodes that didn't crash
		// there. Mirrors saveType. The build below is still queued, so a
		// replay re-establishes the worker.
		if existing, gerr := getVersioned(b, []byte(t.ID)); gerr != nil || !bytes.Equal(existing, bs) {
			if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
				return fmt.Errorf("[wal.ingestable] putVersioned: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if clearedWorkerState && s.metrics != nil {
		s.metrics.SetWorkerParked("ingest", t.ID, false)
	}

	// A replay-guard hit queues nothing — mirrors saveSyncable.
	if !replayed && s.ingestPump != nil {
		// Push, never send — the apply path must not block on the listener
		// (see notifyPump).
		s.logger.Debug("queueing ingestable build", zap.String("id", t.ID))
		s.ingestPump.push(&db.IngestableWithID{ID: t.ID, Build: func() cluster.Ingestable {
			return s.buildIngestable(t)
		}})
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
	if s.ingestPump != nil {
		s.logger.Debug("queueing ingestable delete notification", zap.String("id", string(id)))
		s.ingestPump.push(&db.IngestableWithID{ID: string(id), Delete: true})
	}

	return nil
}

// IngestableExists is SyncableExists's ingest twin — the existence oracle
// for the ingestable status endpoint's 404 gate.
func (s *Storage) IngestableExists(id string) (bool, error) {
	var exists bool
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return nil
		}
		exists = existsVersioned(b, []byte(id))
		return nil
	})
	return exists, err
}

// RequestIngestReconcile is the ingest twin of RequestSyncReconcile: the
// listener converges running ingest workers to the CURRENT config set via the
// closure below, executed at dequeue time. See RequestSyncReconcile for the
// staleness rationale and the ORDERING CONTRACT (sub-parsers registered and
// db.New's listener draining s.ingest before this is called).
func (s *Storage) RequestIngestReconcile() {
	if s.ingestPump == nil {
		return
	}
	// Through the pump for FIFO — see RequestSyncReconcile.
	s.ingestPump.push(&db.IngestableWithID{ReconcileList: s.reconcileIngestableList})
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
		// Same build body as the apply-queued path — see reconcileSyncableList.
		out = append(out, &db.IngestableWithID{ID: r.id, Ingestable: s.buildIngestable(r.cfg)})
	}
	s.sweepConfigErrorsExcept("ingestable", present)
	s.reportNotAdmissible("ingestable")
	return out, nil
}

// buildIngestable is the node-local build body, executed by the db-layer
// LISTENER at dequeue time — never on the apply path (mirrors
// buildSyncable; ParseIngestable reaches the source database via
// Preflight). Returns nil for a config that failed to build on this node,
// recorded as the loud degraded evidence.
func (s *Storage) buildIngestable(t *cluster.Configuration) cluster.Ingestable {
	// Deterministic producer backstop, the twin of buildSyncable's: the
	// leader's admission check can be raced (two proposes admitted against
	// the same applied state, both committed). Replaying the stored producer
	// edges — both kinds jointly — in log-index order decides, identically
	// on every node and every restart, which config a topic collision
	// refuses: the one that landed later. Refused = persisted but degraded
	// (no worker), so two epoch-stamping producers can never actually run on
	// one topic. Deleting the winner un-refuses the loser at its next build
	// (the topic's refresh epoch is topic-keyed and survives, so a promoted
	// producer continues the same epoch space).
	if derr := s.derivationRefusals()[db.EdgeRef{Kind: "ingestable", ID: t.ID}]; derr != nil {
		s.recordConfigError("ingestable", t.ID, configErrBuild, derr)
		s.logger.Error("ingestable config persisted but refused by the producer guard (degraded); use a separate topic or delete the colliding config, then re-POST",
			zap.String("id", t.ID), zap.Error(derr))
		return nil
	}

	_, parsed, err := s.parser.ParseIngestable(t.MimeType, t.Data)
	if err != nil {
		s.recordConfigError("ingestable", t.ID, configErrBuild, err)
		if cluster.IsNotAdmissible(err) {
			s.logger.Error("persisted ingestable config is not admissible under this binary (admission rules have tightened since it was stored); automatic retries cannot help — fix and re-POST the config, or delete it",
				zap.String("id", t.ID), zap.Error(err))
		} else {
			s.logger.Error("ingestable config persisted but could not be built on this node (degraded); fix the environment — the node retries the build every minute (and on restart)",
				zap.String("id", t.ID), zap.Error(err))
		}
		return nil
	}
	s.clearConfigError("ingestable", t.ID, configErrBuild)
	return parsed
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
