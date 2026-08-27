package wal

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/interpretation"
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
// and then queues the node-local BUILD for the listener. The apply path
// itself never parses or builds: ParseSyncable runs Init against the
// destination (DDL, prepares — destination I/O that can hang on a table
// lock), and the apply-liveness invariant is absolute — appliedIndex never
// waits on destination-DB state, and no destination I/O ever runs under
// the bbolt writer lock. The pushed Build closure executes on the
// db-layer listener goroutine at dequeue time, exactly like the reconcile
// closures, FIFO with every other config event.
//
// The push happens AFTER the bbolt Update returns successfully — a failed
// commit must not have queued a build for state that doesn't exist on
// disk.
func (s *Storage) saveSyncable(t *cluster.Configuration, raftIndex uint64) error {
	var replayed bool
	var clearedWorkerState bool
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return ErrBucketMissing
		}

		// Replay guard (config-version-replay): ApplyCommittedBatch can replay a
		// whole Ready on a crash-window restart. A versioned apply whose entry
		// index already produced a version is a replay — skip it (no version
		// append, no build queued), or the last+1 allocator appends a phantom
		// version, diverging history across replicas. The set below rides this
		// same atomic tx, so a failure rolls both back.
		if versionedLastIndex(b, []byte(t.ID)) >= raftIndex {
			replayed = true
			return nil
		}
		if err := setVersionedLastIndex(b, []byte(t.ID), raftIndex); err != nil {
			return err
		}

		// A new config version is the operator's fix (the only path that bumps a
		// syncable's version). Clear any worker-state record — a stale transient
		// stuck OR a terminal park — ATOMICALLY with the version write, so the fixed
		// config can't sit beside a stale/parked record. A bare respawn replays the
		// same version (deduped by the guard above) and never reaches here, so a park
		// correctly survives leadership change / restart. See handleSyncableStuck.
		if cleared, derr := deleteKeyedTx(tx, syncableStuckBucket, []byte(t.ID)); derr != nil {
			return derr
		} else if cleared {
			clearedWorkerState = true
		}

		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.syncable] marshal: %w", err)
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
				return fmt.Errorf("[wal.syncable] putVersioned: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Gauge update rides outside the tx (metrics are not transactional). Every node
	// applies this config change and clears its own record, so both gauges converge
	// to 0 cluster-wide by construction.
	if clearedWorkerState && s.metrics != nil {
		s.metrics.SetSyncStuck(t.ID, false)
		s.metrics.SetWorkerParked("sync", t.ID, false)
	}

	// A replay-guard hit queues nothing: that entry's version already produced
	// its build message (or its worker survives untouched); a duplicate would
	// pointlessly restart the worker on a crash-window replay.
	if !replayed && s.syncPump != nil {
		// Push, never send: the apply path must not block on the listener
		// (see notifyPump — the field wedge where a locked sink table froze
		// appliedIndex cluster-wide through this exact send).
		s.logger.Debug("queueing syncable build", zap.String("id", t.ID))
		s.syncPump.push(&db.SyncableWithID{ID: t.ID, Build: func() cluster.Syncable {
			return s.buildSyncable(t)
		}})
	}

	return nil
}

// buildSyncable is the node-local build body, executed by the db-layer
// LISTENER at dequeue time — never on the apply path (the apply-liveness
// invariant: appliedIndex never waits on destination-DB state). It parses
// and Inits against the destination (bounded inside Init by the sql
// layer's InitTimeout), records/clears the degraded-config evidence, and
// decorates with the migration wrapper. Returns nil for a config that
// failed to build on this node: the raw bytes are already persisted (the
// deterministic state-machine write), no worker is (re)started, and the
// degraded record is the loud, queryable evidence.
func (s *Storage) buildSyncable(t *cluster.Configuration) cluster.Syncable {
	// Deterministic derivation backstop: the leader's admission check can be
	// raced (two proposes admitted against the same applied state, both
	// committed). Replaying the stored configs' derivation edges in log-index
	// order decides — identically on every node and every restart — which
	// config a cycle or fan-in refuses: the one that landed later. A config's
	// verdict depends only on earlier-indexed configs, so listener timing
	// (later configs already persisted when this build dequeues) cannot
	// change the answer. Refused = persisted but degraded (no worker), so a
	// derivation cycle can never actually run.
	if derr := s.derivationRefusals()[db.EdgeRef{Kind: "syncable", ID: t.ID}]; derr != nil {
		s.recordConfigError("syncable", t.ID, configErrBuild, derr)
		s.logger.Error("syncable config persisted but refused by the derivation guard (degraded); fix the graph and re-POST the config",
			zap.String("id", t.ID), zap.Error(derr))
		return nil
	}

	// Epoch-continuity backstop for configs that raced past (or were
	// admitted on a stale applied view by) the admission check — the same
	// predicate admission runs (db.DerivedTopicEpochRegression), so the two
	// cannot skew. Deliberately NOT part of the deterministic replay above:
	// the replay must stay a pure function of the stored config set, and
	// this predicate reads point-in-time replicated state
	// (TopicRefreshEpoch). Build is the right home — it is already
	// node-local and environment-dependent (Init reaches destination
	// databases), degrades loudly, and retries every minute, so a
	// lagging-source handover wedges visibly instead of silently stranding
	// stale rows, and self-heals the moment the source's epoch space climbs
	// past the target's old highwater (the handover has become
	// reconcilable). A healthy loopback can never trip it: the target's
	// epochs are bumped only by this loopback's own forwards of the
	// source's, so target ≤ source holds in steady state.
	if targets, terr := s.parser.SyncableDerivedTopics(t.MimeType, t.Data); terr == nil && len(targets) > 0 {
		sources, _ := s.parser.SyncableTopics(t.MimeType, t.Data)
		if derr := db.DerivedTopicEpochRegression(sources, targets, s.TopicRefreshEpoch); derr != nil {
			s.recordConfigError("syncable", t.ID, configErrBuild, derr)
			s.logger.Error("syncable config persisted but refused by the epoch-continuity guard (degraded); the build retries every minute and admits once the source's epoch space catches up — or derive into a fresh topic",
				zap.String("id", t.ID), zap.Error(derr))
			return nil
		}
	}

	_, parsed, parsedMode, err := s.parser.ParseSyncable(t.MimeType, t.Data, s)
	if err != nil {
		s.recordConfigError("syncable", t.ID, configErrBuild, err)
		if cluster.IsNotAdmissible(err) {
			s.logger.Error("persisted syncable config is not admissible under this binary (admission rules have tightened since it was stored); automatic retries cannot help — fix and re-POST the config, or delete it",
				zap.String("id", t.ID), zap.Error(err))
		} else {
			s.logger.Error("syncable config persisted but could not be built on this node (degraded); fix the environment — the node retries the build every minute (and on restart)",
				zap.String("id", t.ID), zap.Error(err))
		}
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
	// EVERY syncable reads the authoritative interpretation: the outer
	// wrapper rebinds each entity to stamp ⊕ errata fold before the
	// migration chain (inside) or the consumer (either mode) sees it.
	return interpretation.Wrap(parsed, s.InterpretationRegistry, s)
}

// SyncableExists reports whether a syncable config id currently exists (a
// live current version — deleted ids read false). The existence oracle for
// the HTTP status/errors 404 gates; see existsVersioned.
func (s *Storage) SyncableExists(id string) (bool, error) {
	var exists bool
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableBucket)
		if b == nil {
			return nil
		}
		exists = existsVersioned(b, []byte(id))
		return nil
	})
	return exists, err
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

	if s.syncPump != nil {
		s.logger.Debug("queueing syncable delete notification", zap.String("id", string(id)))
		s.syncPump.push(&db.SyncableWithID{ID: string(id), Delete: true, KeepData: keepData})
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
	if s.syncPump == nil {
		return
	}
	// Through the pump like every other producer — not for liveness (this
	// runs on a detached goroutine where blocking was harmless) but for
	// ORDERING: a second direct writer to the channel would race the pump and
	// break the FIFO the notifications rely on. The pump also owns the
	// shutdown drop (a reconcile dropped at close is redundant — the next
	// start reconciles from scratch).
	s.syncPump.push(&db.SyncableWithID{ReconcileList: s.reconcileSyncableList})
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
		// Same build body as the apply-queued path (evidence recording, the
		// derivation guard, and the wrappers live in one place). A nil result
		// is a degraded build: the entry stays PRESENT so its worker is kept
		// (not cancelled as a phantom delete), just not reconfigured.
		out = append(out, &db.SyncableWithID{ID: r.id, Syncable: s.buildSyncable(r.cfg)})
	}
	s.sweepConfigErrorsExcept("syncable", present)
	s.reportNotAdmissible("syncable")
	return out, nil
}

// producerEdgesTx enumerates the stored producer graph — deriving syncables
// and topic-producing ingestables, each with the raft index its current
// version applied at. It is the SINGLE edge source: admission
// (db.ReplayWithCandidate via Storage.ProducerEdges) and the build-time
// replay below both consume it, so the two predicates cannot skew.
func (s *Storage) producerEdgesTx(tx *bolt.Tx) []db.DerivationEdge {
	var edges []db.DerivationEdge
	if b := tx.Bucket(syncableBucket); b != nil {
		_ = forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				return nil // undecodable — degraded elsewhere, no edges
			}
			targets, err := s.parser.SyncableDerivedTopics(cfg.MimeType, cfg.Data)
			if err != nil || len(targets) == 0 {
				return nil // non-deriving kind (or unparseable — degraded elsewhere)
			}
			sources, _ := s.parser.SyncableTopics(cfg.MimeType, cfg.Data)
			edges = append(edges, db.DerivationEdge{
				Kind:    "syncable",
				ID:      string(id),
				Index:   versionedLastIndex(b, id),
				Sources: sources,
				Targets: targets,
			})
			return nil
		})
	}
	if b := tx.Bucket(ingestableBucket); b != nil {
		_ = forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				return nil
			}
			topics, err := s.parser.IngestableTopics(cfg.MimeType, cfg.Data)
			if err != nil || len(topics) == 0 {
				return nil // kind reports no topics (or unparseable — degraded elsewhere)
			}
			edges = append(edges, db.DerivationEdge{
				Kind:    "ingestable",
				ID:      string(id),
				Index:   versionedLastIndex(b, id),
				Targets: topics,
			})
			return nil
		})
	}
	return edges
}

// ProducerEdges is the view-transaction wrapper for admission (db.Storage).
func (s *Storage) ProducerEdges() ([]db.DerivationEdge, error) {
	var edges []db.DerivationEdge
	err := s.view(func(tx *bolt.Tx) error {
		edges = s.producerEdgesTx(tx)
		return nil
	})
	return edges, err
}

// derivationRefusals computes which stored configs the producer guard
// refuses (db.ReplayDerivation over producerEdgesTx: cycles and duplicate
// epoch-stamping producers, replayed jointly in log-index order — an
// ingestable×loopback collision resolves identically to any other). Pure
// function of the stored set — every node computes the same map.
// buildSyncable and buildIngestable consult it at build time (off the apply
// path — pure bbolt reads + config parsing, no destination or source I/O).
func (s *Storage) derivationRefusals() map[db.EdgeRef]error {
	var refusals map[db.EdgeRef]error
	_ = s.view(func(tx *bolt.Tx) error {
		refusals = db.ReplayDerivation(s.producerEdgesTx(tx))
		return nil
	})
	return refusals
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
