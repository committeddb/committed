package wal

import (
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// handleSyncableStuck applies a committed SyncableStuck entity: a delete clears
// the syncable's worker-state record, an upsert replaces it. One entry per
// syncable id (last-writer-wins). The record is either a transient stuck
// (Parked=false — the worker publishes "blocked on index N" and deletes it on
// progress) or a terminal park (Parked=true — published once at a circuit-breaker
// trip and cleared only by an operator fix or a delete). Both worker-state gauges
// are derived from it below.
func (s *Storage) handleSyncableStuck(e *cluster.Entity, _ uint64) error {
	id := string(e.Key)
	if e.IsDelete() {
		if err := s.deleteKeyed(syncableStuckBucket, e.Key); err != nil {
			return err
		}
	} else {
		st := &cluster.SyncableStuck{}
		if err := st.Unmarshal(e.Data); err != nil {
			return err
		}
		id = st.ID
		// Guard: a stuck record persists only while the syncable config exists, so a
		// worker bump that commits after DeleteSyncable (or replays before a same-id
		// recreate) can't leave an orphan a recreate would misreport as "blocked on
		// index N". Reaps any lingering value when the config is gone.
		if err := s.putConfigGuardedKeyed(syncableBucket, syncableStuckBucket, []byte(st.ID), e.Data); err != nil {
			return err
		}
	}
	// Derive BOTH worker-state gauges from the APPLIED record on THIS node — a
	// present record is either transient-stuck (Parked=false ⇒ sync.stuck) or a
	// terminal park (Parked=true ⇒ worker.parked); absent (deleted, or reaped by
	// the config guard) ⇒ neither. Every node runs the apply path, so both gauges
	// converge cluster-wide by construction (companion determinism), truthful on
	// followers; the worker no longer toggles them imperatively, which latched them
	// at 1 on followers that never ran the owner's clear.
	if s.metrics != nil {
		rec, present, _ := s.SyncableStuck(id)
		s.metrics.SetSyncStuck(id, present && !rec.Parked)
		s.metrics.SetWorkerParked("sync", id, present && rec.Parked)
	}
	return nil
}

// refreshWorkerStateGauges re-derives the worker-state gauges (committed.sync.stuck
// and committed.worker.parked, for both sync and ingest) for every syncable/
// ingestable that has an applied record. The per-kind handlers derive them on the
// APPLY path — but apply does not run for entries already applied before a restart
// (replay skips <= appliedIndex), nor for a snapshot install (RestoreSnapshot
// bulk-swaps bbolt without running any entity handler). Without this, a node that
// restarts or receives a snapshot while a worker is stuck/parked reads the gauge as
// 0/unset even though the durable record says otherwise, so a sustained-1 alert
// silently never fires. Called at Open and after a snapshot restore. Ids with no
// record need no action — the gauge defaults to unset and the apply path sets it to
// 0 on the clearing delete.
func (s *Storage) refreshWorkerStateGauges() {
	if s.metrics == nil {
		return
	}
	err := s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(syncableStuckBucket); b != nil {
			if err := b.ForEach(func(k, v []byte) error {
				var rec cluster.SyncableStuck
				if err := rec.Unmarshal(v); err != nil {
					return fmt.Errorf("[wal.stuck] refresh unmarshal syncable id=%s: %w", string(k), err)
				}
				s.metrics.SetSyncStuck(string(k), !rec.Parked)
				s.metrics.SetWorkerParked("sync", string(k), rec.Parked)
				return nil
			}); err != nil {
				return err
			}
		}
		// Ingest: a present record is a terminal park (no transient variant yet).
		if b := tx.Bucket(ingestableStuckBucket); b != nil {
			if err := b.ForEach(func(k, _ []byte) error {
				s.metrics.SetWorkerParked("ingest", string(k), true)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.logger.Warn("refresh worker-state gauges", zap.Error(err))
	}
}

// handleIngestableStuck applies a committed IngestableStuck entity: a delete clears
// the ingestable's parked record, an upsert records it. One entry per ingestable id.
// Mirrors handleSyncableStuck for the ingest side — a parked record is published at
// the freeze/restart supervisor's give-up and clears only on an operator fix
// (saveIngestable, a new config version) or a delete (config-guard reaped). Derives
// the worker.parked{kind:ingest} gauge from the applied record on every node, so it
// is truthful on followers.
func (s *Storage) handleIngestableStuck(e *cluster.Entity, _ uint64) error {
	id := string(e.Key)
	if e.IsDelete() {
		if err := s.deleteKeyed(ingestableStuckBucket, e.Key); err != nil {
			return err
		}
	} else {
		st := &cluster.IngestableStuck{}
		if err := st.Unmarshal(e.Data); err != nil {
			return err
		}
		id = st.ID
		if err := s.putConfigGuardedKeyed(ingestableBucket, ingestableStuckBucket, []byte(st.ID), e.Data); err != nil {
			return err
		}
	}
	if s.metrics != nil {
		_, present, _ := s.IngestableStuck(id)
		s.metrics.SetWorkerParked("ingest", id, present)
	}
	return nil
}

// IngestableStuck returns the ingestable's parked record, or ok=false if its worker
// is not parked. Replicated, so any node answers identically — powers the
// workerState field of GET /ingestable/{id}/status.
func (s *Storage) IngestableStuck(id string) (cluster.IngestableStuck, bool, error) {
	var (
		out cluster.IngestableStuck
		ok  bool
	)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableStuckBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if err := out.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.stuck] unmarshal ingestable id=%s: %w", id, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return cluster.IngestableStuck{}, false, err
	}
	return out, ok, nil
}

// deleteKeyedTx removes key from the named flat bucket within an existing write
// tx, reporting whether it was present. Used by saveSyncable to clear a worker-
// state record atomically with the config-version write it makes.
func deleteKeyedTx(tx *bolt.Tx, bucket, key []byte) (bool, error) {
	b := tx.Bucket(bucket)
	if b == nil {
		return false, nil
	}
	if b.Get(key) == nil {
		return false, nil
	}
	if err := b.Delete(key); err != nil {
		return false, err
	}
	return true, nil
}

// SyncableStuck returns the syncable's current stuck record, or ok=false if
// it isn't blocked. Replicated, so any node answers identically.
func (s *Storage) SyncableStuck(id string) (cluster.SyncableStuck, bool, error) {
	var (
		out cluster.SyncableStuck
		ok  bool
	)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableStuckBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if err := out.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.stuck] unmarshal id=%s: %w", id, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return cluster.SyncableStuck{}, false, err
	}
	return out, ok, nil
}

// handleSyncableSkipRequest applies a committed SyncableSkipRequest entity:
// a delete clears the request, an upsert records it. One entry per syncable
// id — the endpoint proposes it, the worker deletes it once honored or stale.
func (s *Storage) handleSyncableSkipRequest(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		return s.deleteKeyed(syncableSkipRequestBucket, e.Key)
	}
	r := &cluster.SyncableSkipRequest{}
	if err := r.Unmarshal(e.Data); err != nil {
		return err
	}
	// Guard: a skip request persists only while the syncable config exists, so a
	// stale request can't be honored against a matching index on a same-id recreate
	// (a silent skip of a live proposal). Reaps any lingering value when gone.
	return s.putConfigGuardedKeyed(syncableBucket, syncableSkipRequestBucket, []byte(r.ID), e.Data)
}

// SyncableSkipRequest returns the pending operator skip request for the
// syncable, or ok=false if none. The worker reads it while blocked and
// honors it only when its Index matches the proposal currently wedged.
func (s *Storage) SyncableSkipRequest(id string) (cluster.SyncableSkipRequest, bool, error) {
	var (
		out cluster.SyncableSkipRequest
		ok  bool
	)
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableSkipRequestBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if err := out.Unmarshal(v); err != nil {
			return fmt.Errorf("[wal.stuck] unmarshal skip-request id=%s: %w", id, err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return cluster.SyncableSkipRequest{}, false, err
	}
	return out, ok, nil
}

// ParkedWorkers lists every worker with a terminal parked record — syncables whose
// SyncableStuck record has Parked=true, and every ingestable with an IngestableStuck
// record. Replicated state, so any node answers identically; powers the
// /node/status worker summary. Sorted (kind, id) for a stable response.
func (s *Storage) ParkedWorkers() ([]cluster.ParkedWorker, error) {
	var out []cluster.ParkedWorker
	err := s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(syncableStuckBucket); b != nil {
			if err := b.ForEach(func(k, v []byte) error {
				var rec cluster.SyncableStuck
				if err := rec.Unmarshal(v); err != nil {
					return fmt.Errorf("[wal.stuck] parked-workers unmarshal syncable id=%s: %w", string(k), err)
				}
				if rec.Parked {
					out = append(out, cluster.ParkedWorker{Kind: "sync", ID: string(k)})
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if b := tx.Bucket(ingestableStuckBucket); b != nil {
			if err := b.ForEach(func(k, _ []byte) error {
				out = append(out, cluster.ParkedWorker{Kind: "ingest", ID: string(k)})
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// deleteKeyed removes key from the named flat bucket (no-op if absent).
func (s *Storage) deleteKeyed(bucket, key []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.Delete(key)
	})
}
