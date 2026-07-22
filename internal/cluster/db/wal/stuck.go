package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// handleSyncableStuck applies a committed SyncableStuck entity: a delete
// clears the syncable's stuck record, an upsert replaces it. One entry per
// syncable id (last-writer-wins) — the worker publishes "blocked on index N"
// and deletes it on progress.
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
	// Derive the sync-stuck gauge from the APPLIED record on THIS node — present
	// ⇒ stuck, absent (deleted, or reaped by the config guard) ⇒ not. Every node
	// runs the apply path, so the gauge converges cluster-wide by construction
	// (companion determinism); the worker no longer toggles it imperatively,
	// which latched it at 1 on followers that never ran the owner's clear.
	if s.metrics != nil {
		_, present, _ := s.SyncableStuck(id)
		s.metrics.SetSyncStuck(id, present)
	}
	return nil
}

// refreshSyncStuckGauges re-derives the committed.sync.stuck gauge for every
// syncable that has an applied stuck record. handleSyncableStuck derives the
// gauge on the APPLY path — but apply does not run for entries already applied
// before a restart (replay skips <= appliedIndex), nor for a snapshot install
// (RestoreSnapshot bulk-swaps bbolt without running any entity handler). Without
// this, a node that restarts or receives a snapshot while a syncable is stuck
// reads the gauge as 0/unset even though the durable record says stuck, so a
// sustained-1 alert silently never fires. Called at Open and after a snapshot
// restore. Ids with no record need no action — the gauge defaults to unset and
// the apply path sets it to 0 on the clearing delete.
func (s *Storage) refreshSyncStuckGauges() {
	if s.metrics == nil {
		return
	}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(syncableStuckBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, _ []byte) error {
			s.metrics.SetSyncStuck(string(k), true)
			return nil
		})
	})
	if err != nil {
		s.logger.Warn("refresh sync-stuck gauges", zap.Error(err))
	}
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
