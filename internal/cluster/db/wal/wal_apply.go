package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// ApplyCommitted applies a single committed raft entry to application
// state. It is called by the raft Ready loop on each entry from
// rd.CommittedEntries, after Save has persisted the entry and before
// node.Advance(). Apply must complete before Advance per etcd-raft contract.
//
// ApplyCommitted is idempotent on re-apply: entries with index <=
// AppliedIndex are skipped. The applied index is bumped (and persisted to
// bbolt) after each successful apply, so a restart that replays committed
// entries through the Ready loop will skip the already-applied portion.
//
// It also mirrors the raw entry into the permanent EventLog — every
// committed entry, regardless of EntryType or entity kind, so EventLog's
// sequence number stays 1:1 with raft index. Readers filter at read time.
// This is what keeps the storage invariant P_local == R_local true under
// normal operation: both advance together per entry.
//
// Errors here are treated as fatal by raft.go; see the apply error policy
// comment in raft.go's Ready loop.
func (s *Storage) ApplyCommitted(entry *pb.Entry) error {
	// Skip already-applied entries (replay-on-restart safety). A bare
	// EntryNormal with nil data (e.g. the leader's post-election no-op
	// entry from raft) still counts as applied — we bump appliedIndex so
	// the Ready loop's invariant check stays P_local == R_local.
	if entry.GetIndex() <= s.appliedIndex.Load() {
		return nil
	}

	// Mirror the raw raft entry into the permanent event log. The guard
	// against entry.GetIndex() <= eventIndex covers the crash window where
	// appendEvent succeeded but saveAppliedIndex didn't persist:
	// without it, replay would try to append the same entry again and
	// diverge seq vs. raft index on disk.
	if entry.GetIndex() > s.eventIndex.Load() {
		if err := s.appendEvent(entry); err != nil {
			return fmt.Errorf("[wal.storage] %w", err)
		}
	}

	// For EntryNormal with data, apply entities to bucket state and the
	// time-series store. Other entry types (conf changes, no-op entries)
	// still count as "applied" for invariant purposes — we just have
	// nothing to do at the entity level.
	//
	// Unmarshal failures here are returned as errors (not swallowed):
	// the raft ready loop's apply-error policy fatal-exits the node,
	// which is the correct behavior. Swallowing would let appliedIndex
	// advance past an entry that wasn't applied, diverging replicas
	// (node A applies, node B skips) and silently dropping entities
	// from any future snapshot taken at that index.
	if entry.GetType() == pb.EntryNormal && entry.Data != nil {
		p := &cluster.Proposal{}
		if err := p.Unmarshal(entry.Data, s); err != nil {
			s.logger.Error("unmarshal committed proposal",
				zap.Uint64("index", entry.GetIndex()),
				zap.Stringer("entryType", entry.GetType()),
				zap.Error(err))
			return fmt.Errorf("[wal.storage] unmarshal proposal at index %d: %w", entry.GetIndex(), err)
		}

		// Advance the data-entry head iff this is user topic data — exactly
		// the filter the per-syncable reader applies in reader.go, so
		// dataEventIndex tracks the index the reader converges to at EOF.
		// Internal entries (committed's config + coordination) are skipped
		// here just as the reader skips them, so an idle syncable's lag
		// reads 0 rather than a phantom backlog of trailing internal
		// entries. Monotonic: entries apply in index order.
		if len(p.Entities) > 0 && !cluster.IsInternal(p.Entities[0].Type.ID) {
			s.dataEventIndex.Store(entry.GetIndex())
		}

		for _, entity := range p.Entities {
			// Never log the raw entity Key: for a user topic entity (and especially
			// an RTBF delete) it is the erasure subject's PII. The typeID + raft
			// index correlate the entry without exposing the subject.
			s.logger.Debug("applying entity", zap.String("typeID", entity.Type.ID), zap.Uint64("index", entry.GetIndex()))
			if err := s.applyEntity(entity); err != nil {
				return err
			}

			// Record an event-log tombstone for a user-defined delete so a
			// future Scrub can physically remove this subject's PII from the
			// permanent event log. Built-in config/coordination deletes are not
			// PII and are never scrubbed. Deterministic: keyed on the committed
			// entity and stamped with this entry's raft index, so every replica
			// stores identical bytes.
			if isUserDefinedType(entity.Type.ID) && entity.IsDelete() {
				if err := s.recordEventTombstone(entity.Type.ID, entity.Key, entry.GetIndex()); err != nil {
					return fmt.Errorf("[wal.storage] recordEventTombstone: %w", err)
				}
			}

			// Count an applied EntityKindSnapshot entity as metadata-GC backlog so
			// the automatic scrubber fires on a Snapshot-heavy cluster even with no
			// RTBF deletes — covering both internal bookkeeping and user Snapshot
			// streams. (Revision configs and Standalone dead-letters are retained,
			// not counted.) This is a leader-local trigger heuristic, so the live
			// resolved kind is fine here; it over-counts distinct-key writes
			// slightly, which is harmless (see metadataBacklog).
			if entity.Type.EntityKind == cluster.EntityKindSnapshot {
				s.metadataBacklog.Add(1)
			}
		}

		// Advance the per-ingestable source-seq highwater for ingest
		// proposals. Deterministic across replicas (derived only from the
		// committed proposal) and done before saveAppliedIndex so a
		// replayed entry re-applies the idempotent max. Non-ingest
		// proposals carry "" / 0 and no-op here.
		if err := s.advanceIngestSourceSeq(p.IngestableID, p.SourceSeq); err != nil {
			return fmt.Errorf("[wal.storage] advanceIngestSourceSeq: %w", err)
		}
	}

	s.appliedIndex.Store(entry.GetIndex())
	return s.saveAppliedIndex(entry.GetIndex())
}

func (s *Storage) applyEntity(entity *cluster.Entity) error {
	for _, ie := range internalEntities {
		if ie.is(entity.ID) {
			if err := ie.handler(s, entity); err != nil {
				return fmt.Errorf("[wal.storage] %s: %w", ie.name, err)
			}
			return nil
		}
	}
	// User-defined (topic) entities have no apply-time side effect: their
	// durable record is the permanent event log, written in ApplyCommitted
	// before this entity-level dispatch runs.
	return nil
}

// AppliedIndex returns the highest raft entry index that ApplyCommitted has
// fully applied. Loaded from bbolt on Open and bumped after each successful
// apply. This is "R_local" in the storage invariant P_local == R_local.
func (s *Storage) AppliedIndex() uint64 {
	return s.appliedIndex.Load()
}

// saveAppliedIndex persists the applied index to bbolt. Called from
// ApplyCommitted after a per-entry apply succeeds. Each apply runs in its
// own short bbolt transaction; this is acceptable because the per-entity
// handlers are not atomic with each other today either.
func (s *Storage) saveAppliedIndex(idx uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		return b.Put(appliedIndexKey, buf[:])
	})
}

// SetAppliedIndexForTest rewinds the applied index (in memory and in bbolt) to
// simulate a crash in the apply window: the event log has fsync'd entry N but
// saveAppliedIndex persisted only N-1. Reopening the storage then presents the
// eventIndex-ahead-of-appliedIndex gap that restart replay must heal. Test-only;
// never called in production.
func (s *Storage) SetAppliedIndexForTest(idx uint64) error {
	s.appliedIndex.Store(idx)
	return s.saveAppliedIndex(idx)
}

// loadAppliedIndex reads the persisted applied index from bbolt, or returns
// 0 if no apply has happened yet (fresh storage).
func (s *Storage) loadAppliedIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get(appliedIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("appliedIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	})
	return idx, err
}
