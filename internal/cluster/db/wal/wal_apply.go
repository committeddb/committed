package wal

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

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
// AppliedIndex are skipped. The applied index is bumped after each
// successful apply and persisted to bbolt by the caller-facing entry points
// (per entry via ApplyCommitted, once per Ready via ApplyCommittedBatch), so
// a restart that replays committed entries through the Ready loop will skip
// the already-applied portion.
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
	rejected, err := s.orderVerdicts([]*pb.Entry{entry})
	if err != nil {
		return fmt.Errorf("[wal.storage] %w", err)
	}
	if entry.GetIndex() > s.appliedIndex.Load() && entry.GetIndex() > s.eventIndex.Load() {
		stored := entry
		if rejected[entry.GetIndex()] {
			stored = withoutData(entry)
		}
		if err := s.appendEvent(stored); err != nil {
			return fmt.Errorf("[wal.storage] %w", err)
		}
	}
	before := s.appliedIndex.Load()
	if err := s.applyEntries([]*pb.Entry{entry}, rejected); err != nil || s.appliedIndex.Load() == before {
		return err
	}
	return s.saveAppliedIndex(entry.GetIndex())
}

// ApplyCommittedBatch applies one Ready's worth of committed entries with the
// per-entry fsyncs hoisted to per-batch: every fresh entry's event-log record
// is appended in ONE batched write (one sync), and appliedIndex is persisted
// ONCE after the last apply — collapsing the 2-fsyncs-per-entry apply ceiling
// to 2 per Ready. The per-entry applies then skip their own appendEvent via
// the eventIndex guard.
//
// Crash semantics are the existing p > r window, widened from one entry to at
// most one Ready batch (explicitly tolerated by checkStorageInvariant): a
// crash after the event batch fsyncs but before saveAppliedIndex persists
// replays the whole batch on restart. Every entity apply is replay-idempotent:
// keyed puts are last-writer-wins, source-seq/refresh-epoch are monotonic max,
// bundled positions are config-guarded LWW, and VERSIONED applies (type /
// syncable / ingestable / database) skip re-applies via the raft-index replay
// guard (versionedLastIndex) — the last+1 version allocator alone was NOT
// idempotent over a whole-batch replay (it appended phantom versions;
// config-version-replay). appendEvent skips via the eventIndex guard. This is
// the recovery today's per-entry window uses, now proven whole-batch by
// wal/apply_conformance_test.go.
func (s *Storage) ApplyCommittedBatch(entries []*pb.Entry) error {
	fresh := entries[:0:0]
	for _, e := range entries {
		if e.GetIndex() > s.appliedIndex.Load() {
			fresh = append(fresh, e)
		}
	}
	if len(fresh) == 0 {
		// Nothing fresh to advance appliedIndex for. Any ConfState staged this
		// Ready was for an already-applied conf (durable from its original
		// apply) — drop it so it can't later flush against an unrelated
		// appliedIndex write.
		s.snapMu.Lock()
		s.pendingConfState = nil
		s.snapMu.Unlock()
		return nil
	}
	// Sequenced ingest proposals are decided before the append: a rejected
	// one is stored without its data (see ingest_order.go).
	rejected, err := s.orderVerdicts(fresh)
	if err != nil {
		return fmt.Errorf("[wal.storage] %w", err)
	}
	stored := fresh
	if len(rejected) > 0 {
		stored = make([]*pb.Entry, len(fresh))
		for i, e := range fresh {
			stored[i] = e
			if rejected[e.GetIndex()] {
				stored[i] = withoutData(e)
			}
		}
	}
	if err := s.appendEvents(stored); err != nil {
		return fmt.Errorf("[wal.storage] %w", err)
	}
	if err := s.applyEntries(fresh, rejected); err != nil {
		return err
	}
	return s.saveAppliedIndex(fresh[len(fresh)-1].GetIndex())
}

// applyEntries applies entries in order after the caller has mirrored them
// into the event log, WITHOUT persisting appliedIndex (the caller owns that).
//
// A run of consecutive DATA entries (proposals of user-topic entities only:
// ingested rows and CDC transactions, user writes) applies in ONE bbolt
// transaction. Their writes are few and self-contained (applyDataTx), so a
// Ready of N data entries costs one commit instead of up to three per entry
// (the dedup/order record, the bundled position, a refresh-epoch raise). Any
// other entry — a config, a coordination record, a rejected or undecodable
// one — first commits the run, then applies on its own exactly as before, so
// it sees every write the entries before it made, and a run never spans a
// config change it would have to observe.
//
// Each run is decoded before its transaction opens (type resolution reads
// bbolt, and a read on the apply goroutine while its write transaction is
// open can deadlock bbolt's remap), and the in-memory marks (appliedIndex, the
// data head, the scrub triggers) advance only after the commit, so a reader
// that sees appliedIndex move sees the state behind it.
//
// Crash semantics are unchanged: the run's writes commit atomically, before
// saveAppliedIndex, and every one is replay-idempotent (monotonic folds,
// config-guarded last-writer-wins positions, the tombstone's index scan).
func (s *Storage) applyEntries(entries []*pb.Entry, rejected map[uint64]bool) error {
	type decoded struct {
		index uint64
		p     *cluster.Proposal
	}
	var run []decoded
	write := false
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		if write {
			if err := s.update(func(tx *bolt.Tx) error {
				for _, d := range run {
					if err := s.applyDataTx(tx, d.index, d.p); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		for _, d := range run {
			s.noteDataApplied(d.index, d.p)
		}
		run, write = run[:0], false
		return nil
	}
	for _, e := range entries {
		idx := e.GetIndex()
		if idx <= s.appliedIndex.Load() {
			continue
		}
		if !rejected[idx] && e.GetType() == pb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(e.Data, s); err == nil && isDataProposal(p) {
				for _, entity := range p.Entities {
					s.logger.Debug("applying entity", zap.String("typeID", entity.Type.ID), zap.Uint64("index", idx))
				}
				run = append(run, decoded{index: idx, p: p})
				write = write || s.needsDataWrites(p)
				continue
			}
			// Undecodable here: applyCommitted decodes again and owns the
			// skip-or-fatal decision.
		}
		if err := flush(); err != nil {
			return err
		}
		if _, err := s.applyCommitted(e, rejected[idx]); err != nil {
			return err
		}
	}
	return flush()
}

// isDataProposal reports whether every entity is user-topic data, so the
// proposal's apply is applyDataTx alone (a user type has no entity handler).
func isDataProposal(p *cluster.Proposal) bool {
	if len(p.Entities) == 0 {
		return false
	}
	for _, e := range p.Entities {
		if cluster.IsInternal(e.Type.ID) {
			return false
		}
	}
	return true
}

// needsDataWrites reports whether applyDataTx would write anything for p, so
// a proposal (or a run) with nothing to write opens no transaction: a bbolt
// commit costs two fsyncs even when empty. A refresh-epoch raise is checked
// against the stored floor, which is a no-op for all but the first row of an
// epoch.
func (s *Storage) needsDataWrites(p *cluster.Proposal) bool {
	if len(p.Position) > 0 || (p.IngestableID != "" && (p.SourceSeq > 0 || p.IngestSeq > 0)) {
		return true
	}
	for _, e := range p.Entities {
		if isUserDefinedType(e.Type.ID) && e.IsDelete() {
			return true
		}
		if e.Generation > 0 && e.Generation > s.TopicRefreshEpoch(e.Type.ID) {
			return true
		}
	}
	return false
}

// applyDataTx makes a committed proposal's data writes within tx: delete
// tombstones, refresh-epoch raises, the ingest dedup and order records, and a
// bundled resume position. Deterministic: derived only from the committed
// proposal and replicated state.
func (s *Storage) applyDataTx(tx *bolt.Tx, index uint64, p *cluster.Proposal) error {
	for _, entity := range p.Entities {
		// Record an event-log tombstone for a user-defined delete so a
		// future Scrub can physically remove this subject's PII from the
		// permanent event log. Built-in config/coordination deletes are not
		// PII and are never scrubbed. Deterministic: keyed on the committed
		// entity and stamped with this entry's raft index, so every replica
		// stores identical bytes.
		if isUserDefinedType(entity.Type.ID) && entity.IsDelete() {
			if err := recordEventTombstoneTx(tx, entity.Type.ID, entity.Key, index); err != nil {
				return fmt.Errorf("[wal.storage] recordEventTombstone: %w", err)
			}
			// Track the retained delete's index (no subject data) for the
			// delete-key erasure cadence — reconciled at scrub completion.
			if !cluster.IsErasedKey(entity.Key) {
				if err := recordUnhashedDeleteTx(tx, index); err != nil {
					return fmt.Errorf("[wal.storage] recordUnhashedDelete: %w", err)
				}
			}
		}

		// Raise the per-topic refresh-epoch floor to the highest generation
		// ever stamped on this topic (data rows and refresh-boundary markers
		// alike carry Generation). Survives DeleteIngestable, so a same-topic
		// recreate — whose cleared position resets the epoch to 0 — resumes
		// ABOVE the generations still on the sink instead of re-emitting at
		// epoch 1 and leaving a vacuous "gen < 1" sweep. See
		// topic_refresh_epoch.go.
		if entity.Generation > 0 {
			if err := bumpTopicRefreshEpochTx(tx, entity.Type.ID, entity.Generation); err != nil {
				return fmt.Errorf("[wal.storage] bumpTopicRefreshEpoch: %w", err)
			}
		}
	}

	// Advance the per-ingestable source-seq highwater for ingest
	// proposals. Deterministic across replicas (derived only from the
	// committed proposal) and done before saveAppliedIndex so a
	// replayed entry re-applies the idempotent max. Non-ingest
	// proposals carry "" / 0 and no-op here.
	// The dedup record scopes by transaction ONLY for dialects that
	// opted in (their resume granularity is per-transaction — see
	// cluster.Proposal.TxnScopedDedup); everything else folds under the
	// legacy scalar semantics, SourceTxnID notwithstanding.
	dedupTxn := ""
	if p.TxnScopedDedup {
		dedupTxn = p.SourceTxnID
	}
	if err := advanceIngestSourceSeqTx(tx, p.IngestableID, p.SourceSeq, dedupTxn, p.IngestSeq, p.IngestSession); err != nil {
		return fmt.Errorf("[wal.storage] advanceIngestSourceSeq: %w", err)
	}

	// Persist a bundled resume checkpoint ATOMICALLY with the proposal's
	// data: a snapshot batch carries its own position (Proposal.Position) so
	// the checkpoint advances in the SAME raft entry as the rows. This closes
	// the crash window between a committed batch and a separate position
	// proposal — the gap SourceSeq dedup can't cover, because snapshot rows
	// carry SourceSeq 0. Before saveAppliedIndex so a replayed
	// entry re-applies the idempotent (last-writer-wins) put; guarded on the
	// config existing, exactly like a standalone position entity.
	if len(p.Position) > 0 {
		if err := applyBundledIngestablePositionTx(tx, p.IngestableID, p.Position); err != nil {
			return fmt.Errorf("[wal.storage] apply bundled position: %w", err)
		}
	}
	return nil
}

// noteDataApplied advances the in-memory marks for an applied proposal, after
// its writes committed.
func (s *Storage) noteDataApplied(index uint64, p *cluster.Proposal) {
	// Advance the data-entry head iff this proposal carries user topic data —
	// exactly the filter the per-syncable reader applies in reader.go (same
	// userTopicEntities helper, per-entity), so dataEventIndex tracks the
	// index the reader converges to at EOF. Internal entries (committed's
	// config + coordination) are skipped here just as the reader skips them,
	// so an idle syncable's lag reads 0 rather than a phantom backlog of
	// trailing internal entries. Monotonic: entries apply in index order.
	if len(userTopicEntities(p.Entities)) > 0 {
		s.dataEventIndex.Store(index)
	}
	for _, entity := range p.Entities {
		// Count an applied EntityKindSnapshot entity as metadata-GC backlog so
		// the automatic scrubber fires on a Snapshot-heavy cluster even with no
		// RTBF deletes — covering both internal bookkeeping and user Snapshot
		// streams. (Revision configs and Standalone dead-letters are retained,
		// not counted.) This is a leader-local trigger heuristic, so the live
		// resolved kind is fine here; it over-counts distinct-key writes
		// slightly, which is harmless (see metadataBacklog).
		if entity.Type.EntityKind == cluster.EntityKindSnapshot {
			s.metadataBacklog.Add(1)
			// Approximate the reclaimable bytes: the superseded prior entry
			// is ~the size of this, its same-key successor (+ framing).
			s.metadataBacklogBytes.Add(int64(len(entity.Key)+len(entity.Data)) + metadataEntryOverhead)
		} else if tracksCorrections(entity.Type) {
			// A write to a scrub-declared type of another kind is usually an
			// append, not a supersession, so it does not count toward the
			// volume gate; it starts the supersede ceiling instead
			// (hasSupersedeBacklog), which bounds how long a correction's
			// superseded version stays in the log.
			s.noteCorrectionWrite(index)
		}
	}
	s.appliedIndex.Store(index)
}

// applyCommitted applies one committed entry WITHOUT persisting appliedIndex
// (the caller owns that — per entry via ApplyCommitted, per batch via
// ApplyCommittedBatch) and after the caller has mirrored it into the event
// log. Reports whether the entry advanced appliedIndex. A rejected entry (a
// sequenced ingest proposal out of order, see ingest_order.go) advances
// appliedIndex and applies nothing.
func (s *Storage) applyCommitted(entry *pb.Entry, rejected bool) (bool, error) {
	// Skip already-applied entries (replay-on-restart safety). A bare
	// EntryNormal with nil data (e.g. the leader's post-election no-op
	// entry from raft) still counts as applied — we bump appliedIndex so
	// the Ready loop's invariant check stays P_local == R_local.
	if entry.GetIndex() <= s.appliedIndex.Load() {
		return false, nil
	}

	if rejected {
		s.noteIngestRejected(entry)
		s.appliedIndex.Store(entry.GetIndex())
		return true, nil
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
			// A namespaced system type from a NEWER version, marked skippable
			// (ungated): this binary can't apply it, but skipping-and-advancing
			// is safe (a coordination/observability record) and far better than
			// bricking. The raw entry is already in the event log (appended
			// above), so nothing is lost from the log; only this node's derived
			// state doesn't reflect it. A gated or unknown-state reserved type —
			// and any user-type inconsistency — still returns the error, which
			// the raft loop fatals: the loud backstop against a missed gate.
			var ure *cluster.UnknownReservedTypeError
			if errors.As(err, &ure) && ure.Skippable() {
				s.logger.Warn("skipping an unknown skippable (ungated) system entry from a newer version; advancing applied index",
					zap.Uint64("index", entry.GetIndex()),
					zap.String("typeID", ure.ID))
				s.appliedIndex.Store(entry.GetIndex())
				return true, nil
			}
			s.logger.Error("unmarshal committed proposal",
				zap.Uint64("index", entry.GetIndex()),
				zap.Stringer("entryType", entry.GetType()),
				zap.Error(err))
			return false, fmt.Errorf("[wal.storage] unmarshal proposal at index %d: %w", entry.GetIndex(), err)
		}

		for _, entity := range p.Entities {
			// Never log the raw entity Key: for a user topic entity (and especially
			// an RTBF delete) it is the erasure subject's PII. The typeID + raft
			// index correlate the entry without exposing the subject.
			s.logger.Debug("applying entity", zap.String("typeID", entity.Type.ID), zap.Uint64("index", entry.GetIndex()))
			if err := s.applyEntity(entity, entry.GetIndex()); err != nil {
				return false, err
			}
		}
		if s.needsDataWrites(p) {
			if err := s.update(func(tx *bolt.Tx) error { return s.applyDataTx(tx, entry.GetIndex(), p) }); err != nil {
				return false, err
			}
		}
		s.noteDataApplied(entry.GetIndex(), p)
		return true, nil
	}

	s.appliedIndex.Store(entry.GetIndex())
	return true, nil
}

func (s *Storage) applyEntity(entity *cluster.Entity, raftIndex uint64) error {
	for _, ie := range internalEntities {
		if ie.is(entity.ID) {
			// Every internal handler understands exactly two shapes: a config/
			// coordination row and its delete tombstone. Reject anything else
			// (a refresh-boundary marker, or a future variant) at the dispatch
			// boundary so no handler can misread it as an upsert with empty
			// data — one guard here instead of a ritual in every handler.
			switch v := entity.Variant(); v {
			case cluster.EntityVariantRow, cluster.EntityVariantDelete:
			default:
				return fmt.Errorf("[wal.storage] %s: entity variant %q is not applicable to internal type %s", ie.name, v, entity.ID)
			}
			if err := ie.handler(s, entity, raftIndex); err != nil {
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
	s.appliedIndexPersists.Add(1)

	// Snapshot the pending ConfState (staged by ConfState this Ready) and
	// marshal it BEFORE taking the bbolt write lock, so we never nest snapMu
	// inside kvMu. It is then written in the SAME transaction as appliedIndex:
	// a crash leaves either both durable (membership current, applied index
	// past the conf) or neither (membership stale/missing, applied index
	// behind the conf → raft re-delivers and applies it fresh). There is no
	// window where the applied index leads a stale ConfState.
	s.snapMu.Lock()
	pending := s.pendingConfState
	s.snapMu.Unlock()
	var pendingData []byte
	if pending != nil {
		d, err := proto.Marshal(pending)
		if err != nil {
			return fmt.Errorf("marshal conf state: %w", err)
		}
		pendingData = d
	}

	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		if err := b.Put(appliedIndexKey, buf[:]); err != nil {
			return err
		}
		// The data head rides the same transaction: it is applied state, and
		// restart must restore it exactly (the Open-time backscan is a capped
		// fallback the field defeated — see dataEventIndexKey).
		var dbuf [8]byte
		binary.BigEndian.PutUint64(dbuf[:], s.dataEventIndex.Load())
		if err := b.Put(dataEventIndexKey, dbuf[:]); err != nil {
			return err
		}
		if pendingData != nil {
			cb := tx.Bucket(confStateBucket)
			if cb == nil {
				return ErrBucketMissing
			}
			if err := cb.Put(confStateKey, pendingData); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && pending != nil {
		s.snapMu.Lock()
		if s.pendingConfState == pending {
			s.pendingConfState = nil
		}
		s.snapMu.Unlock()
	}
	return err
}

// durableConfState reads the ConfState written atomically with appliedIndex,
// or nil if none has been written (fresh node, or a pre-confStateBucket dir).
func (s *Storage) durableConfState() *pb.ConfState {
	var out *pb.ConfState
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(confStateBucket)
		if b == nil {
			return nil
		}
		data := b.Get(confStateKey)
		if len(data) == 0 {
			return nil
		}
		cs := &pb.ConfState{}
		if err := proto.Unmarshal(data, cs); err != nil {
			return nil
		}
		out = cs
		return nil
	})
	return out
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

// loadDataEventIndex reads the persisted data head, or 0 for a pre-feature
// directory (which then falls back to the capped backscan at Open).
func (s *Storage) loadDataEventIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get(dataEventIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("dataEventIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	})
	return idx, err
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
