package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// This file implements the RTBF delete-key erasure pass: rewriting a retained
// user-delete entry's raw subject key to cluster.ErasedKey once every syncable
// that could have written the raw-keyed downstream row has consumed the delete.
// It closes the last on-disk residual of an erased subject's identifier (the
// upserts are physically removed by the scrub; the delete-tombstone entry —
// load-bearing for in-flight downstream erasure — outlives them carrying the
// raw key).
//
// The pass runs inside the scrub rewrite, authorized by the committed Scrub
// command (Scrub.HashDeleteKeys — feature-gated at the proposer, see
// version.FeatureLevel level 4), and its decision is a pure function of
// replicated state so every replica rewrites identically:
//
//   - A delete at raft index D is eligible once every syncable alive at the
//     command's index S has either CONSUMED it (its committed checkpoint,
//     harvested from the log prefix <= S, is >= D) or was CREATED against an
//     already-scrubbed log (a scrub command whose bound covers D committed
//     before the syncable's config did — such a syncable can never have seen
//     the removed upsert, so it never wrote the row the raw key would erase;
//     see WaitScrubCurrent for the node-local ordering that makes this
//     sound). Both disjuncts are monotone in D, so eligibility is a single
//     threshold: eligibleMax = min over alive syncables of
//     max(checkpoint, scrub-floor), capped at the freeze line.
//   - The gate's inputs are harvested from the log prefix <= S (the command's
//     OWN index, not the freeze line): a checkpoint reset (rebuild /
//     re-materialization) that committed between the freeze line and the
//     command must be visible, or the gate would trust a checkpoint the reset
//     just revoked.
//   - Scrub commands themselves are Snapshot-kind (metadata GC keeps only the
//     latest), so the "scrub committed before the config" half cannot be
//     harvested from the log. handleScrub therefore records every applied
//     command durably (scrubHistoryBucket) on the deterministic apply path.
//
// The sentinel (not a hash) is deliberate — see cluster.ErasedKey.

// errScrubStopped aborts an in-progress rewrite when the storage is closing.
var errScrubStopped = errors.New("scrub worker stopping")

var (
	// scrubHistoryBucket records every applied Scrub command: key = the
	// command's raft index (big-endian), value = its UpperBound (big-endian).
	// Written on the apply path (deterministic, replay-idempotent by keyed
	// overwrite), read filtered to indices below a replicated coordinate, so
	// later appends never perturb an earlier read — the same discipline as
	// eventTombstoneBucket. Grows by 16 bytes per scrub command.
	scrubHistoryBucket = []byte("scrubHistory")

	// unhashedDeleteBucket tracks, by raft index (big-endian key, nil value),
	// every retained user-delete entry whose key has not yet been erased. It
	// carries NO subject data — indices only — and exists to drive the scrub
	// cadence: the scheduler keeps proposing scrubs while an un-erased delete
	// has plausibly become eligible (HasDeleteKeyEraseBacklog). Rows are
	// reconciled to the rewrite's surviving raw deletes at scrub completion.
	unhashedDeleteBucket = []byte("unhashedDeletes")

	// syncableCreateIndexBucket records, per live syncable id, the raft index
	// at which its CURRENT incarnation's config was created (absent→present
	// transition; re-POSTs don't move it, delete removes it). Maintained on
	// the apply path; used only by the live cadence heuristic — the erasure
	// gate itself harvests creation indices from the log prefix, never from
	// this mutable bucket.
	syncableCreateIndexBucket = []byte("syncableCreateIndexes")
)

// recordScrubHistory persists one applied Scrub command. Called from
// handleScrub for EVERY applied command — before its raise-the-bound
// short-circuits — so the stored history is a pure function of the committed
// log, byte-identical on every replica (the short-circuit conditions consult
// node-local scrub progress and must not shape durable state).
func recordScrubHistory(tx *bolt.Tx, cmdIndex, bound uint64) error {
	b := tx.Bucket(scrubHistoryBucket)
	if b == nil {
		return ErrBucketMissing
	}
	var k, v [8]byte
	binary.BigEndian.PutUint64(k[:], cmdIndex)
	binary.BigEndian.PutUint64(v[:], bound)
	return b.Put(k[:], v[:])
}

// recordUnhashedDelete tracks a newly applied user-delete's raft index for the
// erasure cadence. Apply-path, replay-idempotent (keyed overwrite).
func (s *Storage) recordUnhashedDelete(index uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(unhashedDeleteBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], index)
		return b.Put(k[:], nil)
	})
}

// scrubFloor returns the highest scrub bound among commands recorded strictly
// below cmdIndexBelow — the "already-scrubbed as of that point" floor. rows is
// the sorted scrubHistory slice loaded by loadScrubHistory.
func scrubFloor(rows []scrubHistoryRow, cmdIndexBelow uint64) uint64 {
	var floor uint64
	for _, r := range rows {
		if r.cmdIndex >= cmdIndexBelow {
			break
		}
		if r.bound > floor {
			floor = r.bound
		}
	}
	return floor
}

type scrubHistoryRow struct {
	cmdIndex uint64
	bound    uint64
}

// loadScrubHistory reads the recorded Scrub commands with cmdIndex <= cap,
// ascending. The cap keeps the read a pure function of the log prefix the
// caller reasons over.
func (s *Storage) loadScrubHistory(cap uint64) ([]scrubHistoryRow, error) {
	var rows []scrubHistoryRow
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(scrubHistoryBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) != 8 || len(v) != 8 {
				return nil
			}
			ci := binary.BigEndian.Uint64(k)
			if ci > cap {
				return nil
			}
			rows = append(rows, scrubHistoryRow{cmdIndex: ci, bound: binary.BigEndian.Uint64(v)})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].cmdIndex < rows[j].cmdIndex })
	return rows, nil
}

// rawDelete is one retained user-delete entry found in the gate harvest:
// its raft index and its tombstone key (type + NUL + subject key). The tk is
// held in memory only for reconciling unhashedDeleteBucket — never persisted.
type rawDelete struct {
	index uint64
	tk    string
}

// deleteKeyEraseGate computes the erasure threshold for one authorized scrub:
// every retained user delete at raft index <= eligibleMax has its key
// rewritten to cluster.ErasedKey by this rewrite. It also returns the raw
// (not-yet-erased) user deletes at index <= bound seen in the prefix, for
// reconciling the cadence bucket at completion.
//
// Deterministic: a pure function of (log prefix <= cmdIndex, scrubHistory rows
// <= cmdIndex, bound), identical on every replica — the same contract as
// tombstoneSelections/metadataSupersessions. The harvest reads the prefix up
// to the Scrub command's OWN index, not just the freeze line, so a checkpoint
// reset committed in (bound, cmdIndex] is visible and blocks the gate (the
// syncable it reset is about to re-replay from 0).
func (s *Storage) deleteKeyEraseGate(cmdIndex, bound uint64) (eligibleMax uint64, raws []rawDelete, err error) {
	history, err := s.loadScrubHistory(cmdIndex)
	if err != nil {
		return 0, nil, err
	}

	// Harvested per syncable id, from the log prefix <= cmdIndex.
	createIndex := map[string]uint64{} // current incarnation's create index
	ckpt := map[string]uint64{}        // latest committed checkpoint (0 after a reset)

	first, err := s.firstEventSeq()
	if err != nil {
		return 0, nil, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return 0, nil, err
	}
	if first != 0 && last != 0 {
		for seq := first; seq <= last; seq++ {
			raw, rerr := s.readEventAt(seq)
			if rerr != nil {
				return 0, nil, rerr
			}
			pe := &pb.Entry{}
			if uerr := proto.Unmarshal(raw, pe); uerr != nil {
				return 0, nil, uerr
			}
			// Seqs are append order = raft-index order; stop past the command.
			if pe.GetIndex() > cmdIndex {
				break
			}
			if pe.GetType() != pb.EntryNormal || pe.Data == nil {
				continue
			}
			idx := pe.GetIndex()
			if err := cluster.ForEachProposalEntity(pe.Data, func(typeID string, key, data []byte, isDelete bool) error {
				switch {
				case cluster.IsSyncable(typeID):
					id := string(key)
					if isDelete {
						delete(createIndex, id)
						delete(ckpt, id)
					} else if _, alive := createIndex[id]; !alive {
						createIndex[id] = idx // create; re-POSTs don't move it
					}
				case cluster.IsSyncableIndex(typeID):
					id := string(key)
					if isDelete {
						ckpt[id] = 0 // rebuild/re-materialization reset
					} else {
						si := &cluster.SyncableIndex{}
						if uerr := si.Unmarshal(data); uerr != nil {
							// The value decodes on every replica or none —
							// surface it rather than silently skewing the gate.
							return uerr
						}
						ckpt[id] = si.Index
					}
				case isUserDefinedType(typeID) && isDelete && !cluster.IsErasedKey(key) && idx <= bound:
					raws = append(raws, rawDelete{index: idx, tk: string(tombstoneKey(typeID, key))})
				}
				return nil
			}); err != nil {
				return 0, nil, err
			}
		}
	}

	eligibleMax = bound
	for id, c := range createIndex {
		allowance := ckpt[id]
		if floor := scrubFloor(history, c); floor > allowance {
			allowance = floor
		}
		if allowance < eligibleMax {
			eligibleMax = allowance
		}
	}
	return eligibleMax, raws, nil
}

// reconcileUnhashedDeletes rewrites the cadence bucket's rows at index <=
// bound to exactly the raw deletes that SURVIVED the rewrite un-erased: those
// above the gate threshold that metadata GC did not drop. Rows above the bound
// are untouched (they were outside this rewrite). Called from scrub completion;
// content is deterministic (a function of the same inputs as the rewrite),
// timing is node-local — the bucket only feeds the local propose heuristic.
func (s *Storage) reconcileUnhashedDeletes(bound, eligibleMax uint64, raws []rawDelete, msel map[string]uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(unhashedDeleteBucket)
		if b == nil {
			return ErrBucketMissing
		}
		c := b.Cursor()
		var stale [][]byte
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if len(k) == 8 && binary.BigEndian.Uint64(k) <= bound {
				stale = append(stale, append([]byte(nil), k...))
			}
		}
		for _, k := range stale {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		for _, rd := range raws {
			if rd.index <= eligibleMax {
				continue // erased by this rewrite
			}
			if m := msel[rd.tk]; m != 0 && rd.index < m {
				continue // dropped by metadata GC (superseded Snapshot-kind delete)
			}
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], rd.index)
			if err := b.Put(k[:], nil); err != nil {
				return err
			}
		}
		return nil
	})
}

// recordSyncableCreateIndex maintains the live create-index record inside the
// config apply transaction: set on the absent→present transition, cleared on
// delete. Caller passes whether the config existed before this apply.
func recordSyncableCreateIndex(tx *bolt.Tx, id []byte, raftIndex uint64, existedBefore, isDelete bool) error {
	b := tx.Bucket(syncableCreateIndexBucket)
	if b == nil {
		return ErrBucketMissing
	}
	if isDelete {
		return b.Delete(id)
	}
	if existedBefore {
		return nil
	}
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], raftIndex)
	return b.Put(id, v[:])
}

// HasDeleteKeyEraseBacklog reports whether an un-erased user-delete has
// plausibly become eligible for key erasure — the cadence trigger the scrub
// scheduler consults once the feature is cluster-enabled. A live approximation
// of the deterministic gate (live checkpoints, live create indices): a false
// negative merely delays the next scrub a tick, a false positive costs one
// rewrite that erases nothing. Both are bounded and harmless; the committed
// command's own gate decides what is actually rewritten.
func (s *Storage) HasDeleteKeyEraseBacklog() bool {
	history, err := s.loadScrubHistory(^uint64(0))
	if err != nil {
		return false
	}
	eligible := ^uint64(0)
	var minUnhashed uint64
	haveUnhashed := false
	err = s.view(func(tx *bolt.Tx) error {
		ub := tx.Bucket(unhashedDeleteBucket)
		if ub == nil {
			return ErrBucketMissing
		}
		k, _ := ub.Cursor().First()
		if len(k) != 8 {
			return nil
		}
		minUnhashed = binary.BigEndian.Uint64(k)
		haveUnhashed = true

		cb := tx.Bucket(syncableCreateIndexBucket)
		ib := tx.Bucket(syncableIndexBucket)
		if cb == nil {
			return ErrBucketMissing
		}
		return cb.ForEach(func(id, v []byte) error {
			var allowance uint64
			if len(v) == 8 {
				allowance = scrubFloor(history, binary.BigEndian.Uint64(v))
			}
			if ib != nil {
				if raw := ib.Get(id); raw != nil {
					si := &cluster.SyncableIndex{}
					if uerr := si.Unmarshal(raw); uerr == nil && si.Index > allowance {
						allowance = si.Index
					}
				}
			}
			if allowance < eligible {
				eligible = allowance
			}
			return nil
		})
	})
	if err != nil || !haveUnhashed {
		return false
	}
	return minUnhashed <= eligible
}

// BeginFromZeroRead registers an in-flight from-0 log read (a fresh
// syncable's replay, a rebuild, stage-state recovery) and returns its release.
// The scrub swap waits for every registered read to finish, so a from-0 read
// always observes ONE log state — never a raw upsert from the old state paired
// with an erased delete from the new one. Any single state is pair-consistent
// (an erased delete implies its upserts were already removed by the same or an
// earlier rewrite), which is the invariant the erasure gate's soundness rests
// on. Release is idempotent. Holding a pin only delays THIS node's rewrite
// timing; the rewrite's content is fixed by the committed command either way.
func (s *Storage) BeginFromZeroRead() func() {
	s.fromZeroMu.Lock()
	s.fromZeroReads++
	s.fromZeroMu.Unlock()
	released := false
	return func() {
		s.fromZeroMu.Lock()
		if !released {
			released = true
			s.fromZeroReads--
		}
		s.fromZeroMu.Unlock()
	}
}

// waitFromZeroReads blocks the scrub worker until no from-0 reads are in
// flight, aborting on shutdown. Called before the swap lock — never under it —
// so pinned readers keep reading (and raft keeps appending) while it waits. A
// long-held pin (a from-0 replay stalled on its sink) delays this node's
// rewrite, loudly; the pending bound survives and the worker retries.
func (s *Storage) waitFromZeroReads() error {
	warn := time.NewTicker(30 * time.Second)
	defer warn.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		s.fromZeroMu.Lock()
		n := s.fromZeroReads
		s.fromZeroMu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-s.scrubStop:
			return errScrubStopped
		case <-warn.C:
			s.logger.Warn("scrub swap waiting on in-flight from-0 log reads (a stalled fresh replay delays this node's rewrite)",
				zap.Int("fromZeroReads", n))
		case <-tick.C:
		}
	}
}

// WaitScrubCurrent blocks until this node's local event log reflects every
// Scrub command committed so far (lastScrubbedBound has caught up to the
// durable pending bound), kicking the scrub worker along. A sync worker about
// to REPLAY FROM INDEX 0 calls this before its first read: the erasure gate
// exempts syncables created after a covering scrub on the grounds that they
// only ever read already-scrubbed logs — this wait is what makes that true on
// every node the replay may run on (and keeps a from-0 replay from spanning a
// local rewrite swap that erases a delete key mid-read). Resumed reads need no
// wait: a checkpoint at K only reads entries above K, and the gate never
// erases a delete a live checkpoint has not consumed.
func (s *Storage) WaitScrubCurrent(done <-chan struct{}) error {
	target, err := s.loadPendingScrubBound()
	if err != nil {
		return err
	}
	if target == 0 || s.lastScrubbedBound.Load() >= target {
		return nil
	}
	s.signalScrub()
	s.logger.Info("waiting for the local scrub to catch up before a from-0 replay",
		zap.Uint64("pendingBound", target), zap.Uint64("completedBound", s.lastScrubbedBound.Load()))
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return context.Canceled
		case <-tick.C:
			if s.lastScrubbedBound.Load() >= target {
				return nil
			}
		}
	}
}
