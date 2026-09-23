package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// This file implements the RTBF scrubber: physical removal of already-delete-
// proposed entities from the permanent event log. The mechanism is documented
// in docs/event-log-architecture.md § "Right-to-be-forgotten / deletes".
//
// Flow:
//   - A Scrub command commits carrying an upper-bound raft index B (the freeze
//     line). handleScrub records "pending bound B" in bbolt and pokes the
//     worker — cheap, never blocking the raft Ready loop.
//   - The background worker rewrites the event log: copy every entry, dropping
//     entities tombstoned with a delete index in (entry.Index, B], keep delete-
//     tombstones, copy entries with raft index > B verbatim. It writes a fresh
//     sibling directory then swaps it in under eventMu.Lock.
//
// Determinism: the survivor set is a pure function of (event-log bytes,
// tombstone selections filtered to <= B, B), all identical on every replica, so
// every node produces a byte-identical event log. EventIndex never regresses
// because the tail (raft index > B — at minimum the Scrub command's own mirrored
// entry) is never a removal candidate.

// handleScrub applies a committed Scrub command. It records the command in the
// durable scrub history (unconditionally — the history must be a pure function
// of the log, and the short-circuit below consults node-local progress),
// records the requested bound, and signals the worker; the O(N) rewrite
// happens off the Ready loop. Skipping when the bound is already completed
// avoids a redundant full rewrite for a stale or duplicate command.
func (s *Storage) handleScrub(e *cluster.Entity, raftIndex uint64) error {
	sc := &cluster.Scrub{}
	if err := sc.Unmarshal(e.Data); err != nil {
		return err
	}
	if err := s.update(func(tx *bolt.Tx) error {
		return recordScrubHistory(tx, raftIndex, sc.UpperBound)
	}); err != nil {
		return err
	}
	if sc.UpperBound <= s.lastScrubbedBound.Load() {
		return nil
	}
	if err := s.setPendingScrubBound(sc.UpperBound, sc.HashDeleteKeys, raftIndex); err != nil {
		return err
	}
	s.signalScrub()
	return nil
}

// setPendingScrubBound raises the persisted pending bound to max(existing, b).
// Monotonic so a later command never lowers an in-flight bound. The erasure
// authorization (hash) and the command's own raft index ride the pending
// record with the bound they arrived on: the worker executes the highest
// pending bound under that command's authorization, and the gate harvest is
// bounded by that command's index (see deleteKeyEraseGate).
func (s *Storage) setPendingScrubBound(b uint64, hash bool, cmdIndex uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		if cur := bkt.Get(pendingScrubBoundKey); len(cur) == 8 && binary.BigEndian.Uint64(cur) >= b {
			return nil
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], b)
		if err := bkt.Put(pendingScrubBoundKey, buf[:]); err != nil {
			return err
		}
		var flag [1]byte
		if hash {
			flag[0] = 1
		}
		if err := bkt.Put(pendingScrubHashKey, flag[:]); err != nil {
			return err
		}
		var ci [8]byte
		binary.BigEndian.PutUint64(ci[:], cmdIndex)
		return bkt.Put(pendingScrubCmdIndexKey, ci[:])
	})
}

// signalScrub pokes the worker without blocking; a poke that arrives while one
// is already queued is dropped (the worker re-reads the latest bound anyway).
func (s *Storage) signalScrub() {
	select {
	case s.scrubSignal <- struct{}{}:
	default:
	}
}

// stopScrubWorker signals the worker to exit and waits for it. Idempotent.
func (s *Storage) stopScrubWorker() {
	s.scrubStopOnce.Do(func() {
		close(s.scrubStop)
		<-s.scrubDone
	})
}

// scrubWorker runs the rewrite off the raft Ready loop. It resumes any pending
// scrub on startup (a Scrub command may have committed before a crash, leaving a
// durable bound that the replay-skipped entry won't re-fire) and thereafter runs
// on each signal.
func (s *Storage) scrubWorker() {
	defer close(s.scrubDone)
	// Only temporary admission failures are retried automatically. Backend
	// failures may require reopen and must not become a tight retry loop.
	retry := time.NewTimer(time.Hour)
	retry.Stop()
	defer retry.Stop()
	for {
		if err := s.runOwedCompaction(); err != nil {
			s.logger.Error("run owed compaction", zap.Error(err))
		}
		err := s.runPendingScrub()
		switch {
		case errors.Is(err, errScrubStopped):
			return
		case errors.Is(err, errEventRewriteDeferred), errors.Is(err, ErrLayoutFrozen), errors.Is(err, errScrubApplyPending):
			retry.Reset(50 * time.Millisecond)
		case err != nil:
			s.logger.Error("run pending scrub", zap.Error(err))
		}
		select {
		case <-s.scrubStop:
			return
		case <-s.scrubSignal:
			retry.Stop()
		case <-retry.C:
		}
	}
}

// runPendingScrub rewrites the event log for every pending bound not yet
// completed, looping so a bound raised during a rewrite is picked up
// immediately.
func (s *Storage) runPendingScrub() error {
	s.eventMu.RLock()
	shared := s.eventLog.managed != nil
	s.eventMu.RUnlock()
	if shared {
		return s.runPendingSharedScrub()
	}
	for {
		bound, hash, cmdIndex, err := s.loadPendingScrub()
		if err != nil {
			return err
		}
		if bound == 0 || bound <= s.lastScrubbedBound.Load() {
			return nil
		}
		// Not over a log that is being filled from a peer (BeginCatchUp), nor
		// over an empty one — a rewrite of nothing would fatal on the
		// invariant that the tail survives, and a rewrite of a partial log
		// would stamp a generation its content does not have. The release
		// after the install re-signals.
		if s.catchingUp.Load() {
			s.logger.Info("pending scrub deferred: the event log is being filled from a peer", zap.Uint64("pendingBound", bound))
			return nil
		}
		if last, err := s.lastEventSeq(); err != nil {
			return err
		} else if last == 0 {
			s.logger.Info("pending scrub deferred: the event log is empty", zap.Uint64("pendingBound", bound))
			return nil
		}
		erase, err := s.runScrub(bound, hash, cmdIndex)
		if err != nil {
			return err
		}
		// Reconcile the erasure-cadence bucket to the rewrite's surviving raw
		// deletes BEFORE advancing the completed bound, so "completed" implies
		// "reconciled" (idempotent: a crash here re-runs the rewrite and this
		// reconcile on retry).
		if erase != nil {
			if err := s.reconcileUnhashedDeletes(bound, erase.eligibleMax, erase.raws, erase.msel); err != nil {
				return err
			}
		}
		// markScrubComplete prunes the now-dead tombstones and compacts bbolt in one
		// kvMu.Lock critical section, so no snapshot can copy a freed-but-uncompacted
		// erased key.
		if err := s.markScrubComplete(bound); err != nil {
			return err
		}
	}
}

// eraseOutcome carries one authorized rewrite's delete-key erasure results to
// the post-completion reconcile of the cadence bucket.
type eraseOutcome struct {
	eligibleMax uint64
	raws        []rawDelete
	msel        map[string]uint64
}

// runScrub prepares application policy and executes the native rewrite. Physical
// execution must finish before the caller marks completion or reconciles erasure.
func (s *Storage) runScrub(bound uint64, hash bool, cmdIndex uint64) (*eraseOutcome, error) {
	plan, err := s.prepareScrubPlan(bound, hash, cmdIndex)
	if err != nil {
		return nil, err
	}
	if err := s.rewriteLegacyEvents(plan); err != nil {
		return nil, err
	}
	return plan.erase, nil
}

// reopenEventLogAfterSwapOrFatal reopens the permanent event log at
// s.eventLogDir and reassigns s.eventLog, or FATALS if the reopen fails — the
// event-log analogue of reopenKVAfterSwapOrFatal. A returned error would leave
// s.eventLog closed for the next appendEvent to hit as ErrClosed: a delayed
// crash mis-attributed to the append path, since runScrub's caller only logs and
// continues. The caller MUST ensure s.eventLogDir holds the intended (non-empty)
// log before calling — reopening a missing dir would create an empty log (silent
// event loss). Caller holds eventMu.Lock.
// closeEventLogBeforeSwapOrFatal closes the live event-log handle ahead of the
// scrub rename, or FATALS if the close fails — the pre-swap analogue of the
// reopen/recompute ...OrFatal helpers. A returned error would drop into the
// survive-and-continue scrub worker (runPendingScrub only logs and continues),
// leaving the dead handle for the next appendEvent to hit as an ErrClosed crash
// mis-attributed to append. Fataling attributes it at the true site;
// s.eventLogDir is still the original intact log (no rename yet), so a restart
// recovers. Caller holds eventMu.Lock.
func (s *Storage) closeEventLogBeforeSwapOrFatal(what string) {
	if err := s.eventLog.Close(); err != nil {
		s.logger.Fatal("close event log before scrub swap failed; the node cannot continue (restart to recover from the on-disk log)",
			zap.String("op", what), zap.String("path", s.eventLogDir), zap.Error(err))
	}
}

func (s *Storage) reopenEventLogAfterSwapOrFatal(what string) {
	reopened, err := tidwallbackend.OpenLegacy(s.eventLogDir, s.eventOpenOptions)
	if err != nil {
		s.logger.Fatal("event-log swap could not reopen storage; the node cannot continue (restart to recover via recoverScrubDirs)",
			zap.String("op", what), zap.Error(err))
	}
	s.eventLog = bindLegacyEventLog(reopened, s.metrics)
}

// recomputeEventBoundsAfterSwapOrFatal recomputes the in-memory event bounds
// after a committed event-log swap, or FATALS if the recompute fails — the
// post-swap analogue of reopenEventLogAfterSwapOrFatal. Once the swap has
// committed, the on-disk log is the new (re-densified) one; a returned error
// would drop into the survive-and-continue scrub worker, which keeps serving
// with stale eventIndex/firstEventIndex and an un-bumped scrubGen — so in-flight
// Readers never re-derive their walSeq cursor and silently read wrong offsets.
// A restart recomputes the bounds cleanly (Open does). Caller holds eventMu.Lock
// and has already reopened s.eventLog at the swapped-in dir.
func (s *Storage) recomputeEventBoundsAfterSwapOrFatal(what string) {
	if err := s.recomputeEventBoundsLocked(); err != nil {
		s.logger.Fatal("event-log swap committed but in-memory bounds could not be recomputed; the node cannot continue with stale bounds (restart to recover)",
			zap.String("op", what), zap.Error(err))
	}
}

// scrubFilterEntry decides the fate of one event-log record (raw = unframed
// pb.Entry bytes). It returns whether to keep the record and, if kept, the
// payload to write (the original bytes verbatim, or a re-marshaled entry with
// removable entities dropped). Entries without a proposal (conf changes, the
// leader no-op) are kept verbatim. Two removal reasons compose, each false for
// any entry at index > bound (both selections are capped at bound), so the tail
// is always kept:
//
//   - RTBF (user tombstone): a NON-delete entity at raft index I whose
//     (type, key) has a delete at D with I < D <= bound — the entity was written
//     before a delete within the freeze line. Deletes are spared so the
//     tombstone survives. (sel values are the max delete index <= bound.)
//   - Metadata GC (system tombstone): an EntityKindSnapshot entity — an internal
//     built-in OR a user topic — upsert OR delete, at index I superseded by a
//     later entry for the same (type, key) at M <= bound (I < M). Only the latest
//     per key survives; a superseded delete is droppable too. (msel values are
//     that M.)
//
// sel and msel are NOT disjoint (a user EntityKindSnapshot key with a delete is
// in both), but the two predicates are ORed and provably agree on any overlap —
// RTBF spares the delete-tombstone, metadata GC keeps the latest per key — so an
// entity is removed iff either fires, with no conflict.
func scrubFilterEntry(raw []byte, sel, msel map[string]uint64, eraseMax uint64) (keep bool, payload []byte, err error) {
	pe := &pb.Entry{}
	if err := proto.Unmarshal(raw, pe); err != nil {
		return false, nil, err
	}
	if pe.GetType() != pb.EntryNormal || pe.Data == nil {
		return true, raw, nil
	}
	idx := pe.GetIndex()
	remove := func(typeID string, key []byte, isDelete bool) bool {
		tk := string(tombstoneKey(typeID, key))
		// RTBF: spare deletes (the tombstone must survive).
		if !isDelete {
			if d := sel[tk]; d != 0 && idx < d {
				return true
			}
		}
		// Metadata GC: drop anything below the latest entry for this key.
		if m := msel[tk]; m != 0 && idx < m {
			return true
		}
		return false
	}
	// Delete-key erasure (third removal reason's sibling — a REWRITE, not a
	// removal): a spared user-delete at or below the gate threshold has its raw
	// subject key replaced with the erased sentinel. eraseMax 0 = pass disabled
	// (an unauthorized or pre-upgrade scrub). Idempotent: an already-erased key
	// is left alone, so re-scrubs keep the bytes stable.
	var rewriteKey func(typeID string, key []byte) []byte
	if eraseMax != 0 {
		rewriteKey = func(typeID string, key []byte) []byte {
			if idx <= eraseMax && isUserDefinedType(typeID) && !cluster.IsErasedKey(key) {
				return cluster.ErasedKey
			}
			return nil
		}
	}
	newData, allRemoved, changed, err := cluster.ScrubProposalEntities(pe.Data, remove, rewriteKey)
	if err != nil {
		return false, nil, err
	}
	if allRemoved {
		return false, nil, nil
	}
	if !changed {
		return true, raw, nil
	}
	pe.Data = newData
	nb, err := proto.Marshal(pe)
	if err != nil {
		return false, nil, err
	}
	return true, nb, nil
}

// metadataSupersessions scans the event log prefix at raft index <= bound and
// returns, per Snapshot (type, key), the highest raft index <= bound at which an
// entry for that key appears. scrubFilterEntry drops any entry for such a key
// whose index is strictly below this max, so only the latest committed value per
// key survives a scrub — bounding the growth of superseded internal bookkeeping
// (SyncableIndex / position / stuck / skip) AND superseded user
// EntityKindSnapshot streams (see metadata-gc-scrubber + compact-user-snapshot-
// streams).
//
// Compactability = EntityKindSnapshot. Internal Snapshot built-ins are resolved
// by IsSystemTombstonable. A user type's kind is harvested from its type
// registrations as we scan (latest registration wins, in index order): this
// keeps kind resolution a pure function of the log prefix <= bound — and so
// determinism-safe — rather than reading the mutable, deletable live type
// bucket at worker time. typeType is EntityKindRevision (retained, never
// compacted), so every registration is present in the log before the data that
// references it, and the harvest always resolves.
//
// Deterministic: a pure function of the log prefix <= bound, identical on every
// replica, like tombstoneSelections. Keyed by tombstoneKey(type, key) so it
// shares that encoding; disjoint from the RTBF selection (a user delete is
// handled by RTBF, not here). Runs in scrub phase A under the event publication read lock. Appends
// can continue; entries at index > bound are excluded.
func (s *Storage) metadataSupersessions(bound uint64) (map[string]uint64, error) {
	selection := newMetadataSelection()
	err := s.scanEventEntries(bound, selection.observe)
	if err != nil {
		return nil, err
	}
	return selection.latest, nil
}

// recomputeEventBoundsLocked refreshes firstEventIndex/eventIndex from the
// rewritten log. Caller must hold eventMu.Lock. EventIndex (P_local) must never
// regress — the tail is never removed — so this asserts the recomputed value
// equals the existing one and refuses to lower it (a lower value would trip the
// Ready loop's P==R invariant check and fatal-exit the node).
func (s *Storage) recomputeEventBoundsLocked() error {
	first, last, err := s.eventBoundsLocked()
	if err != nil {
		return err
	}
	if last == 0 {
		return fmt.Errorf("scrub emptied the event log: the tail must always survive")
	}
	prev := s.eventIndex.Load()
	if last != prev {
		return fmt.Errorf("scrub changed EventIndex from %d to %d; the tail must be preserved", prev, last)
	}
	s.firstEventIndex.Store(first)
	s.eventIndex.Store(last)
	return nil
}

// loadPendingScrubBound reads the highest requested scrub bound (0 if none).
func (s *Storage) loadPendingScrubBound() (uint64, error) {
	return s.loadScrubUint(pendingScrubBoundKey)
}

// loadPendingScrub reads the pending scrub record: the highest requested bound
// plus the erasure authorization and command index of the command that raised
// it (see setPendingScrubBound). Absent flag/index read as zero values — the
// pre-upgrade record shape — which disables the erasure pass for that rewrite.
func (s *Storage) loadPendingScrub() (bound uint64, hash bool, cmdIndex uint64, err error) {
	err = s.view(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		if b := bkt.Get(pendingScrubBoundKey); len(b) == 8 {
			bound = binary.BigEndian.Uint64(b)
		}
		if fb := bkt.Get(pendingScrubHashKey); len(fb) == 1 && fb[0] == 1 {
			hash = true
		}
		if ci := bkt.Get(pendingScrubCmdIndexKey); len(ci) == 8 {
			cmdIndex = binary.BigEndian.Uint64(ci)
		}
		return nil
	})
	return bound, hash, cmdIndex, err
}

// loadScrubCompleted reads the highest completed scrub bound (0 if none).
func (s *Storage) loadScrubCompleted() (uint64, error) {
	return s.loadScrubUint(scrubCompletedKey)
}

func (s *Storage) loadScrubUint(key []byte) (uint64, error) {
	var v uint64
	err := s.view(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		b := bkt.Get(key)
		if len(b) == 8 {
			v = binary.BigEndian.Uint64(b)
		}
		return nil
	})
	return v, err
}

// markScrubComplete advances the persisted + in-memory completed bound after a
// successful event-log swap, prunes the tombstones the scrub made dead weight, and
// — atomically under the same kvMu.Lock — compacts bbolt so the freed raw keys
// can't ride in a snapshot.
//
// The prune and the compaction MUST be one critical section. CreateSnapshot
// serializes bbolt under kvMu.RLock and its tx.WriteTo copies free pages, so a gap
// between a committed prune and the compaction would let a concurrent snapshot copy
// the freed-but-uncompacted tombstone page (the erased subject key) into a durable,
// replicated snapshot. Holding kvMu.Lock across both makes the intermediate state
// unobservable.
func (s *Storage) markScrubComplete(bound uint64) error {
	// Hold kvMu.Lock across the prune AND the compaction (see above). The prune
	// writes directly on the handle, not via s.update, which takes kvMu.RLock and
	// would deadlock under the Lock — same reason RestoreSnapshot reads directly.
	s.kvMu.Lock()
	defer s.kvMu.Unlock()

	var pruned bool
	err := s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		if cur := bkt.Get(scrubCompletedKey); len(cur) == 8 && binary.BigEndian.Uint64(cur) >= bound {
			return nil
		}
		// RTBF: the rewrite that just ran removed the upserts these tombstones
		// pointed at, so the tombstones (raw subject keys) are now dead weight —
		// prune them in the same tx that advances the bound. That erases the raw
		// key from bbolt's logical tree; the compaction below drops the freed bytes.
		var perr error
		pruned, perr = pruneTombstonesLE(tx, bound)
		if perr != nil {
			return perr
		}
		if pruned {
			// A prune only FREES the pages holding the raw subject key; the bytes
			// remain until compaction rewrites the file, and CreateSnapshot copies
			// free pages. Record "compaction owed" IN THIS TX so that if the
			// compaction below fails (ENOSPC) or the process crashes before it
			// finishes, the physical erasure is re-driven on the next Open / scrub
			// signal (runOwedCompaction) — the "completed" bound advanced here must
			// NOT be the only gate, or the erasure would be suppressed forever.
			if err := bkt.Put(scrubCompactOwedKey, []byte{1}); err != nil {
				return err
			}
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bound)
		return bkt.Put(scrubCompletedKey, buf[:])
	})
	if err != nil {
		return err
	}
	// Still under kvMu.Lock: compact away the freed tombstone pages before any
	// reader (CreateSnapshot) can observe them. Only when something was pruned —
	// compaction is an O(bbolt) rewrite. On success this clears the owed marker;
	// on failure the marker persists so the erasure completes on a later retry.
	if pruned {
		if err := s.compactAndClearOwedLocked(); err != nil {
			return err
		}
	}
	// Advance the atomic only after the durable write (and compaction) succeed.
	for {
		cur := s.lastScrubbedBound.Load()
		if bound <= cur || s.lastScrubbedBound.CompareAndSwap(cur, bound) {
			break
		}
	}
	// The rewrite that just completed removed every superseded metadata entry at
	// index <= bound, so the metadata-GC backlog is cleared. Writes that arrived
	// during the scrub (index > bound) aren't reflected; they re-accumulate and
	// drive the next scrub. Exactness isn't required (see metadataBacklog).
	s.metadataBacklog.Store(0)
	s.metadataBacklogBytes.Store(0)
	return nil
}

// compactAndClearOwedLocked physically compacts bbolt — dropping the freed
// tombstone-key pages so the erased subject identifier leaves the file and every
// subsequent snapshot — and, ONLY on success, clears the durable
// compaction-owed marker. Caller holds kvMu.Lock. On a compaction failure the
// marker stays set, so runOwedCompaction re-drives the erasure later.
func (s *Storage) compactAndClearOwedLocked() error {
	if err := s.compactLocked(); err != nil {
		return err
	}
	return s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		return bkt.Delete(scrubCompactOwedKey)
	})
}

// runOwedCompaction re-drives a bbolt compaction that a prior markScrubComplete
// pruned RTBF tombstones for but did not finish — a crash or a non-fatal
// compaction error (e.g. ENOSPC) in the window after the prune committed and the
// "completed" bound advanced. Because compaction is gated on "did THIS call
// prune", a retry never re-satisfies it and the already-advanced bound
// suppresses re-run; the durable scrubCompactOwedKey marker breaks that, so the
// physical erasure completes on the next Open / scrub signal instead of leaving
// the subject key in bbolt (and every snapshot) forever. Idempotent — a no-op
// when nothing is owed, and compacting an already-compact file is harmless.
func (s *Storage) runOwedCompaction() error {
	s.kvMu.Lock()
	defer s.kvMu.Unlock()
	var owed bool
	if err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return nil
		}
		owed = bkt.Get(scrubCompactOwedKey) != nil
		return nil
	}); err != nil {
		return err
	}
	if !owed {
		return nil
	}
	return s.compactAndClearOwedLocked()
}

// metadataBacklogThreshold is how many system-tombstonable metadata writes must
// accumulate since the last completed scrub before HasScrubBacklog reports
// metadata work. Unlike RTBF erasure (legally urgent — any single unscrubbed
// delete triggers a scrub), metadata GC only reclaims space, so it batches: an
// O(N) full-log rewrite isn't worth triggering to drop a handful of superseded
// entries. This bounds lingering superseded metadata to roughly this many writes
// between scheduled scrubs. A heuristic, not a correctness boundary — the
// rewrite's removal set is exact and deterministic regardless of when it fires.
// A var (not const) only so a test can lower it (SetMetadataBacklogThresholdForTest).
var metadataBacklogThreshold int64 = 128

// metadataEntryOverhead is the per-entry framing/protobuf overhead added to a
// counted supersession's key+data size when estimating reclaimable bytes.
const metadataEntryOverhead = 64

// scrubConvergeRounds bounds the unlocked catch-up passes (phase A' in runScrub)
// that chase the delta appended while the rewrite runs, before it takes
// eventMu.Lock for the final swap. Each pass shrinks the delta (a NoSync copy
// outruns fsync'd appends); the cap guarantees termination against a writer we
// cannot outrun — the locked phase then just copies whatever remains. A latency
// heuristic, not a correctness boundary — phase B catches up to the true tail
// regardless.
const scrubConvergeRounds = 8

// scrubConvergeResidue is the delta (in entries) small enough to copy under the
// lock, so a convergence pass stops chasing once within it. A var (not const) so
// a test can shrink it to force the convergence path on a small delta.
var scrubConvergeResidue uint64 = 4096

// Volume gate for the metadata-only scrub. The rewrite's COST is O(total log
// size) — read the whole log twice, rewrite every survivor — while its BENEFIT
// is only the superseded metadata it drops. The count threshold alone let a
// steady trickle of checkpoint bumps (seconds' worth under sync load) trigger a
// full-log rewrite EVERY scheduler tick, forever: at a 50 GiB log that is
// ~150 GiB of I/O per node per hour to reclaim kilobytes, plus a transient ~2×
// disk spike per cycle. On a log larger than metadataScrubMinLogBytes the
// metadata term therefore also requires the estimated reclaimable bytes to be
// at least logSize/metadataScrubReclaimDivisor (~6%), so scrub I/O is
// proportional to what it reclaims. Below the floor the rewrite is cheap and
// the count threshold alone governs — the pre-gate behavior, which keeps small
// deployments (and the test suite) unchanged. RTBF erasure is deliberately NOT
// gated (legally urgent — see HasScrubBacklog). Vars so tests can shrink them.
var (
	metadataScrubMinLogBytes    int64 = 256 << 20
	metadataScrubReclaimDivisor int64 = 16
)

// hasMetadataBacklog reports whether enough superseded metadata has accumulated
// to be WORTH an O(total-log) rewrite — the count threshold plus, on a large
// log, the reclaimable-volume gate above. On a size-read error (e.g. a
// concurrent scrub swap moved the directory) it falls back to the count-only
// behavior: mid-swap means a scrub just ran and the counters are about to
// reset, so the conservative fallback is momentary.
func (s *Storage) hasMetadataBacklog() bool {
	if s.metadataBacklog.Load() < metadataBacklogThreshold {
		return false
	}
	size, err := s.eventLogApproxSize()
	if err != nil || int64(size) <= metadataScrubMinLogBytes { //nolint:gosec // G115: segment-file sums are far below int64 max
		return true
	}
	return s.metadataBacklogBytes.Load() >= int64(size)/metadataScrubReclaimDivisor //nolint:gosec // G115: as above
}

// HasScrubBacklog reports whether the next scrub has anything to physically
// remove — either RTBF erasure (a delete-tombstone beyond the highest completed
// bound) or enough accumulated superseded metadata to be worth an O(N) rewrite.
// It is the single "is there scrubbable work?" signal the automatic scheduler
// (db.scrubScheduler) consults, deliberately covering both jobs the one rewrite
// pass does, so an idle cadence tick skips the rewrite. A metadata-heavy,
// RTBF-free cluster triggers via the metadata term (see metadata-gc-scrubber).
func (s *Storage) HasScrubBacklog() bool {
	return s.hasRTBFBacklog() || s.hasMetadataBacklog()
}

// hasRTBFBacklog reports whether any tombstone records a delete at an index
// beyond the highest completed scrub bound — i.e. there is RTBF erasure the next
// scrub would physically remove. Scans the tombstone bucket; cheap relative to
// the scrub cadence and the set is the RTBF working set, not the whole log.
func (s *Storage) hasRTBFBacklog() bool {
	bound := s.lastScrubbedBound.Load()
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(eventTombstoneBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.ForEach(func(_, v []byte) error {
			for off := 0; off+8 <= len(v); off += 8 {
				if binary.BigEndian.Uint64(v[off:off+8]) > bound {
					found = true
					return nil
				}
			}
			return nil
		})
	})
	if err != nil {
		s.logger.Warn("scan scrub backlog", zap.Error(err))
		return false
	}
	return found
}

// Scrub working-directory names, all siblings of events/ so a rename is an
// atomic same-filesystem operation. The names, and the Open-time recovery that
// reaps them (datadir.RecoverScrubDirs), live in the datadir package so they
// cannot drift from the backup tool's read-only view of the same residue.
func (s *Storage) scrubTmpDir(bound uint64) string {
	return datadir.ScrubDir(s.eventLogDir, bound)
}

func (s *Storage) eventRetiredDir() string {
	return datadir.RetiredDir(s.eventLogDir)
}
