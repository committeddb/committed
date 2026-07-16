package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
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

// handleScrub applies a committed Scrub command. It records the requested bound
// and signals the worker; the O(N) rewrite happens off the Ready loop. Skipping
// when the bound is already completed avoids a redundant full rewrite for a
// stale or duplicate command.
func (s *Storage) handleScrub(e *cluster.Entity) error {
	sc := &cluster.Scrub{}
	if err := sc.Unmarshal(e.Data); err != nil {
		return err
	}
	if sc.UpperBound <= s.lastScrubbedBound.Load() {
		return nil
	}
	if err := s.setPendingScrubBound(sc.UpperBound); err != nil {
		return err
	}
	s.signalScrub()
	return nil
}

// setPendingScrubBound raises the persisted pending bound to max(existing, b).
// Monotonic so a later command never lowers an in-flight bound.
func (s *Storage) setPendingScrubBound(b uint64) error {
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
		return bkt.Put(pendingScrubBoundKey, buf[:])
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
	if err := s.runPendingScrub(); err != nil {
		s.logger.Error("resume pending scrub", zap.Error(err))
	}
	for {
		select {
		case <-s.scrubStop:
			return
		case <-s.scrubSignal:
			if err := s.runPendingScrub(); err != nil {
				// Leave the pending bound set; a later signal or a restart
				// retries. The rewrite is idempotent, so a retry is safe.
				s.logger.Error("run pending scrub", zap.Error(err))
			}
		}
	}
}

// runPendingScrub rewrites the event log for every pending bound not yet
// completed, looping so a bound raised during a rewrite is picked up
// immediately.
func (s *Storage) runPendingScrub() error {
	for {
		bound, err := s.loadPendingScrubBound()
		if err != nil {
			return err
		}
		if bound == 0 || bound <= s.lastScrubbedBound.Load() {
			return nil
		}
		if err := s.runScrub(bound); err != nil {
			return err
		}
		// markScrubComplete prunes the now-dead tombstones and compacts bbolt in one
		// kvMu.Lock critical section, so no snapshot can copy a freed-but-uncompacted
		// erased key.
		if err := s.markScrubComplete(bound); err != nil {
			return err
		}
	}
}

// runScrub rewrites the event log, removing entities tombstoned within the
// freeze line bound, and swaps the rewritten directory in. See the file header
// for the determinism and invariant guarantees.
func (s *Storage) runScrub(bound uint64) error {
	// RTBF (user-tombstone) selection: max delete index <= bound per (type, key).
	// Captured once; deletes recorded after this point have index > bound and are
	// irrelevant, so the selection is frozen and identical across replicas.
	sel, err := s.tombstoneSelections(bound)
	if err != nil {
		return err
	}

	// Metadata-GC (system-tombstone) selection: max raft index <= bound per
	// system-tombstonable (type, key). The rewrite keeps only that latest entry
	// per key and drops earlier ones. Derived from the log prefix <= bound, so —
	// like sel — it is a pure function of (log bytes, bound), identical on every
	// replica. The two selections range over disjoint type sets (user-defined vs
	// internal-snapshot), so they never collide despite sharing tombstoneKey.
	msel, err := s.metadataSupersessions(bound)
	if err != nil {
		return err
	}

	tmpDir := s.scrubTmpDir(bound)
	if err := os.RemoveAll(tmpDir); err != nil { // clear any stale partial attempt
		return err
	}
	newLog, err := wal.Open(tmpDir, nil)
	if err != nil {
		return err
	}
	swapped := false
	defer func() {
		// On any pre-swap failure, drop the half-built log so a retry starts
		// clean. After a successful swap newLog is already closed and renamed.
		if !swapped {
			_ = newLog.Close()
			_ = os.RemoveAll(tmpDir)
		}
	}()

	var nextSeq uint64
	writeSurvivor := func(payload []byte) error {
		nextSeq++
		return newLog.Write(nextSeq, frame(payload))
	}
	copyRange := func(lo, hi uint64, locked bool) error {
		for seq := lo; seq <= hi; seq++ {
			var raw []byte
			var rerr error
			if locked {
				raw, rerr = s.readEventAtLocked(seq)
			} else {
				raw, rerr = s.readEventAt(seq)
			}
			if rerr != nil {
				return rerr
			}
			keep, payload, ferr := scrubFilterEntry(raw, sel, msel)
			if ferr != nil {
				return ferr
			}
			if keep {
				if werr := writeSurvivor(payload); werr != nil {
					return werr
				}
			}
		}
		return nil
	}

	// Phase A (unlocked): bulk copy the log as it stood at the start. New
	// commits keep appending to the old log meanwhile (all at indices > bound,
	// so always survivors); they're caught up under the lock in phase B.
	first, err := s.firstEventSeq()
	if err != nil {
		return err
	}
	startLast, err := s.lastEventSeq()
	if err != nil {
		return err
	}
	if first != 0 && startLast != 0 {
		if err := copyRange(first, startLast, false); err != nil {
			return err
		}
	}

	// Phase B (locked): catch up the small delta appended during phase A, then
	// swap. eventMu.Lock waits for in-flight reads/the appender to drain and
	// blocks new ones for the brief swap.
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	endLast, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	if endLast > startLast {
		if err := copyRange(startLast+1, endLast, true); err != nil {
			return err
		}
	}
	if err := newLog.Close(); err != nil {
		return err
	}

	// Swap: events -> events.retired, events.scrub.<B> -> events. Renames are
	// atomic on POSIX; a crash between them is rolled back by recoverScrubDirs
	// on the next Open.
	retired := s.eventRetiredDir()
	if err := os.RemoveAll(retired); err != nil {
		return err
	}
	if err := s.eventLog.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.eventLogDir, retired); err != nil {
		// eventLog is already closed but this rename failed, so s.eventLogDir is
		// untouched (still the original log). Reopen it so the node survives; fatal
		// only if that also fails, so a closed handle never reaches appendEvent.
		s.reopenEventLogAfterSwapOrFatal("event-log scrub aborted before swap")
		return fmt.Errorf("move events aside for scrub swap: %w", err)
	}
	if err := os.Rename(tmpDir, s.eventLogDir); err != nil {
		// eventLog is closed and the original was already moved to `retired`, so
		// s.eventLogDir is now missing. Roll the first rename back to restore the
		// original, then reopen it (mirroring recoverScrubDirs). If the rollback
		// fails we cannot restore the log and must NOT reopen a missing dir — that
		// would create an empty log (silent event loss) — so fatal. swapped stays
		// false, so the defer drops tmpDir.
		if rbErr := os.Rename(retired, s.eventLogDir); rbErr != nil {
			s.logger.Fatal("event-log scrub swap failed and rollback failed; the node cannot continue (restart to recover via recoverScrubDirs)",
				zap.Error(err), zap.NamedError("rollback", rbErr))
		}
		s.reopenEventLogAfterSwapOrFatal("event-log scrub swap rolled back")
		return fmt.Errorf("rename scrubbed event log into place: %w", err)
	}
	swapped = true

	// The two renames above changed the events/ parent directory; fsync it so the
	// completed swap survives an immediate crash (an un-persisted rename could
	// resurrect the pre-scrub log, or leave events/ missing until recoverScrubDirs
	// runs). Best-effort — the swap is already committed and visible here.
	s.syncDirBestEffort(filepath.Dir(s.eventLogDir), "event-log scrub swap")

	// Post-swap reopen: s.eventLogDir now holds the scrubbed log. Reopen it or
	// fatal — a returned error here previously left s.eventLog closed for the next
	// appendEvent to hit as ErrClosed, a delayed crash mis-attributed to append.
	s.reopenEventLogAfterSwapOrFatal("reopen event log after scrub")
	// Recompute the in-memory bounds against the re-densified log, or fatal. The
	// swap has committed, so a failure here is NOT survivable: returning it into
	// the survive-and-continue scrub worker would leave stale eventIndex/
	// firstEventIndex and an un-bumped scrubGen (below) while the on-disk log is
	// the new one — in-flight Readers would never re-derive their walSeq cursor
	// and would silently read wrong offsets. A restart recomputes cleanly (Open
	// does the same). Same reason as reopenEventLogAfterSwapOrFatal.
	s.recomputeEventBoundsAfterSwapOrFatal("recompute event bounds after scrub")
	// Bump the generation so in-flight Readers re-derive their walSeq cursor
	// (the rewrite re-densified the seqs underneath them). Done under
	// eventMu.Lock, before releasing it, so no Reader can observe the new log
	// without also observing the new generation.
	s.scrubGen.Add(1)
	if err := os.RemoveAll(retired); err != nil {
		// The swap already succeeded; a leftover events.retired/ is harmless — the
		// next Open's recoverScrubDirs (and temp sweep) reaps it. Warn and continue
		// rather than returning a failure the survive-worker only logs anyway.
		s.logger.Warn("could not remove retired event-log dir after scrub swap; it will be reaped on the next restart",
			zap.String("dir", retired), zap.Error(err))
	}
	s.logger.Info("scrubbed permanent event log",
		zap.Uint64("bound", bound), zap.Int("tombstonedKeys", len(sel)))
	return nil
}

// reopenEventLogAfterSwapOrFatal reopens the permanent event log at
// s.eventLogDir and reassigns s.eventLog, or FATALS if the reopen fails — the
// event-log analogue of reopenKVAfterSwapOrFatal. A returned error would leave
// s.eventLog closed for the next appendEvent to hit as ErrClosed: a delayed
// crash mis-attributed to the append path, since runScrub's caller only logs and
// continues. The caller MUST ensure s.eventLogDir holds the intended (non-empty)
// log before calling — reopening a missing dir would create an empty log (silent
// event loss). Caller holds eventMu.Lock.
func (s *Storage) reopenEventLogAfterSwapOrFatal(what string) {
	reopened, err := wal.Open(s.eventLogDir, nil)
	if err != nil {
		s.logger.Fatal("event-log swap could not reopen storage; the node cannot continue (restart to recover via recoverScrubDirs)",
			zap.String("op", what), zap.Error(err))
	}
	s.eventLog = reopened
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
//   - Metadata GC (system tombstone): a system-tombstonable (internal-snapshot)
//     entity — upsert OR delete — at index I superseded by a later entry for the
//     same (type, key) at M <= bound (I < M). Only the latest per key survives;
//     a superseded internal delete is droppable too. (msel values are that M.)
//
// The two selections range over disjoint type sets, so an entity matches at most
// one reason.
func scrubFilterEntry(raw []byte, sel, msel map[string]uint64) (keep bool, payload []byte, err error) {
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
	newData, allRemoved, changed, err := cluster.FilterProposalEntities(pe.Data, remove)
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
// handled by RTBF, not here). Runs unlocked in scrub phase A, the same access
// pattern as the phase-A copy — entries appended concurrently are all at index
// > bound and excluded.
func (s *Storage) metadataSupersessions(bound uint64) (map[string]uint64, error) {
	sel := make(map[string]uint64)
	// User-type kinds, harvested in index order from type registrations as we
	// scan. typeType being Revision (retained) guarantees a type's registration
	// precedes its data here.
	userKind := make(map[string]cluster.EntityKind)
	first, err := s.firstEventSeq()
	if err != nil {
		return nil, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return nil, err
	}
	if first == 0 || last == 0 {
		return sel, nil
	}
	for seq := first; seq <= last; seq++ {
		raw, err := s.readEventAt(seq)
		if err != nil {
			return nil, err
		}
		pe := &pb.Entry{}
		if err := proto.Unmarshal(raw, pe); err != nil {
			return nil, err
		}
		// Event-log seqs are append order = raft-index order, so once an entry is
		// past the freeze line the rest are too — stop before reading the tail.
		if pe.GetIndex() > bound {
			break
		}
		if pe.GetType() != pb.EntryNormal || pe.Data == nil {
			continue
		}
		idx := pe.GetIndex()
		if err := cluster.ForEachProposalEntity(pe.Data, func(typeID string, key, data []byte, isDelete bool) error {
			// Learn each user type's declared kind from its registration.
			if cluster.IsType(typeID) && !isDelete {
				t := &cluster.Type{}
				if uerr := t.Unmarshal(data); uerr != nil {
					return uerr
				}
				userKind[t.ID] = t.EntityKind
				return nil
			}
			// Compactable iff Snapshot: an internal Snapshot built-in, or a user
			// type harvested as Snapshot. Everything else — Revision configs,
			// Standalone dead-letters, Event/Command/Unspecified streams — is
			// retained.
			if !cluster.IsSystemTombstonable(typeID) && userKind[typeID] != cluster.EntityKindSnapshot {
				return nil
			}
			if tk := string(tombstoneKey(typeID, key)); idx > sel[tk] {
				sel[tk] = idx
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return sel, nil
}

// recomputeEventBoundsLocked refreshes firstEventIndex/eventIndex from the
// rewritten log. Caller must hold eventMu.Lock. EventIndex (P_local) must never
// regress — the tail is never removed — so this asserts the recomputed value
// equals the existing one and refuses to lower it (a lower value would trip the
// Ready loop's P==R invariant check and fatal-exit the node).
func (s *Storage) recomputeEventBoundsLocked() error {
	last, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	if last == 0 {
		return fmt.Errorf("scrub emptied the event log: the tail must always survive")
	}
	lb, err := s.readEventAtLocked(last)
	if err != nil {
		return err
	}
	le := &pb.Entry{}
	if err := proto.Unmarshal(lb, le); err != nil {
		return err
	}
	if le.GetIndex() != s.eventIndex.Load() {
		return fmt.Errorf("scrub changed EventIndex from %d to %d; the tail must be preserved",
			s.eventIndex.Load(), le.GetIndex())
	}

	first, err := s.firstEventSeqLocked()
	if err != nil {
		return err
	}
	fb, err := s.readEventAtLocked(first)
	if err != nil {
		return err
	}
	fe := &pb.Entry{}
	if err := proto.Unmarshal(fb, fe); err != nil {
		return err
	}
	s.firstEventIndex.Store(fe.GetIndex())
	return nil
}

// loadPendingScrubBound reads the highest requested scrub bound (0 if none).
func (s *Storage) loadPendingScrubBound() (uint64, error) {
	return s.loadScrubUint(pendingScrubBoundKey)
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
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bound)
		return bkt.Put(scrubCompletedKey, buf[:])
	})
	if err != nil {
		return err
	}
	// Still under kvMu.Lock: compact away the freed tombstone pages before any
	// reader (CreateSnapshot) can observe them. Only when something was pruned —
	// compaction is an O(bbolt) rewrite.
	if pruned {
		if err := s.compactLocked(); err != nil {
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
	return nil
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

// HasScrubBacklog reports whether the next scrub has anything to physically
// remove — either RTBF erasure (a delete-tombstone beyond the highest completed
// bound) or enough accumulated superseded metadata to be worth an O(N) rewrite.
// It is the single "is there scrubbable work?" signal the automatic scheduler
// (db.scrubScheduler) consults, deliberately covering both jobs the one rewrite
// pass does, so an idle cadence tick skips the rewrite. A metadata-heavy,
// RTBF-free cluster triggers via the metadata term (see metadata-gc-scrubber).
func (s *Storage) HasScrubBacklog() bool {
	return s.hasRTBFBacklog() || s.metadataBacklog.Load() >= metadataBacklogThreshold
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
// atomic same-filesystem operation.
func (s *Storage) scrubTmpDir(bound uint64) string {
	return fmt.Sprintf("%s.scrub.%d", s.eventLogDir, bound)
}

func (s *Storage) eventRetiredDir() string {
	return s.eventLogDir + ".retired"
}

// recoverScrubDirs repairs an interrupted scrub before the event log is opened.
// dir is the storage root (events/ lives at dir/events).
//
//   - If events/ is missing but events.retired/ exists, a swap crashed after
//     renaming events out: roll back to the retired (pre-swap) state. The
//     pending bound re-drives an idempotent rewrite once the worker starts.
//   - Remove any leftover events.retired/ (a swap that completed but crashed
//     before cleanup) and any events.scrub.*/ temp dirs (a rewrite that crashed
//     before swapping).
func recoverScrubDirs(dir string) error {
	eventsDir := filepath.Join(dir, "events")
	retiredDir := eventsDir + ".retired"

	if !dirExists(eventsDir) && dirExists(retiredDir) {
		if err := os.Rename(retiredDir, eventsDir); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(retiredDir); err != nil {
		return err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh data dir; nothing to recover
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "events.scrub.") {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
