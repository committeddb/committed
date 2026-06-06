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

	"github.com/philborlin/committed/internal/cluster"
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
		if err := s.markScrubComplete(bound); err != nil {
			return err
		}
	}
}

// runScrub rewrites the event log, removing entities tombstoned within the
// freeze line bound, and swaps the rewritten directory in. See the file header
// for the determinism and invariant guarantees.
func (s *Storage) runScrub(bound uint64) error {
	// Survivor selection: max delete index <= bound per (type, key). Captured
	// once; deletes recorded after this point have index > bound and are
	// irrelevant, so the selection is frozen and identical across replicas.
	sel, err := s.tombstoneSelections(bound)
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
			keep, payload, ferr := scrubFilterEntry(raw, sel)
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
		return err
	}
	if err := os.Rename(tmpDir, s.eventLogDir); err != nil {
		return err
	}
	swapped = true

	reopened, err := wal.Open(s.eventLogDir, nil)
	if err != nil {
		return fmt.Errorf("reopen event log after scrub: %w", err)
	}
	s.eventLog = reopened
	if err := s.recomputeEventBoundsLocked(); err != nil {
		return err
	}
	// Bump the generation so in-flight Readers re-derive their walSeq cursor
	// (the rewrite re-densified the seqs underneath them). Done under
	// eventMu.Lock, before releasing it, so no Reader can observe the new log
	// without also observing the new generation.
	s.scrubGen.Add(1)
	if err := os.RemoveAll(retired); err != nil {
		return err
	}
	s.logger.Info("scrubbed permanent event log",
		zap.Uint64("bound", bound), zap.Int("tombstonedKeys", len(sel)))
	return nil
}

// scrubFilterEntry decides the fate of one event-log record (raw = unframed
// pb.Entry bytes). It returns whether to keep the record and, if kept, the
// payload to write (the original bytes verbatim, or a re-marshaled entry with
// tombstoned entities removed). Entries without a proposal (conf changes, the
// leader no-op) are kept verbatim. The remove predicate fires for an entity at
// raft index I whose (type, key) has a delete at D with I < D <= bound — i.e.
// the entity was written before a delete within the freeze line — which is also
// false for any entry at index > bound, so the tail is always kept. (The bound
// is baked into sel — each value is the max delete index <= bound — so it is not
// a separate parameter here.)
func scrubFilterEntry(raw []byte, sel map[string]uint64) (keep bool, payload []byte, err error) {
	pe := &pb.Entry{}
	if err := pe.Unmarshal(raw); err != nil {
		return false, nil, err
	}
	if pe.Type != pb.EntryNormal || pe.Data == nil {
		return true, raw, nil
	}
	idx := pe.Index
	remove := func(typeID string, key []byte) bool {
		d := sel[string(tombstoneKey(typeID, key))]
		return d != 0 && idx < d
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
	nb, err := pe.Marshal()
	if err != nil {
		return false, nil, err
	}
	return true, nb, nil
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
	if err := le.Unmarshal(lb); err != nil {
		return err
	}
	if le.Index != s.eventIndex.Load() {
		return fmt.Errorf("scrub changed EventIndex from %d to %d; the tail must be preserved",
			s.eventIndex.Load(), le.Index)
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
	if err := fe.Unmarshal(fb); err != nil {
		return err
	}
	s.firstEventIndex.Store(fe.Index)
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
// successful swap.
func (s *Storage) markScrubComplete(bound uint64) error {
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		if cur := bkt.Get(scrubCompletedKey); len(cur) == 8 && binary.BigEndian.Uint64(cur) >= bound {
			return nil
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bound)
		return bkt.Put(scrubCompletedKey, buf[:])
	})
	if err != nil {
		return err
	}
	// Advance the atomic only after the durable write succeeds.
	for {
		cur := s.lastScrubbedBound.Load()
		if bound <= cur || s.lastScrubbedBound.CompareAndSwap(cur, bound) {
			return nil
		}
	}
}

// HasScrubBacklog reports whether any tombstone records a delete at an index
// beyond the highest completed scrub bound — i.e. there is RTBF erasure the next
// scrub would physically remove. The automatic scheduler (db.scrubScheduler)
// consults it so a cadence tick with no new deletions skips the O(N) rewrite.
// Scans the tombstone bucket; cheap relative to the scrub cadence and the set is
// the RTBF working set, not the whole log.
func (s *Storage) HasScrubBacklog() bool {
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
