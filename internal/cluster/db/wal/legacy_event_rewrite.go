package wal

import (
	"errors"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// rewriteLegacyEvents executes a prepared scrub using native dense sequences.
// It retains the bulk/catch-up phases, protected-reader wait, directory swap,
// durability handling, and fatal recovery policy of production tidwall storage.
// Selection and delete-key authorization have already completed in the plan.
func (s *Storage) rewriteLegacyEvents(plan *scrubPlan) error {
	bound := plan.bound
	tmpDir := s.scrubTmpDir(bound)
	if err := os.RemoveAll(tmpDir); err != nil { // clear any stale partial attempt
		return err
	}
	// NoSync: this is a throwaway temp log that becomes authoritative only at the
	// atomic rename below, and any pre-swap crash discards it (recoverScrubDirs /
	// the defer drop tmpDir), leaving the pre-scrub log untouched — so a per-entry
	// fsync here buys no crash-safety, it only serializes the whole O(N) rewrite
	// behind one fsync per surviving entry. On a large log that starves the raft
	// propose path through the shared storage lock for hours. We fsync explicitly
	// with newLog.Sync() before the rename instead; segment cycling still fsyncs
	// each filled segment (cycle()), so only the tail is left for that final Sync.
	// The rewritten log compresses like the live one; the drain below (before
	// the swap lock) compresses everything this rewrite sealed, so the swap
	// installs an already-compressed log instead of leaving the whole rewrite
	// as backlog for the sealer.
	newLog, err := tidwallbackend.CreateLegacyRewrite(tmpDir)
	if err != nil {
		return err
	}
	swapped := false
	retired := s.eventRetiredDir()
	defer func() {
		if !swapped {
			// On any pre-swap failure, drop the half-built log so a retry starts
			// clean. After a successful swap newLog is already closed and renamed.
			_ = newLog.Close()
			_ = os.RemoveAll(tmpDir)
			return
		}
		// Swap succeeded: reap the retired pre-scrub log — potentially thousands of
		// segment files. This defer is registered BEFORE the eventMu.Lock defer, so
		// LIFO runs it AFTER eventMu is released: doing the O(files) os.RemoveAll
		// under eventMu.Lock would stall the Ready loop's appendEvents for the whole
		// unlink duration, which scales with log size. A leftover events.retired/ is
		// harmless — recoverScrubDirs reaps it on the next Open — so warn, don't fail.
		if err := os.RemoveAll(retired); err != nil {
			s.logger.Warn("could not remove retired event-log dir after scrub swap; it will be reaped on the next restart",
				zap.String("dir", retired), zap.Error(err))
		}
	}()

	transform := func(raw []byte) ([]byte, bool, error) {
		keep, payload, err := plan.transform(raw)
		if err != nil || !keep {
			return nil, false, err
		}
		return frame(payload), true, nil
	}
	copyRange := func(lo, hi uint64, locked bool) error {
		read := s.readEventAt
		if locked {
			read = s.readEventAtLocked
		}
		return newLog.CopyRange(lo, hi, read, transform)
	}

	// Phase A (unlocked): bulk-copy the log as it stood at the start. New commits
	// keep appending to the OLD log meanwhile (all at indices > bound, so always
	// survivors); they are chased down in phase A' and caught up under the lock in
	// phase B.
	first, err := s.firstEventSeq()
	if err != nil {
		return err
	}
	startLast, err := s.lastEventSeq()
	if err != nil {
		return err
	}
	// Sequence numbers and a tombstoned-key count only — no source keys/PII.
	s.logger.Info("scrub: rewriting permanent event log",
		zap.Uint64("bound", bound),
		zap.Uint64("firstSeq", first),
		zap.Uint64("lastSeq", startLast),
		zap.Int("tombstonedKeys", len(plan.selections)))
	if first != 0 && startLast != 0 {
		if err := copyRange(first, startLast, false); err != nil {
			return err
		}
	}

	if s.scrubPostBulkHookForTest != nil {
		s.scrubPostBulkHookForTest()
	}

	// Phase A' (unlocked convergence): on a heavy-write system entries keep landing
	// on the old log while phase A runs. Copying that whole delta under eventMu.Lock
	// would stall the appender in proportion to the write rate, so chase it with
	// repeated UNLOCKED passes first — a NoSync copy outruns fsync'd appends, so each
	// pass leaves less than the last and the residue for the locked phase shrinks to
	// a sliver. Bounded by scrubConvergeRounds so a writer we cannot outrun still
	// terminates (phase B then just copies a larger delta); the reads are of
	// already-durable seqs on the live log, safe without the lock.
	copied := startLast
	for range scrubConvergeRounds {
		cur, lerr := s.lastEventSeq()
		if lerr != nil {
			return lerr
		}
		if cur <= copied || cur-copied <= scrubConvergeResidue {
			break // caught up, or small enough to finish under the lock
		}
		if err := copyRange(copied+1, cur, false); err != nil {
			return err
		}
		copied = cur
	}

	// Durability: fsync the survivors written so far BEFORE taking the lock. With
	// NoSync only the current tail segment is unpersisted (cycle() fsynced the rest),
	// so this flushes at most ~SegmentSize and does the bulk of the fsync work
	// OUTSIDE the lock. The tiny locked delta is synced again below.
	if err := newLog.Sync(); err != nil {
		return err
	}

	// Phase B (locked): catch up the now-small delta, then swap. eventMu.Lock waits
	// for in-flight reads/the appender to drain and blocks new ones for the brief
	// Compress the rewrite's sealed segments BEFORE taking the swap lock —
	// this is O(surviving log) work that must not stall appendEvents. The
	// locked delta below may seal a few more segments; those trickle through
	// the background sealer after the swap.
	if err := newLog.CompressSealed(); err != nil {
		return err
	}

	// Before the swap: wait out any in-flight from-0 log reads, so no such
	// read spans the swap and observes two different rewrite states — the
	// pair-consistency invariant the delete-key erasure gate rests on (see
	// BeginFromZeroRead). Placed before the lock so pinned readers (and the
	// appender) keep running while we wait; shutdown aborts the wait and the
	// pending bound retries later.
	releaseLayout, werr := s.waitLayoutQuiet()
	if werr != nil {
		return werr
	}
	defer releaseLayout()

	// swap.
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	endLast, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	if endLast > copied {
		if err := copyRange(copied+1, endLast, true); err != nil {
			return err
		}
	}
	// Sync the locked delta (bounded by the residue + current tail segment) so the
	// whole rewritten log is durable before the rename makes it authoritative.
	if err := newLog.Sync(); err != nil {
		return err
	}
	if err := newLog.Close(); err != nil {
		return err
	}
	// Durability: fsync the freshly-written swap dir so its segment entries survive
	// power loss BEFORE it is renamed into place. The parent-dir fsync after the
	// swap (below) makes the *rename* durable, but not the segment filenames inside
	// the swapped-in dir. Best-effort, like the parent fsync; the newLog.Sync calls
	// above already committed the entries' content (NoSync moved that fsync out of
	// the per-entry path), so this only needs to persist their directory entries.
	s.syncDirBestEffort(tmpDir, "event-log scrub swap dir")

	// Swap: events -> events.retired, events.scrub.<B> -> events. Renames are
	// atomic on POSIX; a crash between them is rolled back by recoverScrubDirs
	// on the next Open. Clear any stale events.retired/ first (usually absent —
	// recoverScrubDirs reaps it on Open — so this is a fast no-op).
	if err := os.RemoveAll(retired); err != nil {
		return err
	}
	// Close the LIVE event-log handle ahead of the rename, or fatal — a returned
	// error would leave the dead handle for appendEvent to hit later as a
	// mis-attributed ErrClosed crash. (The newLog.Close above is the temp log, not
	// the live handle, so it correctly returns instead of fataling.)
	s.closeEventLogBeforeSwapOrFatal("close event log before scrub swap")
	if err := tidwallbackend.SwapLegacyDirectories(s.eventLogDir, tmpDir, retired); err != nil {
		var swapErr *tidwallbackend.LegacySwapError
		if errors.As(err, &swapErr) && swapErr.Rollback != nil {
			// The original live directory could not be restored. Reopening the
			// missing path would silently create an empty log.
			s.logger.Fatal("event-log scrub swap failed and rollback failed; the node cannot continue (restart to recover via recoverScrubDirs)",
				zap.Error(swapErr.Cause), zap.NamedError("rollback", swapErr.Rollback))
		}
		s.reopenEventLogAfterSwapOrFatal("event-log scrub swap aborted")
		return err
	}
	swapped = true
	// The bytes on disk are now the rewrite's: the log's generation moves with
	// the swap, not with the completion mark (see EventLogGeneration).
	s.swappedBound.Store(bound)

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
	// The retired pre-scrub log is reaped by the deferred cleanup AFTER eventMu is
	// released (see the swapped branch of the defer above), NOT here under the
	// lock — so the O(files) removal can't stall the apply path.
	s.logger.Info("scrubbed permanent event log",
		zap.Uint64("bound", bound),
		zap.Int("tombstonedKeys", len(plan.selections)),
		zap.Uint64("survivorEntries", newLog.Count()))
	return nil
}
