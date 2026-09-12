package wal

import (
	"time"

	"go.uber.org/zap"
)

// FreezeLayout registers an in-flight reader of this node's on-disk log
// layout — a peer fetching the event log to catch up, or a live backup — and
// returns its release (idempotent). While any freeze stands, no mover changes
// the set of segment files: the sealer skips compression (which would
// replace a sealed plain segment with its .zst twin and remove the plain
// file), raft-log compaction returns cluster.ErrCompactionDeferred (it would
// delete whole raft segment files), and the scrub swap waits (it would
// replace the whole events dir). A freeze taken while a mover's step is in
// flight waits for that step to finish, so the layout the freeze holder then
// lists is the layout that stays on disk; freezes coexist, and movers step
// aside from the moment the first one is pending. A freeze only delays
// maintenance — every kind of raft-log compaction included, disk-pressure
// too — nothing it delays changes content, and the holder is expected to
// bound it.
func (s *Storage) FreezeLayout() func() {
	s.layoutMu.Lock()
	if s.layoutFreezes == 0 {
		// The group's exclusive hold: waits out any mover step in flight.
		// Bounded by one step (one segment's compression, one raft-log
		// truncation, one swap's locked phase), and nothing a mover does
		// under its hold takes layoutMu, so holding it here cannot deadlock.
		s.layoutLock.Lock()
		s.layoutFrozenAt = time.Now()
		s.logger.Info("log layout frozen: compression, raft-log compaction, and scrub swaps wait for the reader")
	}
	s.layoutFreezes++
	s.layoutMu.Unlock()
	released := false
	return func() {
		s.layoutMu.Lock()
		defer s.layoutMu.Unlock()
		if released {
			return
		}
		released = true
		s.layoutFreezes--
		if s.layoutFreezes == 0 {
			s.logger.Info("log layout released", zap.Duration("frozenFor", time.Since(s.layoutFrozenAt)))
			s.layoutLock.Unlock()
		}
	}
}

// moveLayout claims the layout for one mover step — a segment compression,
// a raft-log compaction, the scrub swap — and returns its release, or ok ==
// false when a freeze stands or is pending. Movers hold it shared, so they
// never wait on each other, and it never blocks: the mover skips or defers
// and tries again on its next cycle, so a freeze can never stall the raft
// Ready loop or the sealer.
func (s *Storage) moveLayout() (release func(), ok bool) {
	if !s.layoutLock.TryRLock() {
		return nil, false
	}
	return s.layoutLock.RUnlock, true
}

// layoutFrozen reports whether a layout freeze stands.
func (s *Storage) layoutFrozen() bool {
	s.layoutMu.Lock()
	defer s.layoutMu.Unlock()
	return s.layoutFreezes > 0
}
