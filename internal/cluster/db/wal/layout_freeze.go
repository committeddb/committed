package wal

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// A layout freeze holds a log's set of segment files still for a reader of
// that set — a peer fetching the event log to catch up, a live backup —
// while the log's MOVERS, the maintenance that changes the set, step aside.
// Each log has its own freeze, because each has its own movers and a reader
// of one log must not hold the other's maintenance: a live backup streams
// the event log for as long as it takes (terabytes) and must not leave the
// raft log uncompacted meanwhile.
//
//   - The event log's movers: the sealer (compressing a sealed segment
//     replaces the plain file with its .zst twin), the scrub swap (replaces
//     the whole directory), a catch-up's adoption and reset (add and remove
//     files).
//   - The raft entry log's movers: raft-log compaction (deletes whole
//     segment files). A follower's log-conflict truncation and a snapshot
//     install also change its files, but never wait on anything: a live
//     backup detects them and starts over instead.
//
// A freeze only delays maintenance; nothing it delays changes content, and
// the holder is expected to bound it.

// layoutLock is one log's exclusion between two classes that are each
// concurrent within themselves: movers share it, one RLock per step
// (TryRLock, never blocking — a mover skips, it does not wait, and movers
// keep running beside each other as they always have), and the freezes as
// a group hold it exclusively — the first freeze takes it, waiting out any
// step in flight so what it then lists stays on disk, and the last release
// gives it back. The counter is the group's membership; the lock is what
// keeps a listed file on disk.
type layoutLock struct {
	name   string
	logger *zap.Logger

	mu       sync.Mutex // guards freezes and frozenAt
	freezes  int
	frozenAt time.Time
	lock     sync.RWMutex
}

// freeze registers a reader of the log's layout and returns its release
// (idempotent). It waits out a mover step in flight; movers step aside from
// the moment it is pending.
func (l *layoutLock) freeze() func() {
	l.mu.Lock()
	if l.freezes == 0 {
		// The group's exclusive hold. Bounded by one mover step, and nothing
		// a mover does under its hold takes l.mu, so holding it here cannot
		// deadlock.
		l.lock.Lock()
		l.frozenAt = time.Now()
		l.logger.Info("log layout frozen for a reader; its maintenance waits", zap.String("log", l.name))
	}
	l.freezes++
	l.mu.Unlock()
	released := false
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if released {
			return
		}
		released = true
		l.freezes--
		if l.freezes == 0 {
			l.logger.Info("log layout released", zap.String("log", l.name), zap.Duration("frozenFor", time.Since(l.frozenAt)))
			l.lock.Unlock()
		}
	}
}

// move claims the layout for one mover step and returns its release, or
// ok == false when a freeze stands or is pending. It never blocks: the
// mover skips or defers and tries again on its next cycle, so a freeze can
// never stall the raft Ready loop or the sealer.
func (l *layoutLock) move() (release func(), ok bool) {
	if !l.lock.TryRLock() {
		return nil, false
	}
	return l.lock.RUnlock, true
}

// frozen reports whether a freeze stands.
func (l *layoutLock) frozen() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.freezes > 0
}

// FreezeEventLayout holds the event log's set of segment files still: the
// sealer skips compression, the scrub swap waits, and a catch-up's adoption
// or reset is refused, until the returned release (idempotent) runs.
func (s *Storage) FreezeEventLayout() func() { return s.eventLayout.freeze() }

// FreezeRaftLayout holds the raft entry log's set of segment files still:
// raft-log compaction returns cluster.ErrCompactionDeferred until the
// returned release (idempotent) runs. Disk-pressure compaction defers too —
// the holder is expected to be brief (the raft log is bounded; a live
// backup copies it last and lets go).
func (s *Storage) FreezeRaftLayout() func() { return s.raftLayout.freeze() }
