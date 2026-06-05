package db

import (
	"context"
	"time"
)

// This file holds the DB-level linearizable-read path. The raft-protocol
// half (the ReadIndex round-trip and the read-state waiter registry) lives
// in raft.go; here we layer the second half on top: once raft confirms the
// index at which a read is safe, block until this node's AppliedIndex has
// caught up to it, so the caller reads state that reflects every write
// committed before the read began.

// LinearizableRead returns nil only after two conditions hold:
//
//  1. The raft leader has confirmed, via the ReadIndex protocol (a quorum
//     heartbeat round-trip), the log index at which a linearizable read may
//     be served. A node that cannot reach a leader — a partitioned-away
//     follower, or a deposed leader that has lost quorum under CheckQuorum —
//     never gets this confirmation and blocks until ctx fires.
//  2. This node's AppliedIndex has reached that confirmed index, so the
//     local state machine reflects every entry committed before the read.
//
// Callers gate a read on this (then perform the actual local read) to get
// linearizable semantics: the read observes all writes that completed before
// it started, and never serves stale data from a node that has silently
// fallen out of the quorum. The cost is one heartbeat round-trip, coalesced
// by etcd-raft across concurrent reads.
//
// On ctx cancellation (including the per-request deadline an HTTP handler
// attaches) LinearizableRead returns ctx.Err(); on DB shutdown it returns
// db.ctx.Err() or ErrClosed. It never returns having confirmed only one of
// the two conditions.
func (db *DB) LinearizableRead(ctx context.Context) error {
	start := time.Now()

	readIndex, err := db.raft.ReadIndex(ctx)
	if err != nil {
		return err
	}

	if err := db.waitForApplied(ctx, readIndex); err != nil {
		return err
	}

	if db.metrics != nil {
		db.metrics.ReadIndexCompleted(time.Since(start))
	}
	return nil
}

// waitForApplied blocks until db.storage.AppliedIndex() >= target, ctx is
// canceled, or the DB is shutting down. It waits on the applied-index
// broadcast (appliedNotifyCh) that the raft Ready loop fires after each
// apply, re-checking the index on every wake rather than polling.
//
// The fast path is common: on the leader serving a read, AppliedIndex is
// usually already at or past the confirmed ReadIndex, so the loop returns on
// its first check without ever blocking.
func (db *DB) waitForApplied(ctx context.Context, target uint64) error {
	for {
		if db.storage.AppliedIndex() >= target {
			return nil
		}
		// Snapshot the current-generation channel BEFORE re-checking the
		// index below would race: we grab the channel, then the loop top
		// re-reads AppliedIndex. If an apply lands between the two, the
		// channel we hold is already closed, so the select returns
		// immediately and the re-check sees the advanced index. This is
		// the standard close-to-broadcast pattern and is race-free without
		// holding appliedMu across the wait.
		ch := db.appliedNotify()
		if db.storage.AppliedIndex() >= target {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-db.ctx.Done():
			return db.ctx.Err()
		}
	}
}

// appliedNotify returns the current-generation broadcast channel. A waiter
// reads it, re-checks its condition, then selects on it; notifyAppliedIndexAdvanced
// closes it to wake every current waiter and installs a fresh one.
func (db *DB) appliedNotify() <-chan struct{} {
	db.appliedMu.Lock()
	defer db.appliedMu.Unlock()
	return db.appliedNotifyCh
}

// notifyAppliedIndexAdvanced is invoked from the raft Ready loop once per
// iteration in which AppliedIndex advanced. It closes the current broadcast
// channel (waking every waitForApplied caller) and installs a fresh one for
// the next generation. Cheap and lock-bounded; fired at most once per Ready.
func (db *DB) notifyAppliedIndexAdvanced() {
	db.appliedMu.Lock()
	close(db.appliedNotifyCh)
	db.appliedNotifyCh = make(chan struct{})
	db.appliedMu.Unlock()
}
