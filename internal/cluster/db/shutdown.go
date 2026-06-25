package db

import (
	"time"

	"go.uber.org/zap"
)

const (
	// defaultShutdownTransferTimeout bounds how long Close waits for a graceful
	// leadership hand-off to complete before stopping anyway. A transfer to a
	// caught-up voter normally settles in well under a second (one catch-up
	// round plus an immediate election); 3s is generous headroom on a loaded
	// node, and stays comfortably inside cmd's 30s graceful-shutdown deadline.
	defaultShutdownTransferTimeout = 3 * time.Second

	// shutdownTransferPollInterval is how often the wait loop checks whether
	// leadership has moved off this node.
	shutdownTransferPollInterval = 10 * time.Millisecond
)

// transferLeadershipBeforeStop hands the leader role to a caught-up voter before
// this node stops, so a graceful restart (e.g. a rolling upgrade) doesn't force
// the cluster through a full election — by the time raft stops, leadership has
// already settled on a live node. It is best-effort and bounded: a non-leader, a
// single-node cluster, or a hand-off that doesn't complete within
// shutdownTransferTimeout all fall through to the normal stop. A forced election
// is the fallback, never a hang.
func (db *DB) transferLeadershipBeforeStop() {
	target := db.shutdownTransferTargetFn()
	if target == 0 {
		// Not the leader, or no voter to hand off to — nothing to do.
		return
	}

	db.logger.Info("graceful shutdown: transferring leadership before stop",
		zap.Uint64("target", target))
	db.transferLeadershipFn(target)

	deadline := time.Now().Add(db.shutdownTransferTimeout)
	for time.Now().Before(deadline) {
		if !db.isLeaderFn() {
			db.logger.Info("graceful shutdown: leadership handed off",
				zap.Uint64("target", target))
			return
		}
		time.Sleep(shutdownTransferPollInterval)
	}
	db.logger.Warn("graceful shutdown: leadership did not hand off before timeout; stopping anyway",
		zap.Duration("timeout", db.shutdownTransferTimeout), zap.Uint64("target", target))
}

// shutdownTransferTarget is the default shutdownTransferTargetFn: it returns the
// voter to hand leadership to when this node is the leader and shutting down, or
// 0 when this node is not the leader or has no voter successor.
func (db *DB) shutdownTransferTarget() uint64 {
	_, _, _, isLeader, members := db.raft.membershipView()
	if !isLeader {
		return 0
	}
	return pickShutdownTransferTarget(db.ID(), members)
}

// isLeader is the default isLeaderFn: whether raft currently regards this node
// as the leader.
func (db *DB) isLeader() bool {
	_, _, _, isLeader, _ := db.raft.membershipView()
	return isLeader
}

// pickShutdownTransferTarget chooses the voter to hand leadership to on a
// graceful stop: the most caught-up voter other than this node (highest match
// index, so raft's pre-transfer catch-up is shortest and the hand-off completes
// fast). Learners are skipped — only a voter can take leadership. Unlike the
// disk-pressure picker it imposes no disk-health constraint: any reachable,
// caught-up voter is a fine successor for a clean shutdown. Returns 0 when this
// node is the only voter (a single-node cluster has nowhere to hand off).
func pickShutdownTransferTarget(selfID uint64, members []memberView) uint64 {
	var target, best uint64
	for _, m := range members {
		if m.id == selfID || m.learner {
			continue
		}
		if target == 0 || m.match > best {
			target = m.id
			best = m.match
		}
	}
	return target
}
