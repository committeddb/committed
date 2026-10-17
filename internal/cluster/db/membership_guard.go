package db

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The apply-time membership guard. The membership API refuses an invalid
// change when it is proposed, but that check reads one node's configuration
// before the change reaches the log, so two racing requests, or a request
// against a stale follower, can still commit one. raft applies what commits:
// removing the last voter panics every node, now and on every restart, and an
// add-learner for a voter silently demotes it. So before raft applies a
// change, every node checks it against the configuration it applies to, and
// an invalid change becomes a logged no-op by zeroing its node id (etcd's own
// convention; raft skips a zero id). The proposer is told why instead of
// waiting for a change that never happens.
//
// Invalid, change by change within an entry:
//   - an add-learner naming a voter (raft would demote it);
//   - a remove that would leave no voters (raft would panic);
//   - an add or add-learner naming an id removed since the guard switched on
//     (a removed node's data may hold a log the cluster has since moved past;
//     it rejoins under a new id).
//
// Every node must reach the same verdict for the same entry, or their
// configurations diverge. So the guard reads only conf-change-pass state —
// the configuration, and the removals and switch-on point that pass records
// with their log index — and switches on at a log index: the marker the leader
// writes as an UpdateNode once every member runs a release that has the guard
// (featureLevelMembershipGuard). Entries before the marker apply as before, on
// every node. A member running an older release added after the marker
// applies without the guard; membership changes need every member on a
// release that has it.

// featureLevelMembershipGuard: every member must run a release with the guard
// before the leader switches it on. The same release as the cluster id.
const featureLevelMembershipGuard = featureLevelClusterIdentity

// membershipGuardMarker is the Context of the UpdateNode that switches the
// guard on. It is not a URL, so no release records it as an address.
const membershipGuardMarker = "committed:membership-guard"

// confChangeRejection is the guard's verdict on one change, kept for the
// waiter that proposed it.
type confChangeRejection struct {
	seq    uint64
	nodeID uint64
	err    error
}

// membershipGuard is the Raft's guard state outside storage.
type membershipGuard struct {
	// rejections counts verdicts; last is the latest.
	rejections atomic.Uint64
	last       atomic.Pointer[confChangeRejection]
	// removed caches storage's removed ids for the transport's per-message
	// check (IsIDRemoved).
	removed atomic.Pointer[map[uint64]uint64]
}

// refreshRemoved reloads the removed-id cache from storage.
func (n *Raft) refreshRemoved() {
	m := n.storage.RemovedMembers()
	n.guard.removed.Store(&m)
}

// isRemoved reports whether id was removed from the configuration and is not
// a member now (an id re-added before the guard existed is a member again).
func (n *Raft) isRemoved(id uint64) bool {
	m := n.guard.removed.Load()
	if m == nil {
		return false
	}
	if _, ok := (*m)[id]; !ok {
		return false
	}
	return !n.isMember(id)
}

// rejectionsSoFar is the verdict count a waiter starts from.
func (n *Raft) rejectionsSoFar() uint64 { return n.guard.rejections.Load() }

// rejectedSince returns the error for a change naming id that the guard
// rejected after the count was seq.
func (n *Raft) rejectedSince(seq, id uint64) error {
	r := n.guard.last.Load()
	if r == nil || r.seq <= seq || r.nodeID != id {
		return nil
	}
	return r.err
}

// guardConfChange zeroes the node id of each invalid change in cc, which
// commits at index, and returns cc. It reads raft's current configuration —
// the one cc applies to. cc is the entry's own decoded copy.
func (n *Raft) guardConfChange(cc *raftpb.ConfChangeV2, index uint64) *raftpb.ConfChangeV2 {
	if len(cc.Changes) == 0 {
		return cc // leaving a joint configuration
	}
	since, on := n.storage.MembershipGuardIndex()
	if !on || index <= since {
		return cc
	}
	// A change applies only to a configuration that isn't joint (raft's leader
	// refuses one proposed during a joint configuration), so the voters are
	// the incoming set alone.
	voters := maps.Clone(map[uint64]struct{}(n.node.Status().Config.Voters[0]))
	if voters == nil {
		voters = map[uint64]struct{}{}
	}
	removed := n.storage.RemovedMembers()

	for _, ch := range cc.Changes {
		id := ch.GetNodeId()
		if id == 0 {
			continue
		}
		var err error
		_, voter := voters[id]
		switch ch.GetType() {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if at, ok := removed[id]; ok && at > since {
				err = fmt.Errorf("%w: node %d was removed from this cluster; a removed node rejoins under a new id", cluster.ErrInvalidMember, id)
			} else if ch.GetType() == raftpb.ConfChangeAddLearnerNode && voter {
				err = fmt.Errorf("%w: node %d is a voter; adding it as a learner would demote it", cluster.ErrInvalidMember, id)
			}
		case raftpb.ConfChangeRemoveNode:
			if voter && len(voters) == 1 {
				err = fmt.Errorf("%w: node %d is the only voter", cluster.ErrWouldRemoveLastVoter, id)
			}
		}
		if err != nil {
			n.logger.Warn("membership change not applied: invalid for the configuration it reached",
				zap.Uint64("index", index), zap.Uint64("node", id), zap.String("change", ch.GetType().String()), zap.Error(err))
			seq := n.guard.rejections.Add(1)
			n.guard.last.Store(&confChangeRejection{seq: seq, nodeID: id, err: err})
			zero := uint64(0)
			ch.NodeId = &zero
			continue
		}
		switch ch.GetType() {
		case raftpb.ConfChangeAddNode:
			voters[id] = struct{}{}
		case raftpb.ConfChangeRemoveNode:
			delete(voters, id)
		}
	}
	return cc
}

// proposeMembershipGuard switches the guard on: the leader proposes the
// marker once every member runs a release that has the guard. Called from
// the leader's agreed-address loop; raft drops it while another membership
// change is pending, and the next round retries.
func (db *DB) proposeMembershipGuard(ctx context.Context) {
	if _, on := db.storage.MembershipGuardIndex(); on || !db.featureEnabled(featureLevelMembershipGuard) {
		return
	}
	if _, _, joint := db.raft.memberStatus(); joint {
		return
	}
	if err := db.proposeUpdateNode(ctx, db.raft.id, membershipGuardMarker); err != nil && db.ctx.Err() == nil {
		db.logger.Warn("proposing the membership guard; will retry", zap.Error(err))
	}
}

// removedMemberError refuses, at proposal, an add naming a removed id.
func (db *DB) removedMemberError(id uint64) error {
	if !db.raft.isRemoved(id) {
		return nil
	}
	return fmt.Errorf("%w: node %d was removed from this cluster; a removed node rejoins under a new id", cluster.ErrInvalidMember, id)
}
