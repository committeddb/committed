package db

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// Agreed peer addresses. A member's recorded address changes through raft's
// own UpdateNode conf change, carrying the URL in its Context: it applies in
// the conf-change pass in log order on every node, raft treats it as a
// membership no-op, and releases that predate it ignore it — no new entity
// type and no feature gate. Applying it overwrites the recorded address and
// points every node's transport at it (applyConfChange).
//
// Two things propose it:
//   - the leader, once it has delivered a message to a peer's current address
//     and that address differs from the recorded one (a node that moved and
//     self-reported, or an operator's changed COMMITTED_PEERS) — and for its
//     own address, so followers that never had it in COMMITTED_PEERS learn it;
//   - an operator, through PUT /v1/membership/{id}/address (SetMemberAddress),
//     for a node that can't announce itself (a moved learner).

// agreedAddressInterval is how often the leader compares the addresses its
// transport delivers to with the recorded ones.
const agreedAddressInterval = time.Second

// proposeUpdateNode submits an UpdateNode carrying a member's address.
func (db *DB) proposeUpdateNode(ctx context.Context, id uint64, peerURL string) error {
	cc := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeUpdateNode.Enum(), NodeId: &id}},
		Context:    []byte(peerURL),
	}
	select {
	case db.confChangeC <- cc:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}
}

// SetMemberAddress records a member's raft peer address cluster-wide and
// points every node's transport at it, returning once this node has applied
// it. For a node that can't announce its own new address (a learner, which
// never campaigns, or a node that is down).
func (db *DB) SetMemberAddress(ctx context.Context, id uint64, rawURL string) error {
	if id == 0 {
		return fmt.Errorf("%w: id must be non-zero", cluster.ErrInvalidMember)
	}
	peerURL, err := cluster.CanonicalPeerURL(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %w", cluster.ErrInvalidMember, err)
	}
	voters, learners, joint := db.raft.memberStatus()
	_, isVoter := voters[id]
	_, isLearner := learners[id]
	if !isVoter && !isLearner {
		return fmt.Errorf("%w: node %d is not a member", cluster.ErrInvalidMember, id)
	}
	if joint {
		return cluster.ErrMembershipChangeInProgress
	}
	if db.storage.MemberPeerURLs()[id] == peerURL {
		return nil
	}
	if err := db.proposeUpdateNode(ctx, id, peerURL); err != nil {
		return err
	}
	return db.waitForRecordedAddress(ctx, id, peerURL)
}

// waitForRecordedAddress blocks until this node has applied an UpdateNode
// recording peerURL for id (bounded like waitForMembership: raft drops a
// conf change proposed while another is pending).
func (db *DB) waitForRecordedAddress(ctx context.Context, id uint64, peerURL string) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, membershipSettleTimeout, ErrMembershipUnsettled)
		defer cancel()
	}
	for {
		ch := db.appliedNotify()
		if db.storage.MemberPeerURLs()[id] == peerURL {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return ctx.Err()
		case <-db.ctx.Done():
			return db.ctx.Err()
		}
	}
}

// recordAgreedAddresses runs on every node; the leader records any member
// address it has delivered to that differs from the recorded one, one change
// per round (raft holds one pending conf change at a time). It also switches
// the membership guard on (proposeMembershipGuard).
func (db *DB) recordAgreedAddresses() {
	ticker := time.NewTicker(agreedAddressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-db.ctx.Done():
			return
		}
		if db.raft.Leader() != db.raft.id {
			continue
		}
		ctx, cancel := context.WithTimeout(db.ctx, agreedAddressInterval)
		db.proposeMembershipGuard(ctx)
		cancel()
		id, peerURL, ok := db.nextAgreedAddress()
		if !ok {
			continue
		}
		db.logger.Info("recording a member's peer address cluster-wide",
			zap.Uint64("member", id), zap.String("address", peerURL))
		ctx, cancel = context.WithTimeout(db.ctx, agreedAddressInterval)
		if err := db.proposeUpdateNode(ctx, id, peerURL); err != nil && db.ctx.Err() == nil {
			db.logger.Warn("proposing a member's peer address; will retry", zap.Uint64("member", id), zap.Error(err))
		}
		cancel()
	}
}

// nextAgreedAddress is the first member whose recorded address differs from
// the one the leader can show works: its own advertised URL, or a peer
// address its transport has delivered to.
func (db *DB) nextAgreedAddress() (uint64, string, bool) {
	voters, learners, joint := db.raft.memberStatus()
	if joint {
		return 0, "", false
	}
	recorded := db.storage.MemberPeerURLs()
	if self := db.raft.selfURL; self != "" {
		if _, member := voters[db.raft.id]; member && recorded[db.raft.id] != self {
			return db.raft.id, self, true
		}
	}
	for _, set := range []map[uint64]struct{}{voters, learners} {
		for id := range set {
			if id == db.raft.id {
				continue
			}
			view, ok := db.raft.transport.PeerAddress(id)
			if !ok || !view.Delivered {
				continue
			}
			rec := recorded[id]
			if c, err := cluster.CanonicalPeerURL(rec); err == nil {
				rec = c
			}
			if rec != view.URL {
				return id, view.URL, true
			}
		}
	}
	return 0, "", false
}
