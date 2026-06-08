package db

import (
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// announceAPIURL self-announces this node's advertised HTTP API base URL into
// the replicated memberAPIURLs map, so any node — in particular a follower
// proxying a leader-only read (GET /v1/membership) — can resolve this node's
// API address. It runs once: it waits until this node is a member that can
// reach a leader, proposes the record, and returns.
//
// Address discovery is deliberately decoupled from membership changes. Raft
// replicates node ids and raft peer URLs but never API addresses, and the
// leader — whose API address a follower needs to proxy to — is usually an
// original bootstrap node that never went through `member add`. Self-announce
// covers bootstrap and added nodes uniformly. See raft-leader-read-proxy.md.
//
// A node with no advertised URL (WithAdvertisedAPIURL unset) announces
// nothing. A restart re-announces only if the durably-stored value differs
// from the configured one, so steady state and routine restarts produce no
// proposals. The goroutine is not joined by Close; like scrubScheduler it
// exits on db.ctx.
func (db *DB) announceAPIURL() {
	if db.advertisedAPIURL == "" {
		return
	}
	// Already recorded (the common case on restart) — nothing to do.
	if cur, ok := db.storage.MemberAPIURL(db.ID()); ok && cur == db.advertisedAPIURL {
		return
	}

	entity, err := cluster.NewNodeAPIURLEntity(db.ID(), db.advertisedAPIURL)
	if err != nil {
		db.logger.Error("api-url announce: build entity", zap.Error(err))
		return
	}
	proposal := &cluster.Proposal{Entities: []*cluster.Entity{entity}}

	interval := db.announceInterval
	if interval <= 0 {
		interval = defaultTickInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// Only propose once this node is actually part of the cluster and a
		// leader exists to accept (or forward) the proposal. A join-mode node
		// before its add commits is not yet a member; proposing then would
		// either be dropped or record an address for a node not in the
		// configuration.
		if db.selfIsMember() && db.raft.Leader() != 0 {
			// Re-check storage: our own announce landing (or a peer's view
			// arriving) may already have recorded the current value.
			if cur, ok := db.storage.MemberAPIURL(db.ID()); ok && cur == db.advertisedAPIURL {
				return
			}
			// db.ctx bounds the wait: Propose returns on apply, on a leader
			// change (ErrProposalUnknown — retried below), or on Close. The
			// leader gate above means there is normally one to commit it.
			if err := db.Propose(db.ctx, proposal); err == nil {
				db.logger.Info("announced api url",
					zap.Uint64("node", db.ID()), zap.String("url", db.advertisedAPIURL))
				return
			} else if db.ctx.Err() != nil {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-db.ctx.Done():
			return
		}
	}
}

// selfIsMember reports whether this node currently appears in its own applied
// raft configuration, as either a voter or a learner.
func (db *DB) selfIsMember() bool {
	voters, learners, _ := db.raft.memberStatus()
	id := db.ID()
	if _, ok := voters[id]; ok {
		return true
	}
	_, ok := learners[id]
	return ok
}

// MemberAPIURL returns the advertised HTTP API base URL announced by node id,
// and whether one is known. Reads the replicated memberAPIURLs map, so it
// answers on any node — that is what lets a follower resolve the leader's API
// address to proxy a leader-only read. Powers the leader-read proxy.
func (db *DB) MemberAPIURL(id uint64) (string, bool) {
	return db.storage.MemberAPIURL(id)
}

// Membership returns a snapshot of the raft cluster's configuration and
// replication progress as observed by this node. Per-member MatchIndex is
// populated only when this node is the leader (etcd raft tracks follower
// progress only there); the HTTP layer proxies GET /v1/membership to the
// leader so the served answer always carries it. Members are sorted by id for
// a stable response. See cluster.Membership.
func (db *DB) Membership() cluster.Membership {
	leaderID, term, commit, isLeader, views := db.raft.membershipView()
	apiURLs := db.storage.MemberAPIURLs()

	members := make([]cluster.Member, 0, len(views))
	for _, v := range views {
		m := cluster.Member{
			ID:     v.id,
			Role:   cluster.MemberRoleVoter,
			APIURL: apiURLs[v.id],
		}
		if v.learner {
			m.Role = cluster.MemberRoleLearner
		}
		if v.hasMatch {
			match := v.match
			m.MatchIndex = &match
		}
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	return cluster.Membership{
		NodeID:       db.ID(),
		LeaderID:     leaderID,
		Term:         term,
		CommitIndex:  commit,
		AppliedIndex: db.storage.AppliedIndex(),
		IsLeader:     isLeader,
		Members:      members,
	}
}
