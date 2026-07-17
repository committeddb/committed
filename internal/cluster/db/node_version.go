package db

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/version"
)

// Feature-level requirements for emission gating. A feature is emitted only once
// the cluster-agreed minimum feature level (min over all current members'
// self-announced version.FeatureLevel) is at least its requirement, so its
// entries are never committed while a member that can't apply them is present.
// See version.FeatureLevel for how the axis is maintained.
const (
	// featureLevelRefreshBoundary gates the ingest refresh-boundary marker: a
	// pre-mechanism binary (level 0) has no IsRefreshBoundary branch and would
	// dead-letter the marker, so the marker is held until every member is at
	// level 1. It is the baseline feature — level 1 is the first binary carrying
	// the version-announce mechanism, which also understands the marker.
	featureLevelRefreshBoundary uint64 = 1
)

// announceVersion self-announces this node's supported cluster feature level
// (version.FeatureLevel) into the replicated memberVersions map, so any node can
// compute the cluster-agreed minimum and gate emission of features an older peer
// can't yet apply. It mirrors announceAPIURL: waits until this node is a member
// that can reach a leader, proposes the record once, and returns. A restart
// re-announces only if the durably-stored level differs from this binary's — so a
// steady state and routine restarts produce no proposals, while an upgraded
// binary re-announces its higher level. Not joined by Close; exits on db.ctx.
func (db *DB) announceVersion() {
	// Already recorded at this binary's level (the common case on restart, and
	// after an upgrade once the higher level has landed) — nothing to do.
	if cur, ok := db.storage.MemberVersion(db.ID()); ok && cur == version.FeatureLevel {
		return
	}

	entity, err := cluster.NewNodeVersionEntity(db.ID(), version.FeatureLevel)
	if err != nil {
		db.logger.Error("version announce: build entity", zap.Error(err))
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
		// Only propose once this node is part of the cluster and a leader exists
		// to accept (or forward) the proposal — a join-mode node before its add
		// commits is not yet a member.
		if db.selfIsMember() && db.raft.Leader() != 0 {
			if cur, ok := db.storage.MemberVersion(db.ID()); ok && cur == version.FeatureLevel {
				return
			}
			if err := db.Propose(db.ctx, proposal); err == nil {
				db.logger.Info("announced feature level",
					zap.Uint64("node", db.ID()), zap.Uint64("featureLevel", version.FeatureLevel))
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

// featureEnabled reports whether every current member has announced support for
// at least the required feature level — i.e. emitting a feature guarded at that
// level can't reach a member that would mis-apply it. It is the gate that keeps
// the rolling-upgrade promise: a feature's entries are only ever committed once
// the whole cluster can apply them.
func (db *DB) featureEnabled(required uint64) bool {
	return db.clusterMinFeatureLevel() >= required
}

// clusterMinFeatureLevel is the lowest feature level announced across all
// current members (voters ∪ learners). A member with no announcement — one that
// hasn't announced yet, or a binary predating the mechanism — counts as level 0,
// the conservative floor that holds emission until it announces or is upgraded.
// It reads current membership (not the raw bucket), so a removed node's lingering
// announcement never affects the result.
func (db *DB) clusterMinFeatureLevel() uint64 {
	voters, learners, _ := db.raft.memberStatus()
	members := make(map[uint64]struct{}, len(voters)+len(learners))
	for id := range voters {
		members[id] = struct{}{}
	}
	for id := range learners {
		members[id] = struct{}{}
	}
	return minFeatureLevel(members, db.storage.MemberVersions())
}

// minFeatureLevel returns the lowest announced level across members; an id
// absent from versions counts as 0. With no members it returns 0. Split out as a
// pure function so the gating logic is unit-testable without a running cluster.
func minFeatureLevel(members map[uint64]struct{}, versions map[uint64]uint64) uint64 {
	if len(members) == 0 {
		return 0
	}
	min := ^uint64(0)
	for id := range members {
		lvl := versions[id] // 0 if unannounced
		if lvl < min {
			min = lvl
		}
	}
	return min
}

// containsRefreshBoundary reports whether the proposal carries an ingest
// refresh-boundary marker — a semantic entity a pre-mechanism sink would
// mis-apply into a permanent dead-letter.
func containsRefreshBoundary(p *cluster.Proposal) bool {
	for _, e := range p.Entities {
		if e.IsRefreshBoundary() {
			return true
		}
	}
	return false
}

// awaitRefreshBoundaryEnabled blocks until the cluster can apply a
// refresh-boundary marker (every member at featureLevelRefreshBoundary), so the
// marker is never committed while a member that would dead-letter it is present.
// It reports whether the caller should proceed to propose the marker; it returns
// false — telling the caller to DROP this marker — when ctx is cancelled (worker
// stopping) or this node is no longer the ingest node (leadership loss). Dropping
// is safe: the ingest worker's position checkpoint is held behind the same
// backpressure and never advances, so a restart re-runs the refresh and
// re-emits the marker at the same epoch.
//
// Because the ingest worker blocks on its `pr` send until this returns, holding
// here also holds the worker's subsequent position checkpoint — so the marker
// commits before the position advances (ordering preserved) without any change
// to the Ingestable.Ingest interface.
func (db *DB) awaitRefreshBoundaryEnabled(ctx context.Context, id string) bool {
	if db.featureEnabled(featureLevelRefreshBoundary) {
		return true
	}
	db.logger.Info("holding ingest refresh-boundary marker until the cluster is fully upgraded",
		zap.String("id", id),
		zap.Uint64("required", featureLevelRefreshBoundary),
		zap.Uint64("clusterMin", db.clusterMinFeatureLevel()))

	interval := db.announceInterval
	if interval <= 0 {
		interval = defaultTickInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if !db.isNode(id) {
				return false
			}
			if db.featureEnabled(featureLevelRefreshBoundary) {
				return true
			}
		}
	}
}
