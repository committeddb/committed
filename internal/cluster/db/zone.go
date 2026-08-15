package db

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// Zone-pinned syncables: a syncable config carrying `zone = "..."` is served
// by a node in that zone instead of the leader, so sync egress to a sink in
// the same zone never pays a redundant cross-zone crossing (the data already
// lives on every node's event log; raft replication paid the crossing once).
//
// Semantics are STRICT-PIN-ONLY (decided 2026-08-12): when no current member
// announces the pinned zone, the syncable STALLS loudly — visible in status
// as unsatisfiable — and never silently falls back to the leader, because a
// silent fallback quietly reintroduces the exact cost the pin exists to
// avoid. The event log is permanent, so a stalled sink always catches up:
// lag, never loss.
//
// Ownership is a pure function of replicated state (the stored config's
// zone, the announced member zones, current membership), so every node
// computes the same owner. Resolution is feature-gated: until the cluster
// minimum feature level includes zone pinning, EVERY node — including this
// binary — resolves leader-owns, because a mixed-version cluster with an old
// leader resolving "leader owns everything" and a new node resolving a pin
// would stream to the same sink twice.

// featureLevelZonePinning gates zone-pin ownership resolution and admission
// of `zone` syncable configs. See version.FeatureLevel level 3.
const featureLevelZonePinning uint64 = 3

// zoneOwner resolves the owning node for a pinned zone: the LOWEST node id
// among current members announcing that zone (the deterministic tie-break
// when a zone has several nodes), or 0 when no current member announces it —
// the strict pin's unsatisfiable state. Pure function for testability.
func zoneOwner(zone string, members map[uint64]struct{}, zones map[uint64]string) uint64 {
	var owner uint64
	for id := range members {
		if zones[id] != zone {
			continue
		}
		if owner == 0 || id < owner {
			owner = id
		}
	}
	return owner
}

// syncableZonePin reports the stored config's pin: its zone (or "" for an
// unpinned config — today's behavior), whether resolution is ACTIVE (the
// feature gate), and the resolved owner (0 = unsatisfiable when pinned).
func (db *DB) syncableZonePin(id string) (zone string, active bool, owner uint64) {
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return "", false, 0
	}
	zone, err := db.parser.SyncableZone(cfg.MimeType, cfg.Data)
	if err != nil || zone == "" {
		return "", false, 0
	}
	if !db.featureEnabled(featureLevelZonePinning) {
		// Mixed-version cluster: resolve leader-owns everywhere until every
		// member can resolve zones — never two concurrent writers.
		return zone, false, 0
	}
	voters, learners, _ := db.raft.memberStatus()
	members := make(map[uint64]struct{}, len(voters)+len(learners))
	for id := range voters {
		members[id] = struct{}{}
	}
	for id := range learners {
		members[id] = struct{}{}
	}
	return zone, true, zoneOwner(zone, members, db.storage.MemberZones())
}

// SyncableZonePin is the status surface's view: the configured zone (ok=false
// for an unpinned syncable) and whether the pin is currently unsatisfiable
// (no current member in the zone — the strict stall). A pin on a
// not-yet-upgraded cluster reports its zone with unsatisfiable=false: it is
// leader-served until the gate opens, which the ownerNode field shows.
func (db *DB) SyncableZonePin(id string) (zone string, unsatisfiable bool, ok bool) {
	zone, active, owner := db.syncableZonePin(id)
	if zone == "" {
		return "", false, false
	}
	return zone, active && owner == 0, true
}

// refuseUnservableZonePin refuses a syncable config whose `zone` cannot be
// served: the cluster's minimum feature level predates zone resolution (the
// pin would be silently leader-served — the admission-validation rule:
// never accept a config that fails, or silently no-ops, at first use), or no
// current member announces the zone. Unpinned configs pass for free.
func (db *DB) refuseUnservableZonePin(c *cluster.Configuration) error {
	zone, err := db.parser.SyncableZone(c.MimeType, c.Data)
	if err != nil || zone == "" {
		return nil // unpinned (an unparseable config was refused earlier)
	}
	if !db.featureEnabled(featureLevelZonePinning) {
		return &cluster.ClusterBelowFeatureLevelError{
			Feature:    "zone-pinned syncables",
			Required:   featureLevelZonePinning,
			ClusterMin: db.clusterMinFeatureLevel(),
		}
	}
	voters, learners, _ := db.raft.memberStatus()
	members := make(map[uint64]struct{}, len(voters)+len(learners))
	for id := range voters {
		members[id] = struct{}{}
	}
	for id := range learners {
		members[id] = struct{}{}
	}
	if zoneOwner(zone, members, db.storage.MemberZones()) == 0 {
		return cluster.NewConfigError(fmt.Errorf(
			"zone %q has no announced node among current members: set COMMITTED_ZONE=%q on the node in that zone (announced at startup), then re-POST — a strict pin with no serving node would stall at first use",
			zone, zone))
	}
	return nil
}

// announceZone self-announces this node's configured zone (COMMITTED_ZONE)
// into the replicated memberZones map, mirroring announceVersion: it waits
// until this node is a member with a reachable leader, proposes once, and
// exits. Re-announces only when the durably-stored zone differs — including
// announcing "" to CLEAR a stale identity after an operator unsets the env —
// so steady state and routine restarts produce no proposals. An unset zone
// on a node that never announced proposes nothing.
func (db *DB) announceZone() {
	if cur, ok := db.storage.MemberZone(db.ID()); (ok && cur == db.zone) || (!ok && db.zone == "") {
		return
	}

	entity, err := cluster.NewNodeZoneEntity(db.ID(), db.zone)
	if err != nil {
		db.logger.Error("zone announce: build entity", zap.Error(err))
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
		if db.selfIsMember() && db.raft.Leader() != 0 {
			if cur, ok := db.storage.MemberZone(db.ID()); ok && cur == db.zone {
				return
			}
			if err := db.Propose(db.ctx, proposal); err == nil {
				db.logger.Info("announced zone",
					zap.Uint64("node", db.ID()), zap.String("zone", db.zone))
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
