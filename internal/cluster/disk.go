package cluster

import "errors"

// ErrNotLeader is returned by ReportDisk when the receiving node is not the
// current raft leader. Disk reports are aggregated only on the leader (it is
// the node that decides write admission), so a reporter that lands on a
// follower — usually because leadership moved between resolving the leader's
// API URL and the report arriving — should re-resolve the leader and retry on
// its next cycle. The HTTP layer maps it to 503 leader_unavailable.
var ErrNotLeader = errors.New("this node is not the raft leader")

// DiskVerdict is the cluster-wide write-admission decision the leader computes
// from its own disk state plus the per-voter states members report: the
// cluster-effective disk level is max(leader's level, the level a quorum of
// voters can collectively stay under). Every node enforces the verdict at its
// propose gate, so a write is admitted iff the leader is healthy AND at least
// a quorum of voters are healthy — no matter which node the write enters.
// Returned to a reporting member by ReportDisk (piggybacked on the report
// round trip) and surfaced on GET /v1/node/status.
type DiskVerdict struct {
	// State is the cluster-effective disk-pressure level: "ok", "warn",
	// "critical", or "full". The same kind policy as the node-local gate
	// applies cluster-wide: critical rejects user-data proposals, full also
	// freezes config, checkpoints always flow.
	State string
	// Reason explains a degraded State for operators, e.g. "leader disk
	// critical" or "quorum at risk: 1/3 voters have disk headroom (need 2)".
	// Empty when State is ok/warn.
	Reason string
	// LeaderID is the raft node ID of the leader that computed the verdict.
	LeaderID uint64
}

// DiskAdmissionStatus is one node's current view of write admission — the
// decision its propose gate is actually applying — for GET /v1/node/status.
type DiskAdmissionStatus struct {
	// Admitted reports whether user-data writes are currently admitted at
	// this node (State below critical).
	Admitted bool
	// State and Reason mirror the DiskVerdict driving the decision when
	// Source is "cluster", or the node-local disk level when "local".
	State  string
	Reason string
	// Source is "cluster" when the gate is enforcing a fresh leader-computed
	// verdict, or "local" when it has fallen back to the node-local Phase 1
	// gate (no leader known, leader unreachable, or the cached verdict went
	// stale).
	Source string
	// LeaderID is the leader that computed the verdict (0 under "local").
	LeaderID uint64
}
