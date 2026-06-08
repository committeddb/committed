package cluster

import "errors"

// ErrInvalidMember is returned by Cluster.AddMember / Cluster.RemoveMember
// when the supplied node id or url is not usable — a zero id, or an empty
// url on an add. The HTTP layer maps it to 400. It is a static-validation
// error (the request is malformed), distinct from a runtime failure to
// commit the change, which surfaces as a context error.
var ErrInvalidMember = errors.New("cluster: invalid member")

// ErrNotLearner is returned by Cluster.PromoteMember when the target id is not
// a current learner — it is already a voter, or it is unknown to the
// configuration. The HTTP layer maps it to 400. It guards against promoting a
// non-learner: a promote of an unknown id would add a phantom voter with no
// transport entry, and a promote of an existing voter is a no-op. Distinct
// from a runtime failure to commit the change, which surfaces as a context
// error (→ 503).
var ErrNotLearner = errors.New("cluster: not a learner")

// Member roles reported by Membership. A node is a voter (counts toward
// quorum) or a learner (replicates the log but does not vote). Learners are
// not yet added by this build; the role is reported generally so the
// learner-promotion work layers on without a response-shape change.
const (
	MemberRoleVoter   = "voter"
	MemberRoleLearner = "learner"
)

// Member is one node's entry in a Membership snapshot.
type Member struct {
	// ID is the raft node id.
	ID uint64
	// Role is MemberRoleVoter or MemberRoleLearner.
	Role string
	// MatchIndex is the highest log index the leader has confirmed this
	// member has replicated. It is leader-only state (etcd raft tracks
	// follower progress only on the leader), so it is nil unless the
	// snapshot was produced by the leader — which is why the HTTP read
	// proxies to the leader. A caller computes "caught up" by comparing
	// MatchIndex against CommitIndex with its own threshold.
	MatchIndex *uint64
	// APIURL is the member's advertised HTTP API base URL, as self-announced
	// into the replicated address map. Empty when the member has not
	// announced one (e.g. no COMMITTED_API_URL set).
	APIURL string
}

// Membership is a snapshot of the raft cluster's configuration and
// replication progress, as observed by the answering node. It powers
// GET /v1/membership. CommitIndex (the catch-up target) and each member's
// MatchIndex are meaningful for catch-up decisions only when produced by the
// leader (IsLeader true / NodeID == LeaderID); the HTTP layer proxies the
// read to the leader so a caller behind a load balancer gets a truthful
// answer regardless of which node it reaches.
type Membership struct {
	// NodeID is the node that produced this snapshot.
	NodeID uint64
	// LeaderID is the raft leader this node currently believes in (0 if
	// none known). A caller that reaches a node which cannot produce a
	// leader-truthful answer uses this to find the leader.
	LeaderID uint64
	// Term is the current raft term.
	Term uint64
	// CommitIndex is the highest committed raft index this node knows — the
	// catch-up target a caller compares a learner's MatchIndex against.
	CommitIndex uint64
	// AppliedIndex is how far this node has applied committed entries to
	// application state.
	AppliedIndex uint64
	// IsLeader reports whether this node is the raft leader (and thus
	// whether the per-member MatchIndex values are populated).
	IsLeader bool
	// Members is the cluster configuration, voters and learners, sorted by
	// id for a stable response.
	Members []Member
}
