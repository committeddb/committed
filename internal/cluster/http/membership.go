package http

import (
	"context"
	"encoding/json"
	"errors"
	httpgo "net/http"
	"strconv"

	"github.com/committeddb/committed/internal/cluster"
)

// MembershipResponse is the body of GET /v1/membership: the raft cluster
// configuration and replication progress, as produced by the leader (the
// route is leaderRead-wrapped, so a follower proxies to the leader and the
// per-member match_index is always populated). CommitIndex is the catch-up
// target; a caller decides a member is "caught up" by comparing its
// match_index against commit_index with its own threshold.
type MembershipResponse struct {
	NodeID       uint64           `json:"node_id"`
	LeaderID     uint64           `json:"leader_id"`
	Term         uint64           `json:"term"`
	CommitIndex  uint64           `json:"commit_index"`
	AppliedIndex uint64           `json:"applied_index"`
	IsLeader     bool             `json:"is_leader"`
	Members      []MemberResponse `json:"members"`
}

// MemberResponse is one node's entry in MembershipResponse. MatchIndex is
// omitted when unknown (a follower-built snapshot, before the leader-proxy
// hop); APIURL is omitted when the member has not announced one.
type MemberResponse struct {
	ID         uint64  `json:"id"`
	Role       string  `json:"role"`
	MatchIndex *uint64 `json:"match_index,omitempty"`
	APIURL     string  `json:"api_url,omitempty"`
}

// GetMembership handles GET /v1/membership. It returns the cluster
// configuration (voters/learners), each member's leader-observed matched
// index, and this answer's leader/term/commit/applied context. The route is
// wrapped in leaderRead so the answer is always leader-truthful even when a
// caller (behind a load balancer) reaches a follower. See
// docs/operations/membership.md and raft-leader-read-proxy.md.
func (h *HTTP) GetMembership(w httpgo.ResponseWriter, r *httpgo.Request) {
	m := h.c.Membership()

	resp := MembershipResponse{
		NodeID:       m.NodeID,
		LeaderID:     m.LeaderID,
		Term:         m.Term,
		CommitIndex:  m.CommitIndex,
		AppliedIndex: m.AppliedIndex,
		IsLeader:     m.IsLeader,
		Members:      make([]MemberResponse, 0, len(m.Members)),
	}
	for _, mem := range m.Members {
		resp.Members = append(resp.Members, MemberResponse{
			ID:         mem.ID,
			Role:       mem.Role,
			MatchIndex: mem.MatchIndex,
			APIURL:     mem.APIURL,
		})
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal membership", err)
		return
	}
	writeJson(w, bs)
}

// AddMemberRequest is the body of POST /v1/membership: add a node to the raft
// cluster.
//
//   - id      is the new node's raft node id (must be unique and non-zero).
//   - url     is the node's advertised peer URL — the address the existing
//     members will dial. Raft replicates only node ids, never addresses, so
//     the url must be supplied here and rides along on the conf change.
//   - learner adds the node as a non-voting learner (replicates the log but
//     does not count toward quorum) instead of a voter. Defaults to false, so
//     an existing voter-add caller is unchanged. Promote a learner to a voter
//     later with POST /membership/{id}/promote.
//
// The node named here must already be running in join mode (COMMITTED_JOIN)
// with a peer set that lets it reach the existing cluster. See
// docs/operations/membership.md.
type AddMemberRequest struct {
	ID      uint64 `json:"id"`
	URL     string `json:"url"`
	Learner bool   `json:"learner"`
}

// AddMember handles POST /v1/membership. It performs a joint-consensus
// membership change and blocks until the new member is observed in the final
// configuration (or the request context fires). On success it returns 204.
// With "learner": true the node is added as a non-voting learner; otherwise
// (the default) as a voter.
//
// The change is partition-safe: joint consensus requires a majority of both
// the old and new configurations throughout the transition, so it can never
// shift quorum in a way that loses a committed entry. The request may be sent
// to any node — a follower forwards the proposal to the leader.
func (h *HTTP) AddMember(w httpgo.ResponseWriter, r *httpgo.Request) {
	req := &AddMemberRequest{}
	if err := unmarshalBody(r, req); err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}

	add := h.c.AddMember
	if req.Learner {
		add = h.c.AddLearner
	}
	if err := add(r.Context(), req.ID, req.URL); err != nil {
		writeMembershipError(w, err, "add")
		return
	}

	w.WriteHeader(httpgo.StatusNoContent)
}

// PromoteMember handles POST /v1/membership/{id}/promote. It promotes an
// existing learner to a voter via a joint-consensus membership change and
// blocks until the change has taken effect (or the request context fires). On
// success it returns 204; promoting a non-learner / unknown id returns 400.
//
// The endpoint does not judge whether the learner has caught up — the caller
// gates promotion on the replication progress reported by GET /v1/membership.
// Partition-safe and callable on any node, like the other membership changes.
func (h *HTTP) PromoteMember(w httpgo.ResponseWriter, r *httpgo.Request) {
	raw := r.PathValue("id")
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_member_id",
			"member id %q is not a positive integer", raw)
		return
	}

	if err := h.c.PromoteMember(r.Context(), id); err != nil {
		writeMembershipError(w, err, "promote")
		return
	}

	w.WriteHeader(httpgo.StatusNoContent)
}

// RemoveMember handles DELETE /v1/membership/{id}. It performs a
// joint-consensus membership change and blocks until node {id} is observed
// gone from the final configuration (or the request context fires). On
// success it returns 204. Partition-safe and callable on any node, exactly
// like AddMember.
func (h *HTTP) RemoveMember(w httpgo.ResponseWriter, r *httpgo.Request) {
	raw := r.PathValue("id")
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_member_id",
			"member id %q is not a positive integer", raw)
		return
	}

	if err := h.c.RemoveMember(r.Context(), id); err != nil {
		writeMembershipError(w, err, "remove")
		return
	}

	w.WriteHeader(httpgo.StatusNoContent)
}

// writeMembershipError maps an Add/Remove/Promote error to a response:
// a malformed request (cluster.ErrInvalidMember) or a promote of a
// non-learner / unknown id (cluster.ErrNotLearner) → 400; removing the sole
// voter (cluster.ErrWouldRemoveLastVoter) → 409 (well-formed but conflicts with
// the current state); a context error → 503 (the change was submitted to raft
// but couldn't be confirmed before the request deadline — typically this node
// can't currently reach a quorum — and may still take effect); anything else →
// 500.
func writeMembershipError(w httpgo.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, cluster.ErrInvalidMember):
		writeError(w, httpgo.StatusBadRequest, "invalid_member", err.Error())
	case errors.Is(err, cluster.ErrNotLearner):
		writeError(w, httpgo.StatusBadRequest, "not_a_learner", err.Error())
	case errors.Is(err, cluster.ErrWouldRemoveLastVoter):
		writeError(w, httpgo.StatusConflict, "would_remove_last_voter", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, httpgo.StatusServiceUnavailable, "membership_unconfirmed",
			"membership change submitted but not confirmed before the request deadline; it may still take effect once a quorum is reachable")
	default:
		writeInternalError(w, "failed to "+action+" member", err)
	}
}
