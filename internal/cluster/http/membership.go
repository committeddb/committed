package http

import (
	"context"
	"errors"
	httpgo "net/http"
	"strconv"

	"github.com/committeddb/committed/internal/cluster"
)

// AddMemberRequest is the body of POST /v1/membership: add a voting node to
// the raft cluster.
//
//   - id  is the new node's raft node id (must be unique and non-zero).
//   - url is the node's advertised peer URL — the address the existing
//     members will dial. Raft replicates only node ids, never addresses, so
//     the url must be supplied here and rides along on the conf change.
//
// The node named here must already be running in join mode (COMMITTED_JOIN)
// with a peer set that lets it reach the existing cluster. See
// docs/operations/membership.md.
type AddMemberRequest struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

// AddMember handles POST /v1/membership. It performs a joint-consensus
// membership change and blocks until the new member is observed in the final
// configuration (or the request context fires). On success it returns 204.
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

	if err := h.c.AddMember(r.Context(), req.ID, req.URL); err != nil {
		writeMembershipError(w, err, "add")
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

// writeMembershipError maps an AddMember/RemoveMember error to a response:
// a malformed request (cluster.ErrInvalidMember) → 400; a context error →
// 503 (the change was submitted to raft but couldn't be confirmed before the
// request deadline — typically this node can't currently reach a quorum — and
// may still take effect); anything else → 500.
func writeMembershipError(w httpgo.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, cluster.ErrInvalidMember):
		writeError(w, httpgo.StatusBadRequest, "invalid_member", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, httpgo.StatusServiceUnavailable, "membership_unconfirmed",
			"membership change submitted but not confirmed before the request deadline; it may still take effect once a quorum is reachable")
	default:
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to "+action+" member")
	}
}
