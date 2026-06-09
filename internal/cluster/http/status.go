package http

import (
	"encoding/json"
	httpgo "net/http"
)

// NodeStatusResponse is the body of GET /node/status — per-node
// diagnostics for the node that served the request. The fields are
// node-local and ephemeral (not part of the replicated config content),
// so two nodes can legitimately return different bodies for the same
// cluster state. node is the answering node's raft ID — load-bearing
// behind a load balancer, where the path scope ("this node") doesn't tell
// you which node "this" is. leader and appliedIndex mirror the raft
// details the unauthenticated /ready probe deliberately omits, making
// /node/status their queryable, authenticated counterpart.
type NodeStatusResponse struct {
	Node            uint64                   `json:"node"`
	Leader          uint64                   `json:"leader"`
	AppliedIndex    uint64                   `json:"appliedIndex"`
	DegradedConfigs []DegradedConfigResponse `json:"degradedConfigs"`
}

// DegradedConfigResponse names one config this node persisted but could
// not build into a live object — a node-local condition (usually a
// missing ${VAR} secret on this node), not a defect in the replicated
// bytes, which are valid cluster-wide. error names the failing ${VAR},
// never an interpolated value (interpolation failed, so none exists).
type DegradedConfigResponse struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Error string `json:"error"`
}

// NodeStatus serves GET /node/status: this node's degraded configs plus a
// little raft identity. It is authenticated (same group as the config
// endpoints) and answers for the node that received the request — the
// queryable, authenticated diagnosis behind the committed_config_build_errors
// gauge, which can alert "node N has a degraded config" but can't say which
// or why. A healthy node returns an empty degradedConfigs array.
func (h *HTTP) NodeStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	errs := h.c.ConfigBuildErrors()
	degraded := make([]DegradedConfigResponse, 0, len(errs))
	for _, e := range errs {
		degraded = append(degraded, DegradedConfigResponse{Kind: e.Kind, ID: e.ID, Error: e.Error})
	}

	resp := NodeStatusResponse{
		Node:            h.c.ID(),
		Leader:          h.c.Leader(),
		AppliedIndex:    h.c.AppliedIndex(),
		DegradedConfigs: degraded,
	}

	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}
