package http

import (
	"encoding/json"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// ClusterStatusResponse is the body of GET /cluster/status — cluster-wide state
// that reads the same from ANY node, unlike the per-node /node/status. Today it
// carries the parked-worker summary; it is the home reserved for future
// cluster-wide fields (the fan-out sibling of /node/status).
type ClusterStatusResponse struct {
	ParkedWorkers []ParkedWorkerResponse `json:"parkedWorkers"`
}

// ParkedWorkerResponse names one terminally-parked worker — which worker needs
// operator attention (fix its config and re-POST it, or delete it). The detail
// (since, message, blocked index) is on the per-resource status endpoint; this is
// the cluster-wide summary, the queryable counterpart of the
// committed_worker_parked gauge.
type ParkedWorkerResponse struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ClusterStatus serves GET /cluster/status: cluster-wide diagnostics that are the
// same from any node. Because the parked-worker records are replicated, any node
// answers truthfully with no leader hop — which is what makes this endpoint safe
// behind a load balancer (the per-node /node/status, whose other fields are
// node-local, is not). Read behind the linearize barrier, like the other
// replicated-state status reads. A healthy cluster returns an empty parkedWorkers
// array.
func (h *HTTP) ClusterStatus(w httpgo.ResponseWriter, r *httpgo.Request) {
	if !h.linearize(w, r) {
		return
	}

	parked, err := h.db.ParkedWorkers()
	if err != nil {
		writeInternalError(w, "failed to retrieve parked workers", err)
		return
	}

	resp := ClusterStatusResponse{ParkedWorkers: parkedWorkersResponse(parked)}
	bs, err := json.Marshal(resp)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// parkedWorkersResponse renders the replicated parked-worker records; the
// empty slice (never nil) lets clients iterate without a nil check. A free
// function so the rendering is testable directly — parking a worker for
// real means tripping the circuit breaker, which the status tests do not
// pay for.
func parkedWorkersResponse(parked []cluster.ParkedWorker) []ParkedWorkerResponse {
	out := make([]ParkedWorkerResponse, 0, len(parked))
	for _, p := range parked {
		out = append(out, ParkedWorkerResponse{Kind: p.Kind, ID: p.ID})
	}
	return out
}
