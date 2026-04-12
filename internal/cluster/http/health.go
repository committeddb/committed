package http

import (
	"encoding/json"
	httpgo "net/http"
)

// HealthResponse is the body returned by /health. /health is a pure
// liveness signal — if the process can serve HTTP at all the answer is
// always {"status":"ok"}, regardless of raft or apply state. Container
// orchestrators (k8s livenessProbe, ECS) use this to decide whether to
// restart the process. Don't add raft checks here: a follower that has
// lost quorum is still alive and restarting it won't help.
type HealthResponse struct {
	Status string `json:"status"`
}

// ReadyResponse is the body returned by /ready. Only the Status field
// is exposed — the endpoint is unauthenticated (orchestrators need it
// without credentials), so it deliberately omits cluster internals
// like leader ID and applied index. Operators who need those details
// can check the structured logs.
type ReadyResponse struct {
	Status string `json:"status"`
}

// Health is a pure liveness probe. It always returns 200 with a small
// JSON body. The handler intentionally touches no cluster state — its
// only job is to prove the process can accept and respond to a request.
func (h *HTTP) Health(w httpgo.ResponseWriter, r *httpgo.Request) {
	writeJSONStatus(w, httpgo.StatusOK, HealthResponse{Status: "ok"})
}

// Ready is a readiness probe. It returns 200 once raft has elected a
// leader and the local apply state has advanced past 0 (i.e., the node
// has applied at least one entry). Until both conditions hold it
// returns 503 with a body naming the failing check.
//
// The two checks together cover the two failure modes a fresh node can
// be in: (1) raft hasn't elected yet (no leader visible), or (2) raft
// has elected but this node hasn't replayed/applied any entries from
// its WAL yet. Either state means HTTP traffic that depends on bucket
// reads (Type, Database, etc.) will see stale or empty results, so
// orchestrators should keep traffic away.
func (h *HTTP) Ready(w httpgo.ResponseWriter, r *httpgo.Request) {
	leader := h.c.Leader()
	applied := h.c.AppliedIndex()

	if leader == 0 {
		writeJSONStatus(w, httpgo.StatusServiceUnavailable, ReadyResponse{Status: "not ready"})
		return
	}

	if applied == 0 {
		writeJSONStatus(w, httpgo.StatusServiceUnavailable, ReadyResponse{Status: "not ready"})
		return
	}

	writeJSONStatus(w, httpgo.StatusOK, ReadyResponse{Status: "ok"})
}

// writeJSONStatus marshals body and writes it with the given status
// code and Content-Type: application/json. Used by the health handlers
// because they need to set status before writing the body (writeJson
// only writes the body and sets Content-Type, leaving status at 200).
func writeJSONStatus(w httpgo.ResponseWriter, status int, body any) {
	bs, err := json.Marshal(body)
	if err != nil {
		// Marshalling a fixed struct shouldn't fail; if it does, fall
		// back to a bare status so the probe still gets a clear signal.
		w.WriteHeader(httpgo.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bs)
}
