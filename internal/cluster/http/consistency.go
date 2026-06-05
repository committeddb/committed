package http

import (
	"context"
	httpgo "net/http"
	"time"
)

// defaultReadIndexTimeout bounds how long a default (linearizable) GET waits
// for the raft ReadIndex quorum confirmation before the handler gives up and
// returns 503. Sized well under the server WriteTimeout (30s) so a
// partitioned node returns a clean error rather than holding the connection
// open. Overridable per server via WithReadIndexTimeout.
const defaultReadIndexTimeout = 5 * time.Second

// Recognized values of the ?consistency query parameter.
const (
	consistencyStale        = "stale"
	consistencyLinearizable = "linearizable"
)

// linearize enforces the read-consistency contract before a GET handler reads
// replicated state. It returns true if the caller should proceed with the
// read, and false if it has already written an error response.
//
//   - ?consistency=stale — skip the quorum round-trip and serve local state
//     immediately. Tailers and dashboards that tolerate bounded staleness use
//     this to avoid the per-read heartbeat cost. Returns true without calling
//     into raft.
//   - default / ?consistency=linearizable — run cluster.LinearizableRead,
//     bounded by readIndexTimeout. On success returns true. If the leader
//     can't confirm quorum in time (no quorum, this node partitioned out),
//     writes 503 and returns false — the node refuses to serve a read it
//     can't prove is up to date rather than returning possibly-stale data.
//   - any other value — writes 400 and returns false.
//
// Call this immediately before the handler reads replicated state, so a
// request that will fail cheap local validation (bad id, bad page cursor)
// doesn't pay a heartbeat round-trip first.
func (h *HTTP) linearize(w httpgo.ResponseWriter, r *httpgo.Request) bool {
	switch r.URL.Query().Get("consistency") {
	case consistencyStale:
		return true
	case "", consistencyLinearizable:
		ctx, cancel := context.WithTimeout(r.Context(), h.readIndexTimeout)
		defer cancel()
		if err := h.c.LinearizableRead(ctx); err != nil {
			writeError(w, httpgo.StatusServiceUnavailable, "not_linearizable",
				"could not confirm a linearizable read (no quorum or leader unreachable); retry, or pass ?consistency=stale to accept a possibly-stale read")
			return false
		}
		return true
	default:
		writeError(w, httpgo.StatusBadRequest, "invalid_consistency",
			`consistency query parameter must be "linearizable" (the default) or "stale"`)
		return false
	}
}
