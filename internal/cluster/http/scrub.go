package http

import (
	"context"
	"errors"
	httpgo "net/http"
)

// Scrub handles POST /v1/scrub: the manual lever for right-to-be-forgotten
// erasure. It proposes a Scrub command through Raft and, on acceptance, returns
// 202 — each node then physically removes the already-delete-proposed entities
// from its permanent event log in the background. Complements the automatic
// scheduler for SLA-expedited erasure. Callable on any node; the proposal
// forwards to the leader. See docs/event-log-architecture.md
// § "Right-to-be-forgotten / deletes".
func (h *HTTP) Scrub(w httpgo.ResponseWriter, r *httpgo.Request) {
	if err := h.c.Scrub(r.Context()); err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			writeError(w, httpgo.StatusServiceUnavailable, "scrub_unconfirmed",
				"scrub submitted but not confirmed before the request deadline; it may still take effect once a quorum is reachable")
		default:
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to request scrub")
		}
		return
	}
	w.WriteHeader(httpgo.StatusAccepted)
}
