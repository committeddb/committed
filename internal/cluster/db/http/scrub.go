package http

import (
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
	// Route through the shared choke point so a disk-full rejection is a truthful
	// 507 (scrub is admission-config-class, rejected at disk-full) and a deadline is
	// a 503 — not the opaque 500 a hand-rolled switch produced. rebuild/config cases
	// are inert for a scrub.
	if err := h.c.Scrub(r.Context()); err != nil {
		writeProposeError(w, err, "scrub", "request scrub")
		return
	}
	w.WriteHeader(httpgo.StatusAccepted)
}
