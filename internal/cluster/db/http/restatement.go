package http

import (
	"encoding/json"
	httpgo "net/http"
	"sort"
)

// AddRestatement handles POST /v1/restatement/{id}: one append-only
// interpretation-registry statement (see cluster.Restatement). Id-keyed like
// every other config surface, but IMMUTABLE — a re-POST with different
// content is refused with 400 (author a new restatement to correct one); an
// identical re-POST is an idempotent 200 no-op.
func (h *HTTP) AddRestatement(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		h.writeReadError(w, r, err, "invalid_config", "invalid restatement configuration")
		return
	}
	if err := h.c.ProposeRestatement(r.Context(), c); err != nil {
		writeProposeError(w, err, "restatement", "propose restatement")
		return
	}
	bs, err := json.Marshal(ConfigWriteResponse{ID: c.ID})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// DryRunRestatement handles POST /v1/restatement/dryrun: rehearse a restatement against
// the committed log — the same admission validation as the real POST, then a
// scan of the restatement's own index range through the real interpretation fold
// — and return the diagnostic report. Nothing is admitted or stored: the
// instrument exists because a restatement is append-only and rebinds how every
// consumer reads a slice of history, so "valid config, wrong selectors" must
// cost minutes here, not a correction restatement plus re-materializations.
// Query: maxEntries (default 100000), timeoutSeconds (default 120).
func (h *HTTP) DryRunRestatement(w httpgo.ResponseWriter, r *httpgo.Request) {
	mimeType, body, opts, ctx, cancel, ok := h.dryRunRequest(w, r)
	if !ok {
		return
	}
	defer cancel()
	rep, err := h.c.DryRunRestatement(ctx, mimeType, body, opts)
	if err != nil {
		// The dry-run IS the authoring loop: a rejection carries the
		// admission path's actual words.
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_config", "dry-run: %s", err)
		return
	}
	writeJSONStatus(w, httpgo.StatusOK, rep)
}

// RestatementResponse is one applied restatement in the GET /v1/restatement listing.
type RestatementResponse struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	FromIndex     uint64 `json:"fromIndex"`
	ToIndex       uint64 `json:"toIndex"`
	ReadAsVersion int    `json:"readAsVersion"`
	FromVersion   int    `json:"fromVersion,omitempty"`
	Predicate     string `json:"predicate,omitempty"`
	// Index is the restatement's raft index — its interpretation coordinate
	// (later-in-log wins among matching restatements).
	Index uint64 `json:"index"`
}

// GetRestatements handles GET /v1/restatement: every applied restatement, in log order.
func (h *HTTP) GetRestatements(w httpgo.ResponseWriter, r *httpgo.Request) {
	if !h.linearize(w, r) {
		return
	}
	applied, err := h.c.Restatements()
	if err != nil {
		writeInternalError(w, "failed to list restatements", err)
		return
	}
	out := make([]RestatementResponse, 0, len(applied))
	for _, a := range applied {
		out = append(out, RestatementResponse{
			ID: a.Restatement.ID, Type: a.Restatement.TypeID,
			FromIndex: a.Restatement.FromIndex, ToIndex: a.Restatement.ToIndex,
			ReadAsVersion: a.Restatement.ReadAsVersion, FromVersion: a.Restatement.FromVersion,
			Predicate: a.Restatement.Predicate, Index: a.Index,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	bs, err := json.Marshal(out)
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}
