package http

import (
	"encoding/json"
	httpgo "net/http"
	"sort"
)

// AddErratum handles POST /v1/erratum/{id}: one append-only
// interpretation-registry statement (see cluster.Erratum). Id-keyed like
// every other config surface, but IMMUTABLE — a re-POST with different
// content is refused with 400 (author a new erratum to correct one); an
// identical re-POST is an idempotent 200 no-op.
func (h *HTTP) AddErratum(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		h.writeReadError(w, r, err, "invalid_config", "invalid erratum configuration")
		return
	}
	if err := h.c.ProposeErratum(r.Context(), c); err != nil {
		writeProposeError(w, err, "erratum", "propose erratum")
		return
	}
	bs, err := json.Marshal(ConfigWriteResponse{ID: c.ID})
	if err != nil {
		writeInternalError(w, "failed to marshal response", err)
		return
	}
	writeJson(w, bs)
}

// DryRunErratum handles POST /v1/erratum/dryrun: rehearse an erratum against
// the committed log — the same admission validation as the real POST, then a
// scan of the erratum's own index range through the real interpretation fold
// — and return the diagnostic report. Nothing is admitted or stored: the
// instrument exists because an erratum is append-only and rebinds how every
// consumer reads a slice of history, so "valid config, wrong selectors" must
// cost minutes here, not a correction erratum plus re-materializations.
// Query: maxEntries (default 100000), timeoutSeconds (default 120).
func (h *HTTP) DryRunErratum(w httpgo.ResponseWriter, r *httpgo.Request) {
	mimeType, body, opts, ctx, cancel, ok := h.dryRunRequest(w, r)
	if !ok {
		return
	}
	defer cancel()
	rep, err := h.c.DryRunErratum(ctx, mimeType, body, opts)
	if err != nil {
		// The dry-run IS the authoring loop: a rejection carries the
		// admission path's actual words.
		writeErrorf(w, httpgo.StatusBadRequest, "invalid_config", "dry-run: %s", err)
		return
	}
	writeJSONStatus(w, httpgo.StatusOK, rep)
}

// ErratumResponse is one applied erratum in the GET /v1/erratum listing.
type ErratumResponse struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	FromIndex       uint64 `json:"fromIndex"`
	ToIndex         uint64 `json:"toIndex"`
	RebindToVersion int    `json:"rebindToVersion"`
	FromVersion     int    `json:"fromVersion,omitempty"`
	Predicate       string `json:"predicate,omitempty"`
	// Index is the erratum's raft index — its interpretation coordinate
	// (later-in-log wins among matching errata).
	Index uint64 `json:"index"`
}

// GetErrata handles GET /v1/erratum: every applied erratum, in log order.
func (h *HTTP) GetErrata(w httpgo.ResponseWriter, r *httpgo.Request) {
	if !h.linearize(w, r) {
		return
	}
	applied, err := h.c.Errata()
	if err != nil {
		writeInternalError(w, "failed to list errata", err)
		return
	}
	out := make([]ErratumResponse, 0, len(applied))
	for _, a := range applied {
		out = append(out, ErratumResponse{
			ID: a.Erratum.ID, Type: a.Erratum.TypeID,
			FromIndex: a.Erratum.FromIndex, ToIndex: a.Erratum.ToIndex,
			RebindToVersion: a.Erratum.RebindToVersion, FromVersion: a.Erratum.FromVersion,
			Predicate: a.Erratum.Predicate, Index: a.Index,
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
