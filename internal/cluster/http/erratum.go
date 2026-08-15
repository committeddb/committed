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
