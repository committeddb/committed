package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddIngestable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid ingestable configuration")
		return
	}

	if err := h.c.ProposeIngestable(r.Context(), c); err != nil {
		writeProposeError(w, err, "ingestable", "propose ingestable")
		return
	}

	// See database.go AddDatabase for the G705 rationale.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(c.ID)) //nolint:gosec // G705
}

func (h *HTTP) GetIngestables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Ingestables()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve ingestables")
		return
	}

	writeConfigurations(w, cfgs)
}
