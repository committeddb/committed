package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid syncable configuration")
		return
	}

	if err := h.c.ProposeSyncable(r.Context(), c); err != nil {
		writeProposeError(w, err, "syncable", "propose syncable")
		return
	}

	// See database.go AddDatabase for the G705 rationale.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(c.ID)) //nolint:gosec // G705
}

func (h *HTTP) GetSyncables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Syncables()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve syncables")
		return
	}

	writeConfigurations(w, cfgs)
}
