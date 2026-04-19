package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddDatabase(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid database configuration")
		return
	}

	if err := h.c.ProposeDatabase(r.Context(), c); err != nil {
		writeProposeError(w, err, "database", "propose database")
		return
	}

	// text/plain defeats browser content-sniffing; the response is a
	// plain ID echoed back to the same client that POSTed the config,
	// so there's no cross-user XSS surface even with a hostile ID.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(c.ID)) //nolint:gosec // G705
}

func (h *HTTP) GetDatabases(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Databases()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve databases")
		return
	}

	writeConfigurations(w, cfgs)
}
