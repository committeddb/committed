package http

import (
	"errors"
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

func (h *HTTP) AddDatabase(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid database configuration")
		return
	}

	err = h.c.ProposeDatabase(r.Context(), c)
	if err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_database_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose database")
		}
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
