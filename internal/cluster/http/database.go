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
			writeError(w, httpgo.StatusBadRequest, "invalid_database_config", "invalid database configuration")
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose database")
		}
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetDatabases(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Databases()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve databases")
		return
	}

	writeConfigurations(w, cfgs)
}
