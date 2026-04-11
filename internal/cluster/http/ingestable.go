package http

import (
	"errors"
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

func (h *HTTP) AddIngestable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid ingestable configuration")
		return
	}

	err = h.c.ProposeIngestable(r.Context(), c)
	if err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_ingestable_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose ingestable")
		}
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetIngestables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Ingestables()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve ingestables")
		return
	}

	writeConfigurations(w, cfgs)
}
