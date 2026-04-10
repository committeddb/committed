package http

import (
	"errors"
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

func (h *HTTP) AddSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid syncable configuration")
		return
	}

	err = h.c.ProposeSyncable(r.Context(), c)
	if err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_syncable_config", "invalid syncable configuration")
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose syncable")
		}
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetSyncables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Syncables()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve syncables")
		return
	}

	writeConfigurations(w, cfgs)
}
