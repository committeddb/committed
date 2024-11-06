package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		badRequest(w, err)
		return
	}

	err = h.c.ProposeSyncable(c)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetSyncables(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Syncables()
	if err != nil {
		badRequest(w, err)
		return
	}

	writeConfigurations(w, cfgs)
}
