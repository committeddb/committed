package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddDatabase(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(w, r)
	if err != nil {
		badRequest(w, err)
		return
	}

	err = h.c.ProposeDatabase(c)
	if err != nil {
		internalServerError(w, err)
	}
}

func (h *HTTP) GetDatabases(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Databases()
	if err != nil {
		badRequest(w, err)
		return
	}

	writeConfigurations(w, cfgs)
}
