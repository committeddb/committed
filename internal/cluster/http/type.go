package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddType(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		badRequest(w, err)
		return
	}

	err = h.c.ProposeType(c)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetTypes(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Types()
	if err != nil {
		badRequest(w, err)
		return
	}

	writeConfigurations(w, cfgs)
}
