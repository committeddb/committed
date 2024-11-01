package http

import (
	httpgo "net/http"
)

func (h *HTTP) AddIngestable(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(w, r)
	if err != nil {
		badRequest(w, err)
		return
	}

	err = h.c.ProposeIngestable(c)
	if err != nil {
		internalServerError(w, err)
	}
}
