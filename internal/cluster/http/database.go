package http

import (
	"fmt"
	httpgo "net/http"
)

func (h *HTTP) AddDatabase(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		badRequest(w, err)
		return
	}

	fmt.Printf("[http] ProposeDatabase\n")
	err = h.c.ProposeDatabase(c)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Write([]byte(c.ID))
}

func (h *HTTP) GetDatabases(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Databases()
	if err != nil {
		badRequest(w, err)
		return
	}

	writeConfigurations(w, cfgs)
}
