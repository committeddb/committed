package http

import (
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

type TypeRequest struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func (h *HTTP) AddType(w httpgo.ResponseWriter, r *httpgo.Request) {
	var tr TypeRequest
	err := unmarshalBody(r, tr)
	if err != nil {
		badRequest(w, err)
		return
	}

	t := &cluster.Type{
		ID:      tr.ID,
		Name:    tr.Name,
		Version: tr.Version,
	}

	err = h.c.ProposeType(t)
	if err != nil {
		internalServerError(w, err)
		return
	}
}
