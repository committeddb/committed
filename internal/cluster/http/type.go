package http

import (
	"fmt"
	httpgo "net/http"
	"time"
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
		internalServerError(w, err)
		return
	}

	writeConfigurations(w, cfgs)
}

type GetTypeGraphRequest struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type GetTypeGraphResponse struct {
	Time  time.Time `json:"x"`
	Value uint64    `json:"y"`
}

func (h *HTTP) GetType(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")

	tr := &GetTypeGraphRequest{}
	start, err := time.Parse("2006-01-02T15:04:05Z0700", r.URL.Query().Get("start"))
	if err != nil {
		badRequest(w, err)
		return
	}
	tr.Start = start

	end, err := time.Parse("2006-01-02T15:04:05Z0700", r.URL.Query().Get("end"))
	if err != nil {
		badRequest(w, err)
		return
	}
	tr.End = end

	ps, err := h.c.TypeGraph(id, tr.Start, tr.End)
	if err != nil {
		fmt.Printf("TypeGraph %v - %v", id, err)
		internalServerError(w, err)
		return
	}

	var resp []GetTypeGraphResponse
	for _, p := range ps {
		resp = append(resp, GetTypeGraphResponse{Time: p.End, Value: p.Value})
	}

	writeArrayBody(w, resp)
}
