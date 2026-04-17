package http

import (
	"errors"
	httpgo "net/http"
	"time"

	"github.com/philborlin/committed/internal/cluster"
)

func (h *HTTP) AddType(w httpgo.ResponseWriter, r *httpgo.Request) {
	c, err := createConfiguration(r)
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_config", "invalid type configuration")
		return
	}

	err = h.c.ProposeType(r.Context(), c)
	if err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_type_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose type")
		}
		return
	}

	_, _ = w.Write([]byte(c.ID))
}

func (h *HTTP) GetTypes(w httpgo.ResponseWriter, r *httpgo.Request) {
	cfgs, err := h.c.Types()
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve types")
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
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "start parameter is not a valid timestamp")
		return
	}
	tr.Start = start

	end, err := time.Parse("2006-01-02T15:04:05Z0700", r.URL.Query().Get("end"))
	if err != nil {
		writeError(w, httpgo.StatusBadRequest, "invalid_parameter", "end parameter is not a valid timestamp")
		return
	}
	tr.End = end

	ps, err := h.c.TypeGraph(id, tr.Start, tr.End)
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve type graph")
		return
	}

	var resp []GetTypeGraphResponse
	for _, p := range ps {
		resp = append(resp, GetTypeGraphResponse{Time: p.End, Value: p.Value})
	}

	writeArrayBody(w, resp)
}
