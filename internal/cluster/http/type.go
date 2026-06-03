package http

import (
	httpgo "net/http"
	"time"
)

// AddType (POST /type/{id}) and GetTypes (GET /type) are served by the
// generic config handlers in config_handlers.go. GetType below is bespoke:
// GET /type/{id} returns a time-series graph, not a configuration.

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
