package http

import (
	"fmt"
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/syncable"
)

type SyncableRequest struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

func (h *HTTP) AddSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	sr := &SyncableRequest{}
	err := unmarshalBody(r, sr)
	if err != nil {
		badRequest(w, err)
		return
	}

	var s cluster.Syncable
	if sr.Type == "console" {
		s = &syncable.Console{}
	} else {
		badRequest(w, fmt.Errorf("%s is not a support type", sr.Type))
		return
	}

	err = h.c.Sync(r.Context(), sr.ID, s)
	if err != nil {
		internalServerError(w, err)
		return
	}
}
