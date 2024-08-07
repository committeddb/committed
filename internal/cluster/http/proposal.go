package http

import (
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

type ProposalRequest struct {
	Entities []*EntityRequest
}

type EntityRequest struct {
	TypeID string `json:"typeId"`
	Key    string `json:"key"`
	Data   string `json:"data"`
}

func (h *HTTP) AddProposal(w httpgo.ResponseWriter, r *httpgo.Request) {
	var tr ProposalRequest
	err := unmarshalBody(r, tr)
	if err != nil {
		badRequest(w, err)
		return
	}

	var es []*cluster.Entity
	for _, e := range tr.Entities {
		t, err := h.c.Type(e.TypeID)
		if err != nil {
			internalServerError(w, err)
			return
		}
		es = append(es, &cluster.Entity{
			Type: t,
			Key:  []byte(e.Key),
			Data: []byte(e.Data),
		})
	}

	p := &cluster.Proposal{
		Entities: es,
	}

	h.c.Propose(p)
}
