package http

import (
	"encoding/json"
	"fmt"
	httpgo "net/http"

	"github.com/philborlin/committed/internal/cluster"
)

type ProposalRequest struct {
	Entities []*EntityRequest `json:"entities"`
}

type EntityRequest struct {
	TypeID string          `json:"typeId"`
	Key    string          `json:"key"`
	Data   json.RawMessage `json:"data"`
}

func (h *HTTP) AddProposal(w httpgo.ResponseWriter, r *httpgo.Request) {
	pr := &ProposalRequest{}
	err := unmarshalBody(r, pr)
	if err != nil {
		badRequest(w, err)
		return
	}

	var es []*cluster.Entity
	for _, e := range pr.Entities {
		t, err := h.c.Type(e.TypeID)
		if err != nil {
			badRequest(w, err)
			return
		}
		es = append(es, &cluster.Entity{
			Type: t,
			Key:  []byte(e.Key),
			Data: e.Data,
		})
	}

	p := &cluster.Proposal{
		Entities: es,
	}

	fmt.Printf("Proposal contained %d entries\n", len(es))

	err = h.c.Propose(p)
	if err != nil {
		internalServerError(w, err)
		return
	}
}
