package http

import (
	"encoding/json"
	"fmt"
	httpgo "net/http"
	"strconv"

	"github.com/philborlin/committed/internal/cluster"
)

type AddProposalRequest struct {
	Entities []*AddEntityRequest `json:"entities"`
}

type AddEntityRequest struct {
	TypeID string          `json:"typeId"`
	Key    string          `json:"key"`
	Data   json.RawMessage `json:"data"`
}

func (h *HTTP) AddProposal(w httpgo.ResponseWriter, r *httpgo.Request) {
	pr := &AddProposalRequest{}
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

type GetProposalResponse struct {
	Entities []*GetProposalEntityResponse `json:"entities"`
}

type GetProposalEntityResponse struct {
	TypeID   string `json:"typeId"`
	TypeName string `json:"typeName"`
	Key      string `json:"key"`
	Data     string `json:"data"`
}

type GetProposalTypeResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (h *HTTP) GetProposals(w httpgo.ResponseWriter, r *httpgo.Request) {
	t := r.URL.Query().Get("type")

	amount := 10
	qn := r.URL.Query().Get("number")
	if qn != "" {
		n, err := strconv.Atoi(qn)
		if err != nil {
			badRequest(w, err)
		}
		amount = n
	}

	ps, err := h.c.Proposals(uint64(amount), t)
	if err != nil {
		internalServerError(w, err)
	}

	var body []GetProposalResponse
	for _, p := range ps {
		proposalResponse := &GetProposalResponse{}
		for _, e := range p.Entities {
			proposalEntityResponse := &GetProposalEntityResponse{
				TypeID:   e.Type.ID,
				TypeName: e.Type.Name,
				Key:      string(e.Key),
				Data:     string(e.Data),
			}
			proposalResponse.Entities = append(proposalResponse.Entities, proposalEntityResponse)
		}
		body = append(body, *proposalResponse)
	}

	writeArrayBody(w, body)
}
