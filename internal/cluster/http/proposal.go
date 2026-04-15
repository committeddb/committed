package http

import (
	"encoding/json"
	"errors"
	"fmt"
	httpgo "net/http"
	"strconv"
	"time"

	"github.com/philborlin/committed/internal/cluster"
)

// schemaCacheKey identifies a compiled schema artifact by type ID +
// version. Keying by version (instead of comparing schema bytes on every
// request) is correct because Type.Version is system-assigned and bumps
// whenever the schema changes — equal versions imply equal schemas. See
// ticket type-schema-versioning Phase 1 known-risk #1.
type schemaCacheKey struct {
	id      string
	version int
}

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
		writeError(w, httpgo.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}

	// Capture wall-clock once for the whole proposal so every entity in
	// it ends up with the same timestamp. This is what makes apply
	// content-deterministic on multi-node clusters: the proposer fixes
	// the time and every node reads it back from the marshaled entry.
	ts := time.Now().UnixMilli()

	var es []*cluster.Entity
	for _, e := range pr.Entities {
		t, err := h.c.ResolveType(cluster.LatestTypeRef(e.TypeID))
		if err != nil {
			writeErrorf(w, httpgo.StatusBadRequest, "type_not_found", "type %q not found", e.TypeID)
			return
		}

		v, err := h.compiledValidator(t)
		if err != nil {
			writeErrorf(w, httpgo.StatusInternalServerError, "internal_error",
				"failed to compile schema for type %q: %s", t.ID, err)
			return
		}
		if v != nil {
			if err := v.validate(e.Data); err != nil {
				var vErr *schemaValidationError
				if errors.As(err, &vErr) {
					writeErrorWithDetails(w, httpgo.StatusBadRequest, "schema_validation_failed",
						fmt.Sprintf("entity data does not match schema for type %q", t.ID), vErr.Error())
					return
				}
				writeErrorf(w, httpgo.StatusInternalServerError, "internal_error",
					"validation error for type %q: %s", t.ID, err)
				return
			}
		}

		es = append(es, &cluster.Entity{
			Type:      t,
			Key:       []byte(e.Key),
			Data:      e.Data,
			Timestamp: ts,
		})
	}

	p := &cluster.Proposal{
		Entities: es,
	}

	err = h.c.Propose(r.Context(), p)
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose")
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
			writeErrorf(w, httpgo.StatusBadRequest, "invalid_parameter", "number parameter %q is not a valid integer", qn)
			return
		}
		amount = n
	}

	ps, err := h.c.Proposals(uint64(amount), t)
	if err != nil {
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve proposals")
		return
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

// compiledValidator returns a cached entityValidator for the given type
// or compiles one on miss. Returns (nil, nil) if the type doesn't call
// for validation (Validate != ValidateSchema, or an unrecognized
// SchemaType). The cache is keyed by (typeID, Version): Type.Version is
// system-assigned and monotonically bumps on any schema change, so a
// cache hit on (id, version) is guaranteed to reference the same schema
// bytes — no per-request byte comparison needed.
func (h *HTTP) compiledValidator(t *cluster.Type) (entityValidator, error) {
	key := schemaCacheKey{id: t.ID, version: t.Version}
	if cached, ok := h.schemas.Load(key); ok {
		return cached.(entityValidator), nil
	}

	v, err := compileValidator(t)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	h.schemas.Store(key, v)
	return v, nil
}
