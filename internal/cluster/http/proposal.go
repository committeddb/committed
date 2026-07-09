package http

import (
	"encoding/json"
	"errors"
	"fmt"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
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
	// Delete issues a tombstone for (typeId, key) instead of an upsert.
	// A delete carries no payload: Data is ignored, schema validation is
	// skipped (there is nothing to validate), and the syncable removes the
	// downstream record keyed by Key. This is the intake half of
	// right-to-be-forgotten — see the cluster.Syncable contract.
	Delete bool `json:"delete"`
}

func (h *HTTP) AddProposal(w httpgo.ResponseWriter, r *httpgo.Request) {
	pr := &AddProposalRequest{}
	err := unmarshalBody(r, pr)
	if err != nil {
		h.writeReadError(w, r, err, "invalid_json", "request body is not valid JSON")
		return
	}

	var es []*cluster.Entity
	for _, e := range pr.Entities {
		t, err := h.c.ResolveType(cluster.LatestTypeRef(e.TypeID))
		if err != nil {
			writeErrorf(w, httpgo.StatusBadRequest, "type_not_found", "type %q not found", e.TypeID)
			return
		}

		// A delete has no payload, so there is nothing to validate against
		// the schema; build the tombstone entity and move on.
		if e.Delete {
			es = append(es, cluster.NewDeleteEntity(t, []byte(e.Key)))
			continue
		}

		v, err := h.compiledValidator(t)
		if err != nil {
			writeInternalError(w, fmt.Sprintf("failed to compile schema for type %q", t.ID), err)
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
				writeInternalError(w, fmt.Sprintf("validation error for type %q", t.ID), err)
				return
			}
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

	err = h.c.Propose(r.Context(), p)
	if err != nil {
		if errors.Is(err, cluster.ErrProposalTooLarge) {
			writeError(w, httpgo.StatusRequestEntityTooLarge, "proposal_too_large", "proposal exceeds the configured size limit")
			return
		}
		if errors.Is(err, cluster.ErrInsufficientStorage) {
			writeError(w, httpgo.StatusInsufficientStorage, "insufficient_storage",
				"the cluster (or this node) is low on disk space and is rejecting writes; see GET /v1/node/status disk.admission, retry once disk space recovers")
			return
		}
		writeInternalError(w, "failed to propose", err)
		return
	}
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
