package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/http"
)

// TestValidateEntityData pins the tripwire's validator seam: conformant and
// non-validating payloads report nothing, a divergent payload reports
// structured causes (path + keyword + message), and an unknown SchemaType
// fails open — all without gating semantics.
func TestValidateEntityData(t *testing.T) {
	sv := &http.SchemaValidator{}
	announce := &cluster.Type{
		ID: "photo-meta", Name: "PhotoMeta", Version: 2,
		Validate: cluster.ValidateAnnounce, SchemaType: "JSONSchema",
		Schema: []byte(`{"type":"object","properties":{"caption":{"type":"string"},"size":{"type":"number"}},"additionalProperties":false}`),
	}

	// Conformant → no divergence.
	div, err := sv.ValidateEntityData(announce, []byte(`{"caption":"a","size":3}`))
	require.NoError(t, err)
	require.Nil(t, div)

	// Divergent: an added property AND a type mismatch, each a structured cause.
	div, err = sv.ValidateEntityData(announce, []byte(`{"caption":7,"ai_labels":{"x":1}}`))
	require.NoError(t, err)
	require.NotNil(t, div)
	keywords := map[string]string{}
	for _, c := range div.Causes {
		keywords[c.Keyword] = c.Path
		require.NotEmpty(t, c.Message)
	}
	require.Contains(t, keywords, "additionalProperties", "the added path must surface: %v", div.Causes)
	require.Contains(t, keywords, "type", "the type mismatch must surface: %v", div.Causes)
	require.Equal(t, "/caption", keywords["type"], "the mismatch names its instance path")

	// A non-validating type and an unknown SchemaType both report nothing
	// (fail-open, symmetric with ValidateTypeSchema).
	div, err = sv.ValidateEntityData(&cluster.Type{ID: "plain"}, []byte(`{}`))
	require.NoError(t, err)
	require.Nil(t, div)
	div, err = sv.ValidateEntityData(&cluster.Type{ID: "thrift", Validate: cluster.ValidateAnnounce, SchemaType: "Thrift", Schema: []byte("x")}, []byte(`{}`))
	require.NoError(t, err)
	require.Nil(t, div)

	// Malformed payload JSON is a divergence-shaped report from the
	// underlying validator (well-formedness is part of the JSONSchema
	// contract), not a structural error that would abort the tripwire.
	div, err = sv.ValidateEntityData(announce, []byte(`{ not json`))
	require.NoError(t, err)
	require.NotNil(t, div)
}

// TestAddProposal_AnnounceCommitsDivergentPayload pins the signal-not-reject
// rule on the direct-proposal path: a divergent payload under an
// announce-typed topic is PROPOSED (the tripwire in db handles announcing),
// while the same payload under a strict type stays a 400.
func TestAddProposal_AnnounceCommitsDivergentPayload(t *testing.T) {
	schema := `{"type":"object","properties":{"caption":{"type":"string"}},"additionalProperties":false}`
	divergent := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"caption": 7}}]}`

	// Announce: divergence flows through to Propose.
	h, fake := setupTest()
	fake.ResolveTypeReturns(&cluster.Type{
		ID: "t1", Name: "T", Validate: cluster.ValidateAnnounce,
		SchemaType: "JSONSchema", Schema: []byte(schema),
	}, nil)
	req := httptest.NewRequest("POST", "http://localhost/v1/proposal", strings.NewReader(divergent))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Result().StatusCode, "announce never gates: %s", w.Body.String())
	require.Equal(t, 1, fake.ProposeCallCount(), "the divergent payload must still be proposed")

	// Strict: the same divergence is still rejected at the gate.
	h, fake = setupTest()
	fake.ResolveTypeReturns(&cluster.Type{
		ID: "t1", Name: "T", Validate: cluster.ValidateSchema,
		SchemaType: "JSONSchema", Schema: []byte(schema),
	}, nil)
	req = httptest.NewRequest("POST", "http://localhost/v1/proposal", strings.NewReader(divergent))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 400, w.Result().StatusCode, "strict still gates")
	require.Zero(t, fake.ProposeCallCount())
}
