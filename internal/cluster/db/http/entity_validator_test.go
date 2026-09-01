package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
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

// TestAddType_StrandedSyncablesForceFlow pins the ?force=true contract: a
// refused nonConvertible bump renders 409 stranded_always_current with the
// stranded syncables as structured details, and the force re-POST passes the
// acknowledgment through to ProposeType.
func TestAddType_StrandedSyncablesForceFlow(t *testing.T) {
	h, fake := setupTest()
	fake.ProposeTypeReturns(&cluster.StrandedSyncablesError{
		TypeID: "orders", Version: 2, Syncables: []string{"orders-mirror"},
	})

	body := "[type]\nname = \"orders\""
	req := httptest.NewRequest("POST", "http://localhost/v1/type/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/toml")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 409, w.Result().StatusCode)
	require.Contains(t, w.Body.String(), "stranded_always_current")
	require.Contains(t, w.Body.String(), "orders-mirror", "the stranded syncables are named")

	_, _, opts := fake.ProposeTypeArgsForCall(0)
	require.Empty(t, opts, "a plain POST passes no acknowledgment")

	fake.ProposeTypeReturns(nil)
	req = httptest.NewRequest("POST", "http://localhost/v1/type/orders?force=true", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/toml")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Result().StatusCode)
	_, _, opts = fake.ProposeTypeArgsForCall(1)
	require.Len(t, opts, 1, "?force=true passes the acknowledgment option through")
}
