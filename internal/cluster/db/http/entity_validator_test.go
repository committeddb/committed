package http_test

import (
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

// TestAddType_StrandedSyncablesForceFlow pins the ?force=true contract, real
// end to end: a nonConvertible version bump with an always-current consumer
// standing is refused 409 stranded_always_current naming the consumer, and
// the ?force=true re-POST acknowledges the stranding and commits.
func TestAddType_StrandedSyncablesForceFlow(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addAlwaysCurrentRecorder(t, "rec-1", "photos")

	bump := "[type]\nname = \"photos\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n[migration]\nnonConvertible = true\n"
	w := e.doTOML(t, "POST", "/v1/type/photos", bump)
	requireEnvelope(t, w, 409, "stranded_always_current")
	require.Contains(t, w.Body.String(), "rec-1", "the refusal names the stranded consumer")

	w = e.doTOML(t, "POST", "/v1/type/photos?force=true", bump)
	require.Equal(t, 200, w.Code, w.Body.String())
}
