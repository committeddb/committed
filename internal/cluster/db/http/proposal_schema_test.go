package http

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Legacy-schema legs of the proposal gate. These type shapes cannot be
// admitted through the real engine — ProposeType rejects an uncompilable
// or incomplete schema at admission — but types created BEFORE those checks
// shipped can still sit in a log, so the proposal path must handle them:
// an uncompilable schema is a permanent 422 (never a 500), and incomplete
// validation config fails open (skip, don't gate). Pinned here directly
// against compiledValidator and the 422 writer, since there is no seam to
// inject a legacy type through the concrete engine.

func legacyType(name, schemaType string, schema []byte) *cluster.Type {
	return &cluster.Type{
		ID: "legacy", Name: name, Version: 1,
		SchemaType: schemaType, Schema: schema,
		Validate: cluster.ValidateSchema,
	}
}

// TestCompiledValidator_LegacyBrokenSchemasError: every uncompilable legacy
// shape surfaces as a compile error (the handler's 422), never a panic and
// never a silent pass.
func TestCompiledValidator_LegacyBrokenSchemasError(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  *cluster.Type
	}{
		{"proto source that does not parse", legacyType("Person", "Protobuf", []byte("this is not proto source {{{"))},
		{"proto without the named message", legacyType("Person", "Protobuf", []byte(`syntax = "proto3"; message Other { string x = 1; }`))},
		{"JSON schema that is not JSON", legacyType("Bad", "JSONSchema", []byte("not valid json {{{"))},
		{"empty schema", legacyType("Bad", "JSONSchema", nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &HTTP{}
			_, err := h.compiledValidator(tc.typ)
			require.Error(t, err)
		})
	}
}

// TestCompiledValidator_LegacyIncompleteConfigFailsOpen: a validating type
// with no schema language at all skips validation (nil validator, no error)
// — the fail-open half of the legacy contract.
func TestCompiledValidator_LegacyIncompleteConfigFailsOpen(t *testing.T) {
	typ := &cluster.Type{
		ID: "legacy", Name: "Legacy", Version: 1,
		Schema:   []byte(`{"type":"object"}`),
		Validate: cluster.ValidateSchema, // SchemaType absent
	}
	h := &HTTP{}
	v, err := h.compiledValidator(typ)
	require.NoError(t, err)
	require.Nil(t, v, "no schema language means no gate — fail open, not closed")
}

// TestCompiledValidator_CachesByVersion: the same (typeID, version) resolves
// to the same compiled validator instance — the compile happens once.
func TestCompiledValidator_CachesByVersion(t *testing.T) {
	typ := legacyType("Person", "JSONSchema", []byte(`{"type":"object"}`))
	h := &HTTP{}
	v1, err := h.compiledValidator(typ)
	require.NoError(t, err)
	v2, err := h.compiledValidator(typ)
	require.NoError(t, err)
	require.Same(t, v1, v2, "same (id, version) must serve the cached validator")
}

// TestWriteSchemaCompileError pins the permanent-config mapping: 422
// type_schema_invalid naming the type, never a retryable-looking 500.
func TestWriteSchemaCompileError(t *testing.T) {
	w := httptest.NewRecorder()
	writeSchemaCompileError(w, "legacy", fmt.Errorf("compile failed"))
	require.Equal(t, 422, w.Code)
	require.Contains(t, w.Body.String(), "type_schema_invalid")
	require.Contains(t, w.Body.String(), `\"legacy\"`)
}
