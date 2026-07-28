package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/http"
)

// TestSchemaValidator_ValidateTypeSchema pins the injected admission compiler: a
// known SchemaType with a broken schema errors; a valid schema, a non-validating
// type, and an UNKNOWN SchemaType (fail-open for forward-compat) all pass.
func TestSchemaValidator_ValidateTypeSchema(t *testing.T) {
	sv := http.SchemaValidator{}
	for _, tc := range []struct {
		name    string
		typ     *cluster.Type
		wantErr bool
	}{
		{"valid JSONSchema", &cluster.Type{ID: "a", Validate: cluster.ValidateSchema, SchemaType: "JSONSchema", Schema: []byte(`{"type":"object"}`)}, false},
		{"broken JSONSchema (not JSON)", &cluster.Type{ID: "b", Validate: cluster.ValidateSchema, SchemaType: "JSONSchema", Schema: []byte(`{ not json`)}, true},
		{"unknown SchemaType fails open", &cluster.Type{ID: "c", Validate: cluster.ValidateSchema, SchemaType: "Thrift", Schema: []byte(`anything`)}, false},
		{"non-validating type", &cluster.Type{ID: "d", Validate: cluster.NoValidation}, false},
		{"valid Protobuf", &cluster.Type{ID: "e", Name: "M", Validate: cluster.ValidateSchema, SchemaType: "Protobuf", Schema: []byte("syntax=\"proto3\";\nmessage M { int32 x = 1; }")}, false},
		{"broken Protobuf (syntax)", &cluster.Type{ID: "f", Name: "M", Validate: cluster.ValidateSchema, SchemaType: "Protobuf", Schema: []byte("syntax=\"proto3\";\nmessage M { int32 ; }")}, true},
		{"Protobuf missing Name", &cluster.Type{ID: "g", Name: "", Validate: cluster.ValidateSchema, SchemaType: "Protobuf", Schema: []byte("syntax=\"proto3\";\nmessage M {}")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sv.ValidateTypeSchema(tc.typ)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAddProposal_BrokenTypeSchemaIs422: a proposal to a type whose schema won't
// compile — a state only reachable for a type created before the admission check
// shipped — fails with a permanent 422, not a retryable 500 that would loop a
// retrying client forever.
func TestAddProposal_BrokenTypeSchemaIs422(t *testing.T) {
	h, fake := setupTest()
	fake.ResolveTypeReturns(&cluster.Type{
		ID: "t1", Name: "Bad", Validate: cluster.ValidateSchema,
		SchemaType: "JSONSchema", Schema: []byte(`{ not valid json`),
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"foo": "bar"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/v1/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 422, w.Result().StatusCode, "a broken type schema is permanent config-shaped, not a 500")
	require.Zero(t, fake.ProposeCallCount(), "nothing is proposed when the type's schema can't compile")
}
