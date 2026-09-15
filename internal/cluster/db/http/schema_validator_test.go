package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

// TestSchemaValidator_ValidateTypeSchema pins the injected admission compiler: a
// known SchemaType with a broken schema errors, a validating type naming a
// language this binary cannot compile errors (it would validate nothing,
// silently), and a valid schema or a non-validating type passes whatever
// language it names.
func TestSchemaValidator_ValidateTypeSchema(t *testing.T) {
	sv := &http.SchemaValidator{}
	for _, tc := range []struct {
		name    string
		typ     *cluster.Type
		wantErr bool
	}{
		{"valid JSONSchema", &cluster.Type{ID: "a", Validate: cluster.ValidateSchema, SchemaType: "JSONSchema", Schema: []byte(`{"type":"object"}`)}, false},
		{"broken JSONSchema (not JSON)", &cluster.Type{ID: "b", Validate: cluster.ValidateSchema, SchemaType: "JSONSchema", Schema: []byte(`{ not json`)}, true},
		{"unknown SchemaType, validating", &cluster.Type{ID: "c", Validate: cluster.ValidateSchema, SchemaType: "Thrift", Schema: []byte(`anything`)}, true},
		{"unknown SchemaType, announcing", &cluster.Type{ID: "c2", Validate: cluster.ValidateAnnounce, SchemaType: "Thrift", Schema: []byte(`anything`)}, true},
		{"unknown SchemaType, not validating", &cluster.Type{ID: "c3", Validate: cluster.NoValidation, SchemaType: "Thrift"}, false},
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
