package cluster

import (
	"fmt"
	"testing"
)

// stubResolver is a test TypeResolver that returns pre-loaded Types by ID.
type stubResolver struct {
	types map[string]*Type
}

func (r *stubResolver) Type(id string) (*Type, error) {
	t, ok := r.types[id]
	if !ok {
		return nil, fmt.Errorf("type not found: %s", id)
	}
	return t, nil
}

func TestUnmarshalHydratesTypeFromResolver(t *testing.T) {
	original := &Type{
		ID:         "test-type-id",
		Name:       "TestType",
		Version:    3,
		SchemaType: "JSON Schema",
		Schema:     []byte(`{"type":"object","properties":{"name":{"type":"string"}}}`),
		Validate:   NoValidation,
	}

	p := &Proposal{
		Entities: []*Entity{
			NewUpsertEntity(original, []byte("key-1"), []byte("data-1")),
			NewUpsertEntity(original, []byte("key-2"), []byte("data-2")),
		},
	}

	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	resolver := &stubResolver{types: map[string]*Type{
		original.ID: original,
	}}

	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for i, e := range got.Entities {
		if e.Type.ID != original.ID {
			t.Errorf("entity[%d].Type.ID = %q, want %q", i, e.Type.ID, original.ID)
		}
		if e.Type.Name != original.Name {
			t.Errorf("entity[%d].Type.Name = %q, want %q", i, e.Type.Name, original.Name)
		}
		if e.Type.Version != original.Version {
			t.Errorf("entity[%d].Type.Version = %d, want %d", i, e.Type.Version, original.Version)
		}
		if e.Type.SchemaType != original.SchemaType {
			t.Errorf("entity[%d].Type.SchemaType = %q, want %q", i, e.Type.SchemaType, original.SchemaType)
		}
		if string(e.Type.Schema) != string(original.Schema) {
			t.Errorf("entity[%d].Type.Schema = %q, want %q", i, string(e.Type.Schema), string(original.Schema))
		}
		if e.Type.Validate != original.Validate {
			t.Errorf("entity[%d].Type.Validate = %d, want %d", i, e.Type.Validate, original.Validate)
		}
	}
}

func TestUnmarshalNilResolverProducesStub(t *testing.T) {
	typeID := "stub-type-id"
	p := &Proposal{
		Entities: []*Entity{
			NewUpsertEntity(&Type{ID: typeID, Name: "Full"}, []byte("k"), []byte("v")),
		},
	}

	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got := &Proposal{}
	if err := got.Unmarshal(bs, nil); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	e := got.Entities[0]
	if e.Type.ID != typeID {
		t.Errorf("Type.ID = %q, want %q", e.Type.ID, typeID)
	}
	if e.Type.Name != "" {
		t.Errorf("Type.Name = %q, want empty (stub)", e.Type.Name)
	}
}
