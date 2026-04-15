package cluster

import (
	"fmt"
	"strings"
	"testing"
)

// stubResolver is a test TypeResolver that returns pre-loaded Types.
// `types` is keyed by ID for latest lookups (ref.Version == 0);
// `versions` is keyed by "id@version" for exact-version lookups.
// Missing entries return a not-found error — matching production
// semantics, which the unmarshal path now surfaces instead of hiding.
type stubResolver struct {
	types    map[string]*Type
	versions map[string]*Type
}

func (r *stubResolver) ResolveType(ref TypeRef) (*Type, error) {
	if ref.Version > 0 {
		if r.versions != nil {
			if t, ok := r.versions[fmt.Sprintf("%s@%d", ref.ID, ref.Version)]; ok {
				return t, nil
			}
		}
		return nil, fmt.Errorf("type %s version %d not found", ref.ID, ref.Version)
	}
	t, ok := r.types[ref.ID]
	if !ok {
		return nil, fmt.Errorf("type not found: %s", ref.ID)
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

	resolver := &stubResolver{
		types: map[string]*Type{original.ID: original},
		versions: map[string]*Type{
			fmt.Sprintf("%s@%d", original.ID, original.Version): original,
		},
	}

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

// Unmarshal requires a TypeResolver. A nil resolver indicates a
// programming bug in the caller, not a recoverable condition — return
// an error rather than papering over it with stub Types.
func TestUnmarshalNilResolverIsError(t *testing.T) {
	p := &Proposal{
		Entities: []*Entity{NewUpsertEntity(&Type{ID: "t1"}, []byte("k"), []byte("v"))},
	}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got := &Proposal{}
	err = got.Unmarshal(bs, nil)
	if err == nil {
		t.Fatal("Unmarshal with nil resolver: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "TypeResolver is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// When an entity is stamped with a TypeVersion, Unmarshal hydrates the
// entity via TypeAtVersion(id, version), not Type(id). This is the
// invariant that "an old proposal is always paired with the schema
// that was current when it was proposed".
func TestUnmarshalUsesStampedVersion(t *testing.T) {
	v1 := &Type{ID: "t1", Name: "Person", Version: 1, Schema: []byte(`{"v":1}`)}
	v2 := &Type{ID: "t1", Name: "Person", Version: 2, Schema: []byte(`{"v":2}`)}

	p := &Proposal{
		Entities: []*Entity{
			{Type: v1, Key: []byte("k"), Data: []byte("d")},
		},
	}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	resolver := &stubResolver{
		types: map[string]*Type{"t1": v2}, // latest is v2
		versions: map[string]*Type{
			"t1@1": v1,
			"t1@2": v2,
		},
	}

	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Entities[0].Type.Version != 1 {
		t.Errorf("hydrated Type.Version = %d, want 1 (the stamped version, not latest)", got.Entities[0].Type.Version)
	}
	if string(got.Entities[0].Type.Schema) != `{"v":1}` {
		t.Errorf("hydrated Schema = %q, want v1's schema", string(got.Entities[0].Type.Schema))
	}
}

// A Type.Version of 0 on the wire means "pre-versioning entry" — there
// was no stamp, so the resolver falls back to the latest version. New
// proposals all stamp a non-zero version, so this path is only hit by
// entries written before the type-schema-versioning ticket landed.
func TestUnmarshalZeroVersionFallsBackToLatest(t *testing.T) {
	latest := &Type{ID: "t1", Name: "Person", Version: 5}
	p := &Proposal{
		Entities: []*Entity{
			// No Version on Type → wire TypeVersion is 0.
			{Type: &Type{ID: "t1"}, Key: []byte("k"), Data: []byte("d")},
		},
	}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	resolver := &stubResolver{types: map[string]*Type{"t1": latest}}

	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Entities[0].Type.Version != 5 {
		t.Errorf("fallback Type.Version = %d, want 5 (latest)", got.Entities[0].Type.Version)
	}
}

// Resolver failures surface as Unmarshal errors. A type referenced by a
// log entry that storage doesn't know about is a consistency violation,
// not a soft-fail case.
func TestUnmarshalSurfacesResolverErrors(t *testing.T) {
	p := &Proposal{
		Entities: []*Entity{
			{Type: &Type{ID: "missing", Version: 3}, Key: []byte("k"), Data: []byte("d")},
		},
	}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	resolver := &stubResolver{} // knows nothing

	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err == nil {
		t.Fatal("expected Unmarshal to fail when the resolver has no record of the type")
	}
}
