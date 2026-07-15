package cluster

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
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

// TestIsInternal_HidesEveryBuiltInFromSyncables locks in the single predicate
// that keeps committed's control plane out of the syncable projection stream.
// It must cover EVERY built-in type — notably the ingestable config and
// position, which an earlier "hidden from listing" flag (since removed) did
// NOT cover, and which is why classification is membership in the systemTypes
// registry rather than a per-type flag. A user-defined topic type must NOT be
// internal.
func TestIsInternal_HidesEveryBuiltInFromSyncables(t *testing.T) {
	builtins := map[string]string{
		"type config":          typeType.ID,
		"database config":      databaseType.ID,
		"syncable config":      syncableType.ID,
		"ingestable config":    ingestableType.ID,
		"ingestable position":  ingestablePositionType.ID,
		"syncable index":       syncableIndexType.ID,
		"syncable dead-letter": syncableDeadLetterType.ID,
		"syncable stuck":       syncableStuckType.ID,
		"syncable skip":        syncableSkipRequestType.ID,
		"scrub":                scrubType.ID,
		"node api url":         nodeAPIURLType.ID,
		"migration dl":         typeMigrationDeadLetterType.ID,
	}
	for name, id := range builtins {
		if !IsInternal(id) {
			t.Errorf("%s (%s) must be internal so the syncable reader hides it", name, id)
		}
	}

	// A user-defined topic type flows through to syncables.
	if IsInternal("a-user-defined-topic-type") {
		t.Error("user-defined topic types must not be classified internal")
	}
}

// TestSystemTypesDeclareKindAndTombstonability locks in two things about every
// built-in (system) type that drive the event-log scrubber's system-tombstone
// (metadata GC) path:
//
//   - It must declare a deliberate EntityKind, never the Unspecified default —
//     otherwise a new built-in would silently never be classified.
//   - IsSystemTombstonable must match the type's nature. Keep-latest-per-key
//     compaction is sound only for EntityKindSnapshot — full states whose
//     superseded copies are disposable, keyed so the event-log key uniquely
//     identifies the record. The only built-ins excluded are the version-stored
//     configs (type/database/syncable/ingestable), which are EntityKindRevision —
//     every version is retained for rollback and to keep the log self-describing.
//     (The dead-letter logs are now Snapshot too: their upsert and delete keys
//     were reshaped to the full id+index identity, so keep-latest is correct.)
func TestSystemTypesDeclareKindAndTombstonability(t *testing.T) {
	if len(systemTypes) == 0 {
		t.Fatal("systemTypes registry is empty; the package init wiring is broken")
	}
	// The only built-ins the metadata-GC scrubber must NOT compact: the retained
	// version-stored configs (EntityKindRevision).
	notTombstonable := map[string]bool{
		typeType.ID:       true,
		databaseType.ID:   true,
		syncableType.ID:   true,
		ingestableType.ID: true,
	}
	for id, tp := range systemTypes {
		if tp.EntityKind == EntityKindUnspecified {
			t.Errorf("system type %q (%s) must declare a deliberate EntityKind, not the Unspecified default", tp.Name, id)
		}
		want := !notTombstonable[id]
		if got := IsSystemTombstonable(id); got != want {
			t.Errorf("system type %q (%s): IsSystemTombstonable = %v, want %v (kind %q)",
				tp.Name, id, got, want, tp.EntityKind)
		}
	}
}

// TestRefreshGenerationRoundTrip proves the reconciling-refresh fields
// (Entity.Generation and the RefreshBoundary marker) survive Marshal →
// Unmarshal, that a marker is neither a delete nor row data, and that an
// ordinary upsert stays a non-marker gen-0 entity. This is the wire contract
// the SQL sink and webhook then build on.
func TestRefreshGenerationRoundTrip(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1, EntityKind: EntityKindSnapshot}

	upsert := NewUpsertEntity(tp, []byte("k1"), []byte("d1"))
	upsert.Generation = 7 // stamped by the ingest worker at emit time
	del := NewDeleteEntity(tp, []byte("k2"))
	del.Generation = 7
	marker := NewRefreshBoundaryEntity(tp, 7)

	p := &Proposal{Entities: []*Entity{upsert, del, marker}}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	resolver := &stubResolver{
		types:    map[string]*Type{tp.ID: tp},
		versions: map[string]*Type{fmt.Sprintf("%s@%d", tp.ID, tp.Version): tp},
	}
	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Entities) != 3 {
		t.Fatalf("got %d entities, want 3", len(got.Entities))
	}

	gu, gd, gm := got.Entities[0], got.Entities[1], got.Entities[2]

	// Upsert: generation preserved; not a marker, not a delete.
	if gu.Generation != 7 {
		t.Errorf("upsert Generation = %d, want 7", gu.Generation)
	}
	if gu.IsRefreshBoundary() || gu.IsDelete() {
		t.Errorf("upsert: IsRefreshBoundary=%v IsDelete=%v, want both false", gu.IsRefreshBoundary(), gu.IsDelete())
	}

	// Delete: generation preserved and still a delete (marker orthogonal).
	if gd.Generation != 7 {
		t.Errorf("delete Generation = %d, want 7", gd.Generation)
	}
	if !gd.IsDelete() || gd.IsRefreshBoundary() {
		t.Errorf("delete: IsDelete=%v IsRefreshBoundary=%v, want true/false", gd.IsDelete(), gd.IsRefreshBoundary())
	}

	// Marker: carries the epoch, is a boundary, is NOT a delete, and carries no row.
	if !gm.IsRefreshBoundary() {
		t.Error("marker: IsRefreshBoundary = false, want true")
	}
	if gm.IsDelete() {
		t.Error("marker: IsDelete = true, want false (a marker carries no Data sentinel)")
	}
	if gm.Generation != 7 {
		t.Errorf("marker Generation = %d, want 7 (the sweep epoch)", gm.Generation)
	}
	if len(gm.Key) != 0 || len(gm.Data) != 0 {
		t.Errorf("marker carried Key=%q Data=%q, want both empty", gm.Key, gm.Data)
	}
	if gm.Type.ID != tp.ID {
		t.Errorf("marker Type.ID = %q, want %q (names the topic to sweep)", gm.Type.ID, tp.ID)
	}
}

// TestZeroGenerationWireBackCompatible proves the new fields cost nothing on
// the wire when unused and that a pre-feature log entry (no fields 5/6) decodes
// cleanly. proto3 omits zero-valued scalars, so an ordinary upsert must
// marshal byte-identically to a LogProposal built without ever touching
// Generation/RefreshBoundary — i.e. old readers ignore the new tags and old
// bytes still round-trip to gen 0 / non-marker.
func TestZeroGenerationWireBackCompatible(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}

	// An ordinary upsert with the new fields left at their zero values.
	p := &Proposal{Entities: []*Entity{NewUpsertEntity(tp, []byte("k"), []byte("d"))}}
	got, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// The equivalent "pre-feature" wire form: a LogProposal that never sets
	// fields 5/6. If our Marshal emitted a zero generation or marker tag, these
	// would differ.
	want, err := proto.Marshal(&clusterpb.LogProposal{
		LogEntities: []*clusterpb.LogEntity{{
			Type: &clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)},
			Key:  []byte("k"),
			Data: []byte("d"),
		}},
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("zero-generation upsert is not wire-identical to a pre-feature entry:\n got  %x\n want %x", got, want)
	}

	// And a pre-feature entry decodes to gen 0 / non-marker via our Unmarshal.
	resolver := &stubResolver{
		types:    map[string]*Type{tp.ID: tp},
		versions: map[string]*Type{fmt.Sprintf("%s@%d", tp.ID, tp.Version): tp},
	}
	dec := &Proposal{}
	if err := dec.Unmarshal(want, resolver); err != nil {
		t.Fatalf("Unmarshal pre-feature bytes: %v", err)
	}
	e := dec.Entities[0]
	if e.Generation != 0 || e.IsRefreshBoundary() {
		t.Errorf("pre-feature entry decoded to Generation=%d RefreshBoundary=%v, want 0/false", e.Generation, e.IsRefreshBoundary())
	}
}
