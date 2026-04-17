package cluster

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
)

// TypeRef identifies a Type by the compound (ID, Version) key. A zero
// Version means "latest" — resolvers return whatever is current for the
// given ID. This sentinel never appears in caller code: construct
// TypeRefs via LatestTypeRef or TypeRefAt so the intent is visible at
// the call site.
type TypeRef struct {
	ID      string
	Version int
}

// LatestTypeRef constructs a TypeRef that resolvers interpret as "give
// me whatever version is current for this ID."
func LatestTypeRef(id string) TypeRef { return TypeRef{ID: id} }

// TypeRefAt constructs a TypeRef pinned to a specific historical
// version. Used on the apply/replay path to pair an entity with the
// schema that was in force when it was proposed.
func TypeRefAt(id string, version int) TypeRef {
	return TypeRef{ID: id, Version: version}
}

// TypeResolver looks up a full Type by (ID, Version). Implementations
// include wal.Storage (BoltDB-backed) and db.Storage (the interface).
// Passing a TypeResolver to Proposal.Unmarshal hydrates each
// Entity.Type with the complete schema metadata instead of leaving a
// stub with only ID. Lookup failures are errors, not a soft-fail: a
// type referenced by a log entry that storage doesn't know about is a
// consistency violation.
type TypeResolver interface {
	ResolveType(ref TypeRef) (*Type, error)
}

type ValidationStrategy int

const (
	NoValidation   ValidationStrategy = 0
	ValidateSchema ValidationStrategy = 1
)

type Type struct {
	ID         string
	Name       string
	Version    int
	SchemaType string // something like Thrift, Protobuf, JSON Schema, etc.
	Schema     []byte // The contents of the schema
	Validate   ValidationStrategy
	// Migration is the transform program that upgrades data written
	// against Version-1 of this type into the shape this Version
	// expects. Interpreted as a jq program against each entity's JSON
	// payload. Empty means the schema change is additive enough that
	// data doesn't need rewriting. Only applied when a syncable opts
	// into "always-current" mode.
	Migration []byte
	// MigrationExplicit is transient (not persisted). Set by ParseType
	// when the operator provided a [migration] section (either
	// transform or none=true). Used by ProposeType to enforce the
	// requirement that every version after v1 declares its migration
	// intent explicitly.
	MigrationExplicit bool
}

type TimePoint struct {
	Start time.Time
	End   time.Time
	Value uint64
}

func (t *Type) String() string {
	return fmt.Sprintf(" (%s) %s - v%d", t.ID, t.Name, t.Version)
}

var typeType = &Type{
	ID:      "268e1ac4-7d17-4798-afae-3f1f9aa6fc65",
	Name:    "InternalType",
	Version: 1,
}

func IsType(id string) bool {
	return id == typeType.ID
}

func NewUpsertTypeEntity(t *Type) (*Entity, error) {
	bs, err := t.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(typeType, []byte(t.ID), bs), nil
}

func NewDeleteTypeEntity(id string) *Entity {
	return NewDeleteEntity(typeType, []byte(id))
}

func (t *Type) Marshal() ([]byte, error) {
	lt := &clusterpb.LogType{
		ID:   t.ID,
		Name: t.Name,
		// Version and Validate are bounded by the domain: Version is
		// monotonically assigned starting at 1 (will never exceed
		// int32), and Validate has only two defined values
		// (NoValidation=0, ValidateSchema=1).
		Version:    int32(t.Version), //nolint:gosec // G115: bounded by domain
		SchemaType: t.SchemaType,
		Schema:     t.Schema,
		Validate:   clusterpb.LogValidationStrategy(t.Validate), //nolint:gosec // G115: bounded by domain
		Migration:  t.Migration,
	}

	return proto.Marshal(lt)
}

func (t *Type) Unmarshal(bs []byte) error {
	lt := &clusterpb.LogType{}
	err := proto.Unmarshal(bs, lt)
	if err != nil {
		return err
	}

	t.ID = lt.ID
	t.Name = lt.Name
	t.Version = int(lt.Version)
	t.Schema = lt.Schema
	t.SchemaType = lt.SchemaType
	t.Validate = ValidationStrategy(lt.Validate)
	t.Migration = lt.Migration

	return nil
}
