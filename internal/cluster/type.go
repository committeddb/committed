package cluster

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
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

// EntityKind declares what the entities written under a type are,
// ordered by how much interpretation a consumer needs to apply one. It
// is declarative metadata: the log stores every kind of entity
// identically; the entity kind only drives config-time validation
// (and, later, per-kind retention).
type EntityKind int

const (
	// EntityKindUnspecified is the grandfathered default: every type
	// written before the field existed, and every type that does not
	// declare an entity kind, behaves exactly like an untyped topic.
	// No enforcement ever applies to it.
	EntityKindUnspecified EntityKind = 0
	// EntityKindSnapshot entities are full objects ("tenant X is now
	// {…}"); apply = overwrite, LWW per key.
	EntityKindSnapshot EntityKind = 1
	// EntityKindDelta entities would be state-relative patches
	// ("add 3"). Hostile to at-least-once sync delivery (a redelivered
	// non-idempotent op corrupts), so ProposeType rejects it; the
	// constant exists to keep the taxonomy complete.
	EntityKindDelta EntityKind = 2
	// EntityKindEvent entities are domain facts ("tenant.provisioned
	// happened"); apply = fold via domain rules, partial and
	// implicative by design.
	EntityKindEvent EntityKind = 3
	// EntityKindCommand entities are requests ("please provision X");
	// replay is dangerous, lifecycle belongs to the consumer.
	EntityKindCommand EntityKind = 4
	// EntityKindStandalone entities are facts with no aggregate to
	// converge on (audit, telemetry); apply = append, never folded.
	EntityKindStandalone EntityKind = 5
)

// ParseEntityKind maps the TOML string form to an EntityKind. The
// empty string is EntityKindUnspecified (the field is optional);
// unknown strings return an error so typos surface at config-parse
// time rather than silently defaulting.
func ParseEntityKind(s string) (EntityKind, error) {
	switch s {
	case "":
		return EntityKindUnspecified, nil
	case "snapshot":
		return EntityKindSnapshot, nil
	case "delta":
		return EntityKindDelta, nil
	case "event":
		return EntityKindEvent, nil
	case "command":
		return EntityKindCommand, nil
	case "standalone":
		return EntityKindStandalone, nil
	}
	return 0, fmt.Errorf("unknown entity kind %q (expected \"snapshot\", \"delta\", \"event\", \"command\", or \"standalone\")", s)
}

func (k EntityKind) String() string {
	switch k {
	case EntityKindSnapshot:
		return "snapshot"
	case EntityKindDelta:
		return "delta"
	case EntityKindEvent:
		return "event"
	case EntityKindCommand:
		return "command"
	case EntityKindStandalone:
		return "standalone"
	default:
		return "unspecified"
	}
}

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
	// EntityKind declares what the entities written under this type
	// are. EntityKindUnspecified (the default) behaves exactly like
	// today — enforcement only ever applies to kinded types. Immutable
	// once declared: a version bump cannot change it (ProposeType
	// rejects).
	EntityKind EntityKind
	// Discriminator is a jsonpath (e.g. "$.event_type") naming the
	// field that distinguishes entity variants. Only valid for
	// EntityKindEvent; projection-style syncables can default their
	// match rules to it.
	Discriminator string
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

var typeType = registerSystemType(&Type{
	ID:         "268e1ac4-7d17-4798-afae-3f1f9aa6fc65",
	Name:       "InternalType",
	Version:    1,
	EntityKind: EntityKindSnapshot,
})

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
		// Version, Validate, and EntityKind are bounded by the domain:
		// Version is monotonically assigned starting at 1 (will never
		// exceed int32), Validate has only two defined values
		// (NoValidation=0, ValidateSchema=1), and EntityKind only the
		// six EntityKind constants (ParseEntityKind rejects anything
		// else).
		Version:       int32(t.Version), //nolint:gosec // G115: bounded by domain
		SchemaType:    t.SchemaType,
		Schema:        t.Schema,
		Validate:      clusterpb.LogValidationStrategy(t.Validate), //nolint:gosec // G115: bounded by domain
		Migration:     t.Migration,
		EntityKind:    clusterpb.LogEntityKind(t.EntityKind), //nolint:gosec // G115: bounded by domain
		Discriminator: t.Discriminator,
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
	t.EntityKind = EntityKind(lt.EntityKind)
	t.Discriminator = lt.Discriminator

	return nil
}
