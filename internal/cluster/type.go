package cluster

import (
	"bytes"
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
	// ValidateAnnounce is the tripwire, never a gate: a divergent payload
	// still commits (a non-conformant CDC row is a true fact about the
	// source), and the first occurrence of each distinct divergent shape
	// emits a ContractExtension event to the Type's SchemaChangeTopic.
	ValidateAnnounce ValidationStrategy = 2
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
	// EntityKindRevision entities are full states in a retained, ordered
	// series — like EntityKindSnapshot (self-contained, no folding), but
	// every prior version is kept and individually addressable (roll back,
	// read "as of"). The latest is current; the history is part of the data.
	// So unlike Snapshot — whose superseded copies are disposable and get
	// compacted away — a Revision's predecessors are never compacted. This is
	// the shape of committed's own versioned configs (type/database/syncable/
	// ingestable, with their rollback endpoints) and of any user data kept as
	// a revision history rather than just a current value.
	EntityKindRevision EntityKind = 6
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
	case "revision":
		return EntityKindRevision, nil
	}
	return 0, fmt.Errorf("unknown entity kind %q (expected \"snapshot\", \"delta\", \"event\", \"command\", \"standalone\", or \"revision\")", s)
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
	case EntityKindRevision:
		return "revision"
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
	// SchemaChangeTopic is the Type ID of the topic that receives
	// ContractExtension events when data claiming this type diverges from
	// its schema under ValidateAnnounce. Type-level so both arrival paths
	// (CDC ingest, direct proposals) announce through one knob. Empty for
	// non-announce types.
	SchemaChangeTopic string
	// NonConvertible declares the third migration intent: this version
	// requires information the previous version's actuals never contained,
	// so no program can upgrade old data. Always-current syncables are
	// refused across the break; the migration chain dead-letters old-stamped
	// entities at it instead of silently delivering unconverted data.
	NonConvertible bool
	// MigrationExplicit is transient (not persisted). Set by ParseType
	// when the operator provided a [migration] section (either
	// transform or none=true). Used by ProposeType to enforce the
	// requirement that every version after v1 declares its migration
	// intent explicitly.
	MigrationExplicit bool
}

// ProposeTypeOption adjusts type admission (see Cluster.ProposeType).
type ProposeTypeOption func(*ProposeTypeOptions)

// ProposeTypeOptions is the resolved option set. Callers use the With-style
// constructors; implementations fold the options with ResolveProposeTypeOptions.
type ProposeTypeOptions struct {
	// AcknowledgeStranded admits a nonConvertible version bump although it
	// strands always-current syncables — the operator's deliberate
	// acknowledgment that those consumers' promise is being broken.
	AcknowledgeStranded bool
}

// AcknowledgeStrandedSyncables acknowledges that a nonConvertible bump
// strands the named always-current syncables and admits it anyway. The HTTP
// layer passes it for POST /v1/type/{id}?force=true.
func AcknowledgeStrandedSyncables() ProposeTypeOption {
	return func(o *ProposeTypeOptions) { o.AcknowledgeStranded = true }
}

// ResolveProposeTypeOptions folds an option list into its resolved set.
func ResolveProposeTypeOptions(opts []ProposeTypeOption) ProposeTypeOptions {
	var o ProposeTypeOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// StrandedSyncablesError refuses a nonConvertible version bump that would
// strand always-current syncables: their promise — every entity delivered at
// the current version — becomes unkeepable for data below the break. The
// operator either re-declares those syncables under a stance that survives a
// break (version-pinned / version-aware) or re-POSTs with force to
// acknowledge the stranding deliberately. The HTTP layer renders it 409.
type StrandedSyncablesError struct {
	// TypeID and Version identify the refused nonConvertible bump.
	TypeID  string
	Version int
	// Syncables are the always-current syncables consuming this type's topic.
	Syncables []string
}

func (e *StrandedSyncablesError) Error() string {
	return fmt.Sprintf(
		"type %q version %d is nonConvertible and would strand always-current syncable(s) %v — their data below the break can never reach the current version. Re-declare them version-pinned or version-aware, or re-POST with ?force=true to acknowledge the stranding",
		e.TypeID, e.Version, e.Syncables)
}

// TypeSchemaValidator validates that a Type's entity schema is structurally
// usable — that a known SchemaType's schema actually compiles. It closes the gap
// where a broken schema is accepted at POST /type (200) but then fails EVERY
// proposal to that type (a permanent error reported as a retryable 500). The
// check runs at admission (ProposeType), symmetric with how the jq migration is
// compiled at ParseType.
//
// It is INJECTED into the db layer (DB.SetTypeSchemaValidator) rather than called
// directly, because the concrete schema compilers (JSONSchema, Protobuf) live in
// the http layer, which db must not import — the same dependency inversion as the
// SyncableParser / IngestableParser / DatabaseParser seams.
type TypeSchemaValidator interface {
	// ValidateTypeSchema returns nil for a valid schema, a non-validating type,
	// or an UNKNOWN SchemaType (fail-open, so a schema type a newer producer
	// understands is not rejected here); it returns an error only when a KNOWN
	// SchemaType's schema will not compile.
	ValidateTypeSchema(t *Type) error
}

// MigrationEditAdvisory returns an operator-facing notice when an in-place type
// update changed only the [migration] transform — schema and version unchanged,
// the "fix a forgotten or buggy migration" path in ProposeType. Such an edit
// changes how FUTURE Actuals of the type are read, but does NOT re-materialize
// rows already synced through the previous migration on the type's dependent
// projections; rebuilding those projections to reach already-synced history is
// the operator's responsibility (see docs/read-models.md). before is the type as
// it stood before the update (nil for a brand-new type); after is the current
// type. Returns "" when no advisory is warranted — a new type, a schema/version
// bump (which forces a new version and its own migration), or a byte-identical
// no-op.
func MigrationEditAdvisory(before, after *Type) string {
	if before == nil || after == nil {
		return ""
	}
	migrationOnlyInPlace := before.Version == after.Version &&
		bytes.Equal(before.Schema, after.Schema) &&
		!bytes.Equal(before.Migration, after.Migration)
	if !migrationOnlyInPlace {
		return ""
	}
	return "the [migration] transform was updated in place at the same version, so it " +
		"applies only to Actuals synced from now on; already-synced rows keep the " +
		"previous migration's output on every always-current consumer of this " +
		"type's topic (migrationEditDependents names them). Re-materialize each " +
		"(POST /v1/syncable/{id}/rematerialize — keyed sinks converge in place) or " +
		"rebuild blue-green — see docs/read-models.md, \"Changing the rules after a " +
		"projection is live\"."
}

type TimePoint struct {
	Start time.Time
	End   time.Time
	Value uint64
}

func (t *Type) String() string {
	return fmt.Sprintf(" (%s) %s - v%d", t.ID, t.Name, t.Version)
}

// EntityKindRevision, not Snapshot: type configs are version-stored with
// rollback (every version retained and addressable), so the metadata-GC
// scrubber must NOT keep-latest-compact them. Retaining every registration in
// the permanent event log also keeps the log self-describing — a data entity's
// schema is always resolvable from the log itself, which is what lets the
// user-data-kind harvest work and the log replay back into bbolt's versioned
// type store. The other versioned configs (database/syncable/ingestable) are
// Revision for the same reason.
var typeType = registerSystemType(&Type{
	ID:         "268e1ac4-7d17-4798-afae-3f1f9aa6fc65",
	Name:       "InternalType",
	Version:    1,
	EntityKind: EntityKindRevision,
}, AdmissionConfig)

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
		// exceed int32), Validate has only the defined ValidationStrategy
		// constants (ParseType rejects anything else), and EntityKind only
		// the defined EntityKind constants (ParseEntityKind rejects
		// anything else).
		Version:           int32(t.Version), //nolint:gosec // G115: bounded by domain
		SchemaType:        t.SchemaType,
		Schema:            t.Schema,
		Validate:          clusterpb.LogValidationStrategy(t.Validate), //nolint:gosec // G115: bounded by domain
		Migration:         t.Migration,
		EntityKind:        clusterpb.LogEntityKind(t.EntityKind), //nolint:gosec // G115: bounded by domain
		Discriminator:     t.Discriminator,
		SchemaChangeTopic: t.SchemaChangeTopic,
		NonConvertible:    t.NonConvertible,
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
	t.SchemaChangeTopic = lt.SchemaChangeTopic
	t.NonConvertible = lt.NonConvertible

	return nil
}
