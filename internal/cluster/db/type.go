package db

import (
	"bytes"
	"context"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

func (db *DB) ProposeType(ctx context.Context, c *cluster.Configuration) error {
	_, t, err := ParseType(c, db.storage)
	if err != nil {
		return cluster.NewConfigError(err)
	}

	// Admission schema check: compile the entity schema here so a broken one is a
	// ConfigError (400) at POST /type, not an accepted-then-permanent-500 on every
	// proposal to the type — symmetric with the jq migration compiled in ParseType.
	// Nil-safe (some tests inject no validator); the schema is self-contained, so
	// this admission check need not re-run on apply. Fail-open for unknown
	// SchemaTypeS is preserved by the validator (returns nil).
	if db.schemaValidator != nil {
		if err := db.schemaValidator.ValidateTypeSchema(t); err != nil {
			return cluster.NewConfigError(err)
		}
	}

	// An announce-typed type's event destination must be usable the moment a
	// divergence needs announcing — checked LOUDLY here at POST, not
	// discovered as a silently unannounced divergence at first use (the
	// admission-validation bug class). The destination must exist (so the
	// operator declares the events topic first) and must not itself be
	// announce-typed (an event whose own divergence announces somewhere is a
	// cycle in the making; the emitter also guards at runtime).
	if t.Validate == cluster.ValidateAnnounce {
		dest, derr := db.storage.ResolveType(cluster.LatestTypeRef(t.SchemaChangeTopic))
		if derr != nil || dest == nil {
			return &cluster.ConfigError{
				Err: fmt.Errorf("schemaChangeTopic %q does not name an existing type: declare the ContractExtension events topic first, then the announce-typed type", t.SchemaChangeTopic),
			}
		}
		if dest.Validate == cluster.ValidateAnnounce {
			return &cluster.ConfigError{
				Err: fmt.Errorf("schemaChangeTopic %q is itself announce-typed: an events topic cannot announce its own divergences (chain events topics are not supported)", t.SchemaChangeTopic),
			}
		}
	}

	// Delta topics are hostile to the sync contract: at-least-once
	// delivery redelivers, and a redelivered non-idempotent patch
	// ("add 3") corrupts. Rejected at creation rather than carried as a
	// footgun — model state changes as events (or snapshots) instead.
	// See README § Entity kinds.
	if t.EntityKind == cluster.EntityKindDelta {
		return &cluster.ConfigError{
			Err: fmt.Errorf("entityKind \"delta\" is not supported: at-least-once sync delivery corrupts non-idempotent patches — model the changes as events instead (see README § Entity kinds)"),
		}
	}

	existing, err := db.storage.ResolveType(cluster.LatestTypeRef(c.ID))
	isNew := existing == nil || err != nil

	if isNew {
		// First version of this type.
		t.Version = 1
	} else {
		// The entity kind is immutable once declared: data already in
		// the log was written under the declared kind's semantics, and
		// reinterpreting it under another kind is exactly the misuse
		// the field exists to catch. Changing your mind = new
		// type/topic. Declaring an entity kind on a previously-
		// unspecified type is allowed (unspecified is the absence of a
		// declaration, not a declaration), which is the one-way path
		// grandfathered types use to adopt kinds.
		if existing.EntityKind != cluster.EntityKindUnspecified && t.EntityKind != existing.EntityKind {
			if t.EntityKind == cluster.EntityKindUnspecified {
				return &cluster.ConfigError{
					Err: fmt.Errorf("type %q has entityKind %q but the proposed config omits it: entityKind is immutable, restate entityKind = %q (changing it requires a new type)", c.ID, existing.EntityKind, existing.EntityKind),
				}
			}
			return &cluster.ConfigError{
				Err: fmt.Errorf("type %q has entityKind %q and entityKind is immutable (got %q): changing it requires a new type", c.ID, existing.EntityKind, t.EntityKind),
			}
		}

		schemaChanged := !bytes.Equal(existing.Schema, t.Schema) ||
			existing.SchemaType != t.SchemaType ||
			existing.Validate != t.Validate ||
			existing.Name != t.Name
		migrationChanged := !bytes.Equal(existing.Migration, t.Migration)
		// The entity kind can only ever differ here as
		// unspecified→declared (the adoption path above); the
		// discriminator is mutable sugar.
		entityKindChanged := existing.EntityKind != t.EntityKind
		discriminatorChanged := existing.Discriminator != t.Discriminator
		// The event destination is mutable routing, like the discriminator:
		// re-pointing it changes where FUTURE divergences announce, not the
		// shape data is written in.
		schemaChangeTopicChanged := existing.SchemaChangeTopic != t.SchemaChangeTopic

		if !schemaChanged && !migrationChanged && !entityKindChanged && !discriminatorChanged && !schemaChangeTopicChanged {
			return nil // byte-identical, no-op
		}

		if schemaChanged {
			// Schema evolution requires an explicit [migration] section
			// (transform or none=true) so operators can't accidentally
			// break always-current syncables by forgetting a migration.
			if !t.MigrationExplicit {
				return &cluster.ConfigError{
					Err: fmt.Errorf("schema changed for type %q: a [migration] section is required (provide transform = \"<jq>\" or none = true)", c.ID),
				}
			}
			t.Version = existing.Version + 1
		} else {
			// Schema unchanged: only migration, entity-kind adoption,
			// or discriminator changed. None of these alter the shape
			// data is written in, so update the current version in
			// place, no version bump (the migration case is the "fix a
			// forgotten or buggy migration" path).
			t.Version = existing.Version
		}
	}

	e, err := cluster.NewUpsertTypeEntity(t)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

func ParseType(c *cluster.Configuration, s cluster.DatabaseStorage) (string, *cluster.Type, error) {
	// A user cannot author a type whose id collides with committed's internal
	// system types — either the reserved system-type namespace (an older node
	// would treat it as a skippable/must-gate system record) OR a grandfathered
	// built-in id. The built-in case is load-bearing: the apply path resolves a
	// built-in id to the system type (systemType-first, see resolveType), so a
	// user type sitting in the bucket under that id lets a later proposal's user
	// bytes reach an internal config handler that Fatals on the decode mismatch —
	// a committed, deterministic entry that crash-loops every node. Built-in
	// system types register directly and never reach here, so this only guards
	// user submissions.
	if cluster.IsReservedSystemID(c.ID) || cluster.IsInternal(c.ID) {
		return "", nil, fmt.Errorf("type id %q is a committed system-type id (reserved namespace or built-in) and cannot be used for a user type", c.ID)
	}

	// Type configs decode without ${VAR} interpolation, deliberately:
	// schemas and jq programs are not secrets, and a literal "${" in a
	// schema document must not error on a missing env var.
	v, err := cluster.ParseConfigBytes(c.MimeType, c.Data)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("type.name")
	version := 0
	if v.IsSet("type.version") {
		version = v.GetInt("type.version")
	}

	var schemaType string
	if v.IsSet("type.schemaType") {
		schemaType = v.GetString("type.schemaType")
	}

	var schema []byte
	if v.IsSet("type.schema") {
		schema = []byte(v.GetString("type.schema"))
	}

	var validate cluster.ValidationStrategy
	if v.IsSet("type.validate") {
		validate = cluster.ValidationStrategy(v.GetInt("type.validate"))
	}
	switch validate {
	case cluster.NoValidation, cluster.ValidateSchema, cluster.ValidateAnnounce:
	default:
		return "", nil, fmt.Errorf("validate = %d is not a known validation strategy (0 = none, 1 = gate on schema, 2 = announce divergence)", validate)
	}

	// Both validating strategies need a schema to check against; announce
	// (the tripwire) additionally needs somewhere to announce to.
	if validate == cluster.ValidateSchema || validate == cluster.ValidateAnnounce {
		if schemaType == "" {
			return "", nil, fmt.Errorf("validate is enabled but schemaType is not set")
		}
		if len(schema) == 0 {
			return "", nil, fmt.Errorf("validate is enabled but schema is empty")
		}
	}

	// schemaChangeTopic names the Type ID that receives ContractExtension
	// events for this type's divergences. Required with announce (a tripwire
	// with nowhere to announce is silent — the failure mode it exists to
	// kill), meaningless without it, and never this type itself (an event
	// about a divergence must not be validated by the very contract it
	// reports on). Whether the destination type EXISTS is checked in
	// ProposeType, which has storage.
	schemaChangeTopic := v.GetString("type.schemaChangeTopic")
	if validate == cluster.ValidateAnnounce && schemaChangeTopic == "" {
		return "", nil, fmt.Errorf("validate = 2 (announce) requires schemaChangeTopic: the Type ID that receives ContractExtension events")
	}
	if validate != cluster.ValidateAnnounce && schemaChangeTopic != "" {
		return "", nil, fmt.Errorf("schemaChangeTopic is only valid with validate = 2 (announce)")
	}
	if schemaChangeTopic == c.ID {
		return "", nil, fmt.Errorf("schemaChangeTopic cannot be the type itself")
	}

	// entityKind declares what the entities written under this type
	// are. Optional — omitted means unspecified, which behaves exactly
	// like an untyped topic. ProposeType enforces the kind rules (delta
	// rejection, immutability); here we only reject unknown strings.
	entityKind, err := cluster.ParseEntityKind(v.GetString("type.entityKind"))
	if err != nil {
		return "", nil, err
	}

	// discriminator names the field (as a jsonpath) that distinguishes
	// entity variants. It is projection sugar for event topics; on any
	// other kind there are no variants to discriminate, so its presence
	// is a config mistake worth failing loudly on.
	discriminator := v.GetString("type.discriminator")
	if discriminator != "" && entityKind != cluster.EntityKindEvent {
		return "", nil, fmt.Errorf("discriminator is only valid for entityKind = \"event\" (got entityKind %q)", entityKind)
	}

	// [migration] section. Exactly one of these must be present for
	// every type version after v1:
	//  - transform = "<jq>" — a jq program upgrading v(N-1) data to vN.
	//  - none = true — the operator asserts v(N-1) data is already valid
	//    at vN shape and no transform is needed.
	// ParseType stores the result; ProposeType enforces the requirement.
	var migration []byte
	hasMigrationTransform := v.IsSet("migration.transform")
	hasMigrationNone := v.IsSet("migration.none") && v.GetBool("migration.none")

	if hasMigrationTransform && hasMigrationNone {
		return "", nil, fmt.Errorf("[migration] cannot specify both transform and none")
	}
	if hasMigrationTransform {
		migration = []byte(v.GetString("migration.transform"))
		if err := compileMigration(migration); err != nil {
			return "", nil, fmt.Errorf("migration.transform is not valid jq: %w", err)
		}
	}

	// Optional pre-flight: migration.validateAgainst carries a sample
	// JSON payload in the previous version's shape to run the proposed
	// transform against, so a program that parses but breaks on real data
	// is caught at propose time instead of at first-sync. Validation-only:
	// it never becomes part of the Type (though, like the rest of the
	// TOML, it stays visible in the raw config version history).
	if v.IsSet("migration.validateAgainst") {
		if !hasMigrationTransform {
			return "", nil, fmt.Errorf("migration.validateAgainst requires migration.transform (there is no program to validate)")
		}
		sample := []byte(v.GetString("migration.validateAgainst"))
		if err := runMigrationSample(migration, sample); err != nil {
			return "", nil, fmt.Errorf("migration.transform failed against the validateAgainst sample: %w", err)
		}
	}

	t := &cluster.Type{
		ID:                c.ID,
		Name:              name,
		Version:           version,
		SchemaType:        schemaType,
		Schema:            schema,
		Validate:          validate,
		Migration:         migration,
		EntityKind:        entityKind,
		Discriminator:     discriminator,
		SchemaChangeTopic: schemaChangeTopic,
		MigrationExplicit: hasMigrationTransform || hasMigrationNone,
	}

	return name, t, nil
}

// compileMigration validates a jq program at parse time so bad programs
// fail at ProposeType, not at first-sync.
func compileMigration(program []byte) error {
	return migration.Compile(program)
}

// runMigrationSample runs a proposed transform against an operator-supplied
// sample payload — the pre-flight half of migration validation. Like
// compileMigration, it lives outside ParseType because the local `migration`
// variable there shadows the package name.
func runMigrationSample(program, sample []byte) error {
	// ParseType carries no ctx (it runs under the propose handler), so pass a
	// background ctx; migration.Run's internal per-run timeout still bounds a
	// pathological validateAgainst program so it can't wedge the handler.
	_, err := migration.Run(context.Background(), program, sample)
	return err
}

// SetTypeSchemaValidator injects the entity-schema compiler that ProposeType uses
// to reject a broken schema at admission. Wired in cmd/node.go with the http-layer
// implementation, which db must not import directly (see
// cluster.TypeSchemaValidator). Call once after db.New, before serving.
func (db *DB) SetTypeSchemaValidator(v cluster.TypeSchemaValidator) {
	db.schemaValidator = v
}

func (db *DB) Types() ([]*cluster.Configuration, error) {
	return db.storage.Types()
}

func (db *DB) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	return db.storage.ResolveType(ref)
}

func (db *DB) TypeVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.TypeVersions(id)
}

func (db *DB) TypeVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.TypeVersion(id, version)
}

// SetEntityValidator injects the entity-payload validator the validation
// tripwire runs in Propose (see tripwire.go). Injected for the same reason as
// SetTypeSchemaValidator: the schema compilers live in http, which db must
// not import.
func (db *DB) SetEntityValidator(v cluster.EntitySchemaValidator) {
	db.entityValidator = v
}
