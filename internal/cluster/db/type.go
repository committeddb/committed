package db

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/migration"
)

func (db *DB) ProposeType(ctx context.Context, c *cluster.Configuration) error {
	_, t, err := ParseType(c, db.storage)
	if err != nil {
		return &cluster.ConfigError{Err: err}
	}

	existing, err := db.storage.ResolveType(cluster.LatestTypeRef(c.ID))
	isNew := existing == nil || err != nil

	if isNew {
		// First version of this type.
		t.Version = 1
	} else {
		schemaChanged := !bytes.Equal(existing.Schema, t.Schema) ||
			existing.SchemaType != t.SchemaType ||
			existing.Validate != t.Validate ||
			existing.Name != t.Name
		migrationChanged := !bytes.Equal(existing.Migration, t.Migration)

		if !schemaChanged && !migrationChanged {
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
			// Schema unchanged, only migration changed. This is the
			// "fix a forgotten or buggy migration" path: update the
			// current version's migration in place, no version bump.
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
	v, err := parseBytes(c.MimeType, bytes.NewReader(c.Data))
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

	if validate == cluster.ValidateSchema {
		if schemaType == "" {
			return "", nil, fmt.Errorf("validate is enabled but schemaType is not set")
		}
		if len(schema) == 0 {
			return "", nil, fmt.Errorf("validate is enabled but schema is empty")
		}
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

	t := &cluster.Type{
		ID:                c.ID,
		Name:              name,
		Version:           version,
		SchemaType:        schemaType,
		Schema:            schema,
		Validate:          validate,
		Migration:         migration,
		MigrationExplicit: hasMigrationTransform || hasMigrationNone,
	}

	return name, t, nil
}

// compileMigration validates a jq program at parse time so bad programs
// fail at ProposeType, not at first-sync.
func compileMigration(program []byte) error {
	return migration.Compile(program)
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

func (db *DB) TypeGraph(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error) {
	return db.storage.TimePoints(typeID, start, end)
}
