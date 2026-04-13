package db

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) ProposeType(ctx context.Context, c *cluster.Configuration) error {
	_, t, err := ParseType(c, db.storage)
	if err != nil {
		return &cluster.ConfigError{Err: err}
	}

	// Enforce immutability: a type+version pair is immutable once defined.
	// To evolve a type, increment the version field.
	existing, err := db.storage.Type(c.ID)
	if err == nil && existing != nil && existing.Version == t.Version {
		return &cluster.ConfigError{
			Err: fmt.Errorf("type %q version %d already exists and is immutable; increment the version to evolve", c.ID, t.Version),
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

	t := &cluster.Type{
		ID:         c.ID,
		Name:       name,
		Version:    version,
		SchemaType: schemaType,
		Schema:     schema,
		Validate:   validate,
	}

	return name, t, nil
}

func (db *DB) Types() ([]*cluster.Configuration, error) {
	return db.storage.Types()
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
