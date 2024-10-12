package db

import (
	"bytes"
	"fmt"

	"github.com/oklog/ulid/v2"
	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) ProposeType(c *cluster.Configuration) error {
	_, t, err := ParseType(c.MimeType, c.Data, db.storage)
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertTypeEntity(t)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(p)
}

func ParseType(mimeType string, data []byte, s cluster.DatabaseStorage) (string, *cluster.Type, error) {
	v, err := parseBytes(mimeType, bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}

	id := fmt.Sprintf("%v", ulid.Make())
	name := v.GetString("type.name")
	version := 0
	if v.IsSet("type.version") {
		version = v.GetInt("type.version")
	}

	t := &cluster.Type{
		ID:         id,
		Name:       name,
		Version:    version,
		SchemaType: "",
		Schema:     []byte{},
		Validate:   0,
	}

	return name, t, nil
}
