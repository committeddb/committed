package db

import (
	"bytes"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) ProposeType(c *cluster.Configuration) error {
	_, t, err := ParseType(c, db.storage)
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

	t := &cluster.Type{
		ID:         c.ID,
		Name:       name,
		Version:    version,
		SchemaType: "",
		Schema:     []byte{},
		Validate:   0,
	}

	return name, t, nil
}

func (db *DB) Types() ([]*cluster.Configuration, error) {
	return db.storage.Types()
}
