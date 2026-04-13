package db

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) AddDatabaseParser(name string, p cluster.DatabaseParser) {
	db.parser.AddDatabaseParser(name, p)
}

func (db *DB) ProposeDatabase(ctx context.Context, c *cluster.Configuration) error {
	_, database, err := db.parser.ParseDatabase(c.MimeType, c.Data)
	if err != nil {
		return &cluster.ConfigError{Err: err}
	}

	err = database.Close()
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertDatabaseEntity(c)
	if err != nil {
		return err
	}
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

func (db *DB) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	return db.parser.ParseDatabase(mimeType, data)
}

func (db *DB) Databases() ([]*cluster.Configuration, error) {
	return db.storage.Databases()
}

func (db *DB) DatabaseVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.DatabaseVersions(id)
}

func (db *DB) DatabaseVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.DatabaseVersion(id, version)
}
