package db

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
)

type SyncableWithID struct {
	ID       string
	Syncable cluster.Syncable
}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	db.parser.AddSyncableParser(name, p)
}

func (db *DB) ProposeSyncable(ctx context.Context, c *cluster.Configuration) error {
	_, _, err := db.ParseSyncable(c.MimeType, c.Data, db.storage)
	if err != nil {
		return &cluster.ConfigError{Err: err}
	}

	e, err := cluster.NewUpsertSyncableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

func (db *DB) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error) {
	return db.parser.ParseSyncable(mimeType, data, s)
}

func (db *DB) Syncables() ([]*cluster.Configuration, error) {
	return db.storage.Syncables()
}

func (db *DB) SyncableVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.SyncableVersions(id)
}

func (db *DB) SyncableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.SyncableVersion(id, version)
}
