package db

import "github.com/philborlin/committed/internal/cluster"

type SyncableWithID struct {
	ID       string
	Syncable cluster.Syncable
}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	db.parser.AddSyncableParser(name, p)
}

func (db *DB) ProposeSyncable(c *cluster.Configuration) error {
	_, _, err := db.ParseSyncable(c.MimeType, c.Data, db.storage)
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertSyncableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(p)
}

func (db *DB) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error) {
	return db.parser.ParseSyncable(mimeType, data, s)
}

func (db *DB) Syncables() ([]*cluster.Configuration, error) {
	return db.storage.Syncables()
}
