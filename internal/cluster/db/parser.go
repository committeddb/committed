package db

import "github.com/philborlin/committed/internal/cluster"

type Parser interface {
	AddSyncableParser(name string, p cluster.SyncableParser)
	AddDatabaseParser(name string, p cluster.DatabaseParser)
	ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error)
	ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error)
}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	db.parser.AddSyncableParser(name, p)
}

func (db *DB) AddDatabaseParser(name string, p cluster.DatabaseParser) {
	db.parser.AddDatabaseParser(name, p)
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
	db.Propose(p)

	return nil
}
func (db *DB) ProposeDatabase(c *cluster.Configuration) error {
	_, _, err := db.parser.ParseDatabase(c.MimeType, c.Data)
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertDatabaseEntity(c)
	if err != nil {
		return err
	}
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	db.Propose(p)

	return nil
}

func (db *DB) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error) {
	return db.parser.ParseSyncable(mimeType, data, s)
}

func (db *DB) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	return db.parser.ParseDatabase(mimeType, data)
}
