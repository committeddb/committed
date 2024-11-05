package db

import "github.com/philborlin/committed/internal/cluster"

func (db *DB) AddDatabaseParser(name string, p cluster.DatabaseParser) {
	db.parser.AddDatabaseParser(name, p)
}

func (db *DB) ProposeDatabase(c *cluster.Configuration) error {
	_, database, err := db.parser.ParseDatabase(c.MimeType, c.Data)
	if err != nil {
		return err
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
	return db.Propose(p)
}

func (db *DB) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	return db.parser.ParseDatabase(mimeType, data)
}

func (db *DB) Databases() ([]*cluster.Configuration, error) {
	return db.storage.Databases()
}
