package db

import "github.com/philborlin/committed/internal/cluster"

type IngestableWithID struct {
	ID         string
	Ingestable cluster.Ingestable
}

func (db *DB) AddIngestableParser(name string, p cluster.IngestableParser) {
	db.parser.AddIngestableParser(name, p)
}

func (db *DB) ProposeIngestable(c *cluster.Configuration) error {
	_, _, err := db.ParseIngestable(c.MimeType, c.Data)
	if err != nil {
		return err
	}

	e, err := cluster.NewUpsertIngestableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(p)
}

func (db *DB) ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error) {
	return db.parser.ParseIngestable(mimeType, data)
}

func (db *DB) Ingestables() ([]*cluster.Configuration, error) {
	return db.storage.Ingestables()
}
