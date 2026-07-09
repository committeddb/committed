package db

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

type IngestableWithID struct {
	ID         string
	Ingestable cluster.Ingestable
	// Delete signals that the ingestable with ID was removed from the log
	// (deleteIngestable on the apply path), rather than upserted. The DB-layer
	// consumer (listenForIngestables) cancels the worker and, on the owner, tears
	// down the source-side replication resources (drops the Postgres slot +
	// publication). Ingestable is nil for a delete.
	Delete bool
}

func (db *DB) AddIngestableParser(name string, p cluster.IngestableParser) {
	db.parser.AddIngestableParser(name, p)
}

func (db *DB) ProposeIngestable(ctx context.Context, c *cluster.Configuration) error {
	name, _, err := db.ParseIngestable(c.MimeType, c.Data)
	if err != nil {
		return cluster.NewConfigError(err)
	}
	c.Name = name

	e, err := cluster.NewUpsertIngestableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

func (db *DB) ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error) {
	return db.parser.ParseIngestable(mimeType, data)
}

// DeleteIngestable removes an ingestable: its config and checkpoint position are
// deleted atomically (one proposal), and on apply the owner node cancels the
// worker and tears down the source-side replication resources (drops the Postgres
// slot + publication) so an orphaned slot can't pin the source's WAL. Blocks
// until applied (Propose returns after the delete is durable). There is no
// keep-data option — dropping the slot is always the right thing on decommission.
func (db *DB) DeleteIngestable(ctx context.Context, id string) error {
	p := &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities(id)}
	return db.Propose(ctx, p)
}

func (db *DB) Ingestables() ([]*cluster.Configuration, error) {
	return db.storage.Ingestables()
}

func (db *DB) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.IngestableVersions(id)
}

func (db *DB) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.IngestableVersion(id, version)
}
