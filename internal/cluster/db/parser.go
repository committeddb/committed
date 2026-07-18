package db

import (
	"github.com/committeddb/committed/internal/cluster"
)

type Parser interface {
	AddIngestableParser(name string, p cluster.IngestableParser)
	AddSyncableParser(name string, p cluster.SyncableParser)
	AddDatabaseParser(name string, p cluster.DatabaseParser)
	ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error)
	ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error)
	ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error)
	// SyncableTopics reports which topics the syncable config consumes, read
	// from the config alone (no Init / no DDL) so the propose path can enumerate
	// the syncables an ingestable primaryKey change affects. Returns nil for a
	// syncable kind whose parser can't extract topics.
	SyncableTopics(mimeType string, data []byte) ([]string, error)
	// SyncableDatabases reports which destination databases the syncable config
	// references, read from the config alone, so the propose path can enumerate
	// the syncables a database connection change would break. Returns nil for a
	// syncable kind whose parser can't extract databases.
	SyncableDatabases(mimeType string, data []byte) ([]string, error)
	// SyncableSchemaChange reports whether replacing the prior syncable config
	// document with next would change the materialized destination in a way that
	// CREATE TABLE IF NOT EXISTS can't apply in place (returning a
	// cluster.RebuildRequiredError), or nil if it is safe. Both schemas are derived
	// from the config documents alone (no database resolution), so a missing
	// database secret on this node can't defeat the guard. Fails open (returns nil)
	// when either document is unparseable or the kind has no materialized schema.
	SyncableSchemaChange(mimeType string, prior, next []byte, s cluster.DatabaseStorage) error
	Validate(mimeType string, data []byte) error
}
