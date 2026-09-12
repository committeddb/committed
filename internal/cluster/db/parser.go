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
	// IngestableTopics reports which topics the ingestable config PRODUCES,
	// read from the config alone (no Preflight / no source connection), so
	// the propose path and the apply-time build guard can walk the producer
	// graph. Returns nil for a kind whose parser can't extract topics.
	IngestableTopics(mimeType string, data []byte) ([]string, error)
	ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error)
	// SyncableTopics reports which topics the syncable config consumes, read
	// from the config alone (no Init / no DDL) so the propose path can enumerate
	// the syncables an ingestable primaryKey change affects. Returns nil for a
	// syncable kind whose parser can't extract topics.
	SyncableTopics(mimeType string, data []byte) ([]string, error)
	// SyncableDerivedTopics reports which topics the syncable config PRODUCES
	// (its derivation targets — a loopback's loopback.target), read from the
	// config alone, so the propose path and the apply-time build guard can
	// walk the derivation graph. Returns nil for every non-deriving kind.
	SyncableDerivedTopics(mimeType string, data []byte) ([]string, error)
	// SyncableZone reports the syncable config's pinned zone ([syncable]
	// envelope `zone` key; "" = unpinned), read from the config alone, so
	// ownership resolution and admission run no build.
	SyncableZone(mimeType string, data []byte) (string, error)
	// SyncableMode reports the syncable config's consumer stance
	// (as-stored / always-current), read from the config alone — no Init, no
	// destination pool — so admission checks can classify every stored
	// syncable without side effects.
	SyncableMode(mimeType string, data []byte) (cluster.SyncableMode, error)
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
