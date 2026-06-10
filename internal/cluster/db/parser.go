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
	Validate(mimeType string, data []byte) error
}
