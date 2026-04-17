package db

import (
	"io"

	"github.com/spf13/viper"

	"github.com/philborlin/committed/internal/cluster"
)

type Parser interface {
	AddIngestableParser(name string, p cluster.IngestableParser)
	AddSyncableParser(name string, p cluster.SyncableParser)
	AddDatabaseParser(name string, p cluster.DatabaseParser)
	ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error)
	ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error)
	ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error)
}

func parseBytes(mimeType string, reader io.Reader) (*viper.Viper, error) {
	style := "toml"
	if mimeType == "application/json" {
		style = "json"
	}

	v := viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	return v, nil
}
