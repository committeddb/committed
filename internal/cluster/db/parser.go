package db

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type Parser interface {
	AddSyncableParser(name string, p cluster.SyncableParser)
	AddDatabaseParser(name string, p cluster.DatabaseParser)
	ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error)
	ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error)
}

func parseBytes(mimeType string, reader io.Reader) (*viper.Viper, error) {
	style := "toml"
	if mimeType == "application/json" {
		style = "json"
	}

	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	return v, nil
}
