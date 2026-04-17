package parser

import (
	"io"

	"github.com/spf13/viper"

	"github.com/philborlin/committed/internal/cluster"
)

type Parser struct {
	databaseParsers   map[string]cluster.DatabaseParser
	ingestableParsers map[string]cluster.IngestableParser
	syncableParsers   map[string]cluster.SyncableParser
}

func New() *Parser {
	return &Parser{
		databaseParsers:   make(map[string]cluster.DatabaseParser),
		ingestableParsers: make(map[string]cluster.IngestableParser),
		syncableParsers:   make(map[string]cluster.SyncableParser),
	}
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
