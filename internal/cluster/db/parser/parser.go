package parser

import (
	"io"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type Parser struct {
	databaseParsers map[string]cluster.DatabaseParser
	syncableParsers map[string]cluster.SyncableParser
}

func New() *Parser {
	return &Parser{
		databaseParsers: make(map[string]cluster.DatabaseParser),
		syncableParsers: make(map[string]cluster.SyncableParser),
	}
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
