package db

import (
	"bytes"
	"fmt"
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

func (p *Parser) AddSyncableParser(name string, sp cluster.SyncableParser) {
	p.syncableParsers[name] = sp
}

func (p *Parser) AddDatabaseParser(name string, dp cluster.DatabaseParser) {
	p.databaseParsers[name] = dp
}

func (p *Parser) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, error) {
	v, err := parseBytes(mimeType, bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("syncable.name")
	tipe := v.GetString("syncable.type")
	parser, ok := p.syncableParsers[tipe]

	if !ok {
		return "", nil, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	syncable, err := parser.Parse(v, s)
	if err != nil {
		return "", nil, err
	}
	return name, syncable, nil
}

func (p *Parser) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	v, err := parseBytes(mimeType, bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("database.name")
	dbType := v.GetString("database.type")
	parser, ok := p.databaseParsers[dbType]

	if !ok {
		return "", nil, fmt.Errorf("cannot parse database of type: %s", dbType)
	}

	database, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}
	return name, database, nil
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
