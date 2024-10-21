package parser

import (
	"bytes"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (p *Parser) AddDatabaseParser(name string, dp cluster.DatabaseParser) {
	p.databaseParsers[name] = dp
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

	database, err := parser.Parse(v, name)
	if err != nil {
		return "", nil, err
	}

	return name, database, nil
}
