package parser

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

func (p *Parser) AddDatabaseParser(name string, dp cluster.DatabaseParser) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.databaseParsers[name] = dp
}

func (p *Parser) ParseDatabase(mimeType string, data []byte) (string, cluster.Database, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("database.name")
	dbType := v.GetString("database.type")
	p.mu.RLock()
	parser, ok := p.databaseParsers[dbType]
	p.mu.RUnlock()

	if !ok {
		return "", nil, fmt.Errorf("cannot parse database of type: %s", dbType)
	}

	database, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}

	return name, database, nil
}
