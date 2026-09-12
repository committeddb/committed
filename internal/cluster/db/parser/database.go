package parser

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// databaseEnvelopeKeys is the [database] vocabulary (ParseDatabase's reads).
var databaseEnvelopeKeys = []string{"name", "type"}

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
	if err := v.RejectUnknownSections("database", dbType); err != nil {
		return "", nil, err
	}
	if err := v.RejectUnknownKeys("database", databaseEnvelopeKeys...); err != nil {
		return "", nil, err
	}
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
