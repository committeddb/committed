package parser

import (
	"bytes"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (p *Parser) AddSyncableParser(name string, sp cluster.SyncableParser) {
	p.syncableParsers[name] = sp
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
