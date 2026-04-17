package parser

import (
	"bytes"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (p *Parser) AddSyncableParser(name string, sp cluster.SyncableParser) {
	p.syncableParsers[name] = sp
}

func (p *Parser) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error) {
	v, err := parseBytes(mimeType, bytes.NewReader(data))
	if err != nil {
		return "", nil, 0, err
	}

	name := v.GetString("syncable.name")
	tipe := v.GetString("syncable.type")
	parser, ok := p.syncableParsers[tipe]

	if !ok {
		return "", nil, 0, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	mode, err := cluster.ParseSyncableMode(v.GetString("syncable.mode"))
	if err != nil {
		return "", nil, 0, err
	}

	syncable, err := parser.Parse(v, s)
	if err != nil {
		return "", nil, 0, err
	}
	return name, syncable, mode, nil
}
