package parser

import (
	"bytes"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (p *Parser) AddIngestableParser(name string, sp cluster.IngestableParser) {
	p.ingestableParsers[name] = sp
}

func (p *Parser) ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error) {
	v, err := parseBytes(mimeType, bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("ingestable.name")
	tipe := v.GetString("ingestable.type")
	parser, ok := p.ingestableParsers[tipe]

	if !ok {
		return "", nil, fmt.Errorf("cannot parse ingestable of type: %s", tipe)
	}

	ingestable, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}
	return name, ingestable, nil
}
