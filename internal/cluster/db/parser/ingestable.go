package parser

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// ingestableEnvelopeKeys is the [ingestable] vocabulary: what ParseIngestable
// and cluster.ParseCensusOptions read. Pinned to those reads by the
// vocabulary conformance test.
var ingestableEnvelopeKeys = []string{"name", "type", "census", "censusValues", "censusValueLimit"}

func (p *Parser) AddIngestableParser(name string, sp cluster.IngestableParser) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ingestableParsers[name] = sp
}

// IngestableTopics reports which topics the ingestable config PRODUCES, read
// from the config alone (no Preflight / no source connection). It mirrors
// SyncableTopics: pick the type-specific parser, then delegate to its
// cluster.IngestableTopicExtractor. Returns nil (not an error) when the
// matched parser doesn't extract topics, so an unknown ingestable kind
// simply contributes no producer edges. Errors only when the bytes don't
// parse at all.
func (p *Parser) IngestableTopics(mimeType string, data []byte) ([]string, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return nil, err
	}

	tipe := v.GetString("ingestable.type")
	p.mu.RLock()
	parser, ok := p.ingestableParsers[tipe]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("cannot parse ingestable of type: %s", tipe)
	}

	extractor, ok := parser.(cluster.IngestableTopicExtractor)
	if !ok {
		return nil, nil // this ingestable kind can't report its topics — no edges
	}
	return extractor.TopicsFromConfig(v), nil
}

func (p *Parser) ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return "", nil, err
	}

	name := v.GetString("ingestable.name")
	tipe := v.GetString("ingestable.type")
	// The document's vocabulary is closed: the [ingestable] header and the
	// type's own section; the sub-parser checks the latter's keys.
	if err := v.RejectUnknownSections("ingestable", tipe); err != nil {
		return "", nil, err
	}
	if err := v.RejectUnknownKeys("ingestable", ingestableEnvelopeKeys...); err != nil {
		return "", nil, err
	}
	p.mu.RLock()
	parser, ok := p.ingestableParsers[tipe]
	p.mu.RUnlock()

	if !ok {
		return "", nil, fmt.Errorf("cannot parse ingestable of type: %s", tipe)
	}

	ingestable, err := parser.Parse(v)
	if err != nil {
		return "", nil, err
	}
	return name, ingestable, nil
}
