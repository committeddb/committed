package parser

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

func (p *Parser) AddSyncableParser(name string, sp cluster.SyncableParser) {
	p.syncableParsers[name] = sp
}

// SyncableTopics reports which topics the syncable config consumes, read from
// the config alone (no Init / no DDL). It reuses ParseSyncable's front half to
// pick the type-specific parser, then delegates to that parser's
// cluster.SyncableTopicExtractor. Returns nil (not an error) when the matched
// parser doesn't extract topics, so an unknown syncable kind simply contributes
// no dependents. Errors only when the bytes don't parse at all.
func (p *Parser) SyncableTopics(mimeType string, data []byte) ([]string, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return nil, err
	}

	tipe := v.GetString("syncable.type")
	parser, ok := p.syncableParsers[tipe]
	if !ok {
		return nil, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	extractor, ok := parser.(cluster.SyncableTopicExtractor)
	if !ok {
		return nil, nil // this syncable kind can't report its topics — no dependents
	}
	return extractor.TopicsFromConfig(v), nil
}

// SyncableDatabases reports which destination databases the syncable config
// references, read from the config alone (no Init / no destination connection).
// It mirrors SyncableTopics: reuse ParseSyncable's front half to pick the
// type-specific parser, then delegate to that parser's
// cluster.SyncableDatabaseExtractor. Returns nil (not an error) when the matched
// parser doesn't extract databases, so an unknown syncable kind simply
// contributes no dependents. Errors only when the bytes don't parse at all.
func (p *Parser) SyncableDatabases(mimeType string, data []byte) ([]string, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return nil, err
	}

	tipe := v.GetString("syncable.type")
	parser, ok := p.syncableParsers[tipe]
	if !ok {
		return nil, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	extractor, ok := parser.(cluster.SyncableDatabaseExtractor)
	if !ok {
		return nil, nil // this syncable kind can't report its databases — no dependents
	}
	return extractor.DatabasesFromConfig(v), nil
}

func (p *Parser) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error) {
	v, err := parseBytes(mimeType, data)
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
