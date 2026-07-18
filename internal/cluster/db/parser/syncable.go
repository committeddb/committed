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

// SyncableSchemaChange reports whether replacing the prior syncable config
// document with next would change the materialized destination in a way CREATE
// TABLE IF NOT EXISTS can't apply in place, returning a
// cluster.RebuildRequiredError if so and nil if it is safe. Both schemas are
// derived from the config documents alone (no database resolution) via
// syncableSchema, so a missing database secret on this node can't defeat the
// guard. Fails open (returns nil) when either document is unparseable or the kind
// has no materialized schema.
func (p *Parser) SyncableSchemaChange(mimeType string, prior, next []byte, s cluster.DatabaseStorage) error {
	priorSchema, err := p.syncableSchema(mimeType, prior, s)
	if err != nil || priorSchema == nil {
		return nil // prior document unparseable, or prior kind has no schema — fail open
	}
	nextSchema, err := p.syncableSchema(mimeType, next, s)
	if err != nil || nextSchema == nil {
		return nil // next document unparseable, or next kind has no schema — fail open
	}
	return nextSchema.SchemaChange(priorSchema)
}

// syncableSchema builds a config's comparable schema from the config alone (no
// database resolution). Mirrors SyncableTopics: parse the bytes, pick the
// type-specific parser, delegate to its cluster.SyncableSchemaExtractor. Returns
// (nil, nil) when the matched parser has no schema extractor (e.g. a webhook) — the
// caller then allows the change. s is passed for type resolution only; the
// extractor never resolves the database.
func (p *Parser) syncableSchema(mimeType string, data []byte, s cluster.DatabaseStorage) (cluster.SyncableSchemaComparable, error) {
	v, err := parseBytes(mimeType, data)
	if err != nil {
		return nil, err
	}

	tipe := v.GetString("syncable.type")
	parser, ok := p.syncableParsers[tipe]
	if !ok {
		return nil, fmt.Errorf("cannot parse syncable of type: %s", tipe)
	}

	extractor, ok := parser.(cluster.SyncableSchemaExtractor)
	if !ok {
		return nil, nil // this syncable kind has no materialized schema — nothing to guard
	}
	return extractor.SchemaFromConfig(v, s)
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
