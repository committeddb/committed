package sql

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

//counterfeiter:generate . Typer
type Typer interface {
	ResolveType(ref cluster.TypeRef) (*cluster.Type, error)
}

type IngestableParser struct {
	Dialects map[string]Dialect
	typer    Typer
}

func NewIngestableParser(t Typer) *IngestableParser {
	dialects := make(map[string]Dialect)
	return &IngestableParser{Dialects: dialects, typer: t}
}

func (p *IngestableParser) Parse(v *cluster.ParsedConfig) (cluster.Ingestable, error) {
	config, dialect, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}

	ingestable := New(dialect, config)

	return ingestable, nil
}

func (p *IngestableParser) ParseConfig(v *cluster.ParsedConfig) (*Config, Dialect, error) {
	dialectName := v.GetString("sql.dialect")
	topic := v.GetString("sql.topic")
	connectionString := v.GetString("sql.connectionString")
	primaryKey := v.GetString("sql.primaryKey")

	var mappings []Mapping
	if err := v.UnmarshalKey("sql.mappings", &mappings); err != nil {
		return nil, nil, fmt.Errorf("parse sql.mappings: %w", err)
	}

	tables := v.GetStringSlice("sql.tables")
	options := v.GetStringMapString("sql." + dialectName)

	dialect, ok := p.Dialects[dialectName]
	if !ok {
		return nil, nil, fmt.Errorf("dialect %s not found", dialectName)
	}

	tipe, err := p.typer.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil {
		return nil, nil, err
	}

	config := &Config{
		ConnectionString: connectionString,
		Type:             tipe,
		Mappings:         mappings,
		PrimaryKey:       primaryKey,
		Tables:           tables,
		Options:          options,
	}

	return config, dialect, nil
}
