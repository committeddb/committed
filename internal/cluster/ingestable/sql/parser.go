package sql

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

//counterfeiter:generate . Typer
type Typer interface {
	Type(id string) (*cluster.Type, error)
}

type IngestableParser struct {
	Dialects map[string]Dialect
	typer    Typer
}

func NewIngestableParser(t Typer) *IngestableParser {
	dialects := make(map[string]Dialect)
	return &IngestableParser{Dialects: dialects, typer: t}
}

func (p *IngestableParser) Parse(v *viper.Viper) (cluster.Ingestable, error) {
	config, dialect, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}

	ingestable := New(dialect, config)

	return ingestable, nil
}

func (p *IngestableParser) ParseConfig(v *viper.Viper) (*Config, Dialect, error) {
	dialectName := v.GetString("sql.dialect")
	topic := v.GetString("sql.topic")
	connectionString := v.GetString("sql.connectionString")
	primaryKey := v.GetString("sql.primaryKey")

	var mappings []Mapping
	for _, item := range v.Get("sql.mappings").([]interface{}) {
		m := item.(map[string]interface{})
		mapping := Mapping{
			JSONName:  m["jsonName"].(string),
			SQLColumn: m["column"].(string),
		}
		mappings = append(mappings, mapping)
	}

	dialect, ok := p.Dialects[dialectName]
	if !ok {
		return nil, nil, fmt.Errorf("dialect %s not found", dialectName)
	}

	tipe, err := p.typer.Type(topic)
	if err != nil {
		return nil, nil, err
	}

	config := &Config{
		ConnectionString: connectionString,
		Type:             tipe,
		Mappings:         mappings,
		PrimaryKey:       primaryKey,
	}

	return config, dialect, nil
}
