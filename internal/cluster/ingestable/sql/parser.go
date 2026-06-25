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

	// Expand a map-all config into explicit mappings against the live source
	// schema, freezing the column set at build time — a column added later does
	// not silently enter payloads until the config is re-POSTed. Done before
	// Preflight so the fully-built config is what we validate and run.
	if config.MapAllColumns {
		colsByTable, err := dialect.SourceColumns(config)
		if err != nil {
			return nil, fmt.Errorf("[ingestable.parser] map all columns: %w", err)
		}
		if err := expandMapAllColumns(config, colsByTable); err != nil {
			return nil, fmt.Errorf("[ingestable.parser] map all columns: %w", err)
		}
	}

	// Preflight before building the worker: a source that would silently drop
	// deletes (inadequate replica identity / binlog row image) fails the build
	// here, so it degrades loudly instead of running and quietly losing deletes.
	if err := dialect.Preflight(config); err != nil {
		return nil, fmt.Errorf("[ingestable.parser] preflight: %w", err)
	}

	ingestable := New(dialect, config)

	return ingestable, nil
}

func (p *IngestableParser) ParseConfig(v *cluster.ParsedConfig) (*Config, Dialect, error) {
	dialectName := v.GetString("sql.dialect")
	topic := v.GetString("sql.topic")
	connectionString := v.GetString("sql.connectionString")
	// primaryKey accepts a scalar (primaryKey = "pk") or a list
	// (primaryKey = ["tconst", "ordering"]) for composite keys; GetStringSlice
	// normalizes both. Column names have no spaces, so the scalar path's
	// whitespace split is a no-op.
	primaryKey := v.GetStringSlice("sql.primaryKey")

	var mappings []Mapping
	if err := v.UnmarshalKey("sql.mappings", &mappings); err != nil {
		return nil, nil, fmt.Errorf("parse sql.mappings: %w", err)
	}

	// mapAllColumns infers a jsonName=column mapping for every source column;
	// any listed mappings then override the inferred one (a rename), and
	// excludeColumns drops columns from the inferred set. The parser expands all
	// this against the live schema in Parse.
	mapAllColumns := v.GetBool("sql.mapAllColumns")
	excludeColumns := v.GetStringSlice("sql.excludeColumns")
	if len(excludeColumns) > 0 && !mapAllColumns {
		return nil, nil, fmt.Errorf("sql.excludeColumns requires sql.mapAllColumns = true")
	}

	tables := v.GetStringSlice("sql.tables")
	options := v.GetStringMapString("sql." + dialectName)

	dialect, ok := p.Dialects[dialectName]
	if !ok {
		return nil, nil, cluster.UnknownDialectError(dialectName, dialectNames(p.Dialects))
	}

	if topic == "" {
		return nil, nil, &cluster.FieldError{Field: "sql.topic", Issue: "required"}
	}
	tipe, err := p.typer.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil {
		return nil, nil, &cluster.FieldError{
			Field: "sql.topic",
			Issue: fmt.Sprintf("type %q not found: create the type (POST /v1/type/%s) before the ingestable", topic, topic),
			Err:   err,
		}
	}

	config := &Config{
		ConnectionString: connectionString,
		Type:             tipe,
		Mappings:         mappings,
		MapAllColumns:    mapAllColumns,
		ExcludeColumns:   excludeColumns,
		PrimaryKey:       primaryKey,
		Tables:           tables,
		Options:          options,
	}

	return config, dialect, nil
}

// dialectNames returns the registered dialect names, for the
// "valid: ..." list in an unknown-dialect error.
func dialectNames(m map[string]Dialect) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return names
}
