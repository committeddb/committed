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
	// EpochFloor supplies the delete-surviving per-topic refresh-epoch floor to
	// every built Ingestable (see TopicEpochReader). Wired by the node to the db;
	// left nil in tests, where an Ingestable degrades to floor 0 (first snapshot at
	// epoch 1).
	EpochFloor TopicEpochReader
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

	// Introspect the live source schema once, up front — needed both to expand a
	// map-all config and to validate that every mapping references a real column.
	// Parse always reaches the source here anyway (Preflight below connects too),
	// so this adds no new connectivity requirement.
	colsByTable, generatedByTable, err := dialect.SourceColumns(config)
	if err != nil {
		return nil, fmt.Errorf("[ingestable.parser] source columns: %w", err)
	}

	// A generated/computed column can't be replicated (the change stream omits it,
	// so it is present on the snapshot but null on every later CDC row). Refuse an
	// explicit mapping/PK of one loudly at POST; MapAllColumns skips it (below).
	if err := rejectGeneratedColumnRefs(config, generatedByTable); err != nil {
		return nil, fmt.Errorf("[ingestable.parser] %w", err)
	}

	// Expand a map-all config into explicit mappings against the live source
	// schema, freezing the column set at build time — a column added later does
	// not silently enter payloads until the config is re-POSTed. Generated columns
	// are excluded (and logged), so map-all mirrors only what committed can
	// faithfully replicate. Done before Preflight so the fully-built config is what
	// we validate and run.
	if config.MapAllColumns {
		if err := expandMapAllColumns(config, excludeGeneratedFromMapAll(colsByTable, generatedByTable)); err != nil {
			return nil, fmt.Errorf("[ingestable.parser] map all columns: %w", err)
		}
	}

	// Validate every mapping resolves (case-insensitively) to a real source
	// column, so a typo / renamed-or-dropped column / unresolvable case is a loud
	// rejection at POST rather than a silent null on every row. Runs for map-all
	// too, so it also catches a source table whose columns collide when lowercased.
	if err := validateMappingColumns(config, colsByTable); err != nil {
		return nil, fmt.Errorf("[ingestable.parser] %w", err)
	}

	// Preflight before building the worker: a source that would silently drop
	// deletes (inadequate replica identity / binlog row image) fails the build
	// here, so it degrades loudly instead of running and quietly losing deletes.
	if err := dialect.Preflight(config); err != nil {
		return nil, fmt.Errorf("[ingestable.parser] preflight: %w", err)
	}

	ingestable := New(dialect, config).WithEpochFloor(p.EpochFloor)

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
	// The ingest topic must be a USER type, never a committed-internal/system type.
	// Emitted rows carry this type, and at apply resolveType is systemType-first, so
	// an internal topic id would route user row bytes into an internal config handler
	// that Fatals on the decode mismatch — a committed, deterministic entry that
	// crash-loops every node. ParseType blocks creating such a type on a fresh
	// cluster, but a colliding type from a pre-guard binary or a restore must not be
	// usable as an ingest topic either — mirror the AddProposal / ParseType guard
	// structurally here (this is the one user-controlled type resolution the propose
	// path's guard doesn't cover).
	if cluster.IsInternal(topic) || cluster.IsReservedSystemID(topic) {
		return nil, nil, &cluster.FieldError{
			Field: "sql.topic",
			Issue: fmt.Sprintf("type %q is a committed system-type id and cannot be used as an ingest topic", topic),
		}
	}
	tipe, err := p.typer.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil {
		return nil, nil, &cluster.FieldError{
			Field: "sql.topic",
			Issue: fmt.Sprintf("type %q not found: create the type (POST /v1/type/%s) before the ingestable", topic, topic),
			Err:   err,
		}
	}

	// Required-field validation. Without these an ingestable is accepted at POST
	// but wedges: an empty primaryKey collapses every row onto the single "[]"
	// composite key (and the snapshot's `ORDER BY ""` is a SQL syntax error that
	// the snapshot loop retries forever), an empty tables list snapshots nothing,
	// and no mappings (without mapAllColumns) produces an empty payload. Fail fast
	// with an actionable FieldError instead of a silent spin.
	if len(primaryKey) == 0 {
		return nil, nil, &cluster.FieldError{
			Field: "sql.primaryKey",
			Issue: "required: an ingestable needs a primary key to build per-row entity keys",
		}
	}
	if len(tables) == 0 {
		return nil, nil, &cluster.FieldError{
			Field: "sql.tables",
			Issue: "required: list at least one source table to ingest",
		}
	}
	if len(mappings) == 0 && !mapAllColumns {
		return nil, nil, &cluster.FieldError{
			Field: "sql.mappings",
			Issue: "required: define at least one mapping, or set sql.mapAllColumns = true",
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
	// The flat form is a single topic-spec (Config.Tables above is already the
	// union for one spec). Per-table call sites resolve via config.SpecForTable;
	// nothing reads Topics until then.
	config.Topics = []TopicSpec{{
		Type:           tipe,
		Tables:         tables,
		Mappings:       mappings,
		MapAllColumns:  mapAllColumns,
		ExcludeColumns: excludeColumns,
		PrimaryKey:     primaryKey,
	}}

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
