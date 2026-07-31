package sql

import (
	"fmt"
	"strings"

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

	// Validate and expand each topic-spec independently against the one
	// introspected source schema (SourceColumns returns the union per-table map;
	// each spec filters to its own tables). The flat single-topic config is one
	// spec, so this loop runs once and is behavior-identical to the pre-routing
	// path; the [[sql.topics]] form runs it N times.
	for i := range config.Topics {
		spec := &config.Topics[i]

		// A generated/computed column can't be replicated (the change stream omits
		// it, so it is present on the snapshot but null on every later CDC row).
		// Refuse an explicit mapping/PK of one loudly at POST; MapAllColumns skips
		// it (below).
		if err := rejectGeneratedColumnRefs(spec, generatedByTable); err != nil {
			return nil, fmt.Errorf("[ingestable.parser] %w", err)
		}

		// Expand a map-all spec into explicit mappings against the live source
		// schema, freezing the column set at build time — a column added later
		// does not silently enter payloads until the config is re-POSTed. Generated
		// columns are excluded (and logged), so map-all mirrors only what committed
		// can faithfully replicate. Done before Preflight so the fully-built config
		// is what we validate and run.
		if spec.MapAllColumns {
			if err := expandMapAllColumns(spec, excludeGeneratedFromMapAll(colsByTable, generatedByTable)); err != nil {
				return nil, fmt.Errorf("[ingestable.parser] map all columns: %w", err)
			}
		}

		// Validate every mapping resolves (case-insensitively) to a real source
		// column, so a typo / renamed-or-dropped column / unresolvable case is a
		// loud rejection at POST rather than a silent null on every row. Runs for
		// map-all too, so it also catches a source table whose columns collide when
		// lowercased.
		if err := validateMappingColumns(spec, colsByTable); err != nil {
			return nil, fmt.Errorf("[ingestable.parser] %w", err)
		}
	}

	// The runtime still reads the singular Config.Mappings until per-spec entity
	// build lands; keep it in sync with the (single) spec, whose Mappings map-all
	// expansion may have rewritten. Harmless for the flat form (one spec); the
	// [[sql.topics]] path routes per spec and does not depend on this.
	config.Mappings = config.Topics[0].Mappings

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
	dialect, ok := p.Dialects[dialectName]
	if !ok {
		return nil, nil, cluster.UnknownDialectError(dialectName, dialectNames(p.Dialects))
	}
	connectionString := v.GetString("sql.connectionString")
	options := v.GetStringMapString("sql." + dialectName)

	// Two mutually-exclusive config shapes share the dialect / connectionString /
	// sql.<dialect> options: the flat single-topic form (sql.topic + sql.tables +
	// sql.primaryKey + sql.mappings) and the [[sql.topics]] array, one self-contained
	// entry per topic. The presence of [[sql.topics]] selects the multi-topic path.
	if v.IsSet("sql.topics") {
		return p.parseTopicsConfig(v, dialect, connectionString, options)
	}
	return p.parseFlatConfig(v, dialect, connectionString, options)
}

// parseFlatConfig parses the flat single-topic form into a one-element Topics. It is
// the original config shape; the singular Config fields and Topics[0] carry the same
// values.
func (p *IngestableParser) parseFlatConfig(v *cluster.ParsedConfig, dialect Dialect, connectionString string, options map[string]string) (*Config, Dialect, error) {
	topic := v.GetString("sql.topic")
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

	if topic == "" {
		return nil, nil, &cluster.FieldError{Field: "sql.topic", Issue: "required"}
	}
	tipe, err := p.resolveTopicType(topic, "sql.topic")
	if err != nil {
		return nil, nil, err
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
	// union for one spec). Per-table call sites resolve via config.SpecForTable.
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

// topicSpecTOML is the decode shape of one [[sql.topics]] entry. PrimaryKey is
// decoded as any so a scalar (primaryKey = "id") and a list (primaryKey = ["a","b"])
// both parse — coerceStringSlice normalizes them, matching the flat form's
// GetStringSlice. Field names match case-insensitively (mapstructure), the same
// tolerance the flat form has; the nested [[sql.topics.mappings]] decodes into
// []Mapping via its mapstructure tags exactly like the flat sql.mappings.
type topicSpecTOML struct {
	Topic          string    `mapstructure:"topic"`
	Tables         []string  `mapstructure:"tables"`
	PrimaryKey     any       `mapstructure:"primaryKey"`
	Mappings       []Mapping `mapstructure:"mappings"`
	MapAllColumns  bool      `mapstructure:"mapAllColumns"`
	ExcludeColumns []string  `mapstructure:"excludeColumns"`
}

// parseTopicsConfig parses the [[sql.topics]] array — one self-contained topic-spec
// per entry — into a multi-element Topics. It rejects a config that mixes the flat
// per-topic fields with [[sql.topics]] (ambiguous), a table routed to more than one
// topic, and a topic id claimed by more than one entry. The singular Config fields
// mirror Topics[0] for the few remaining flat-compat readers (topicID/topicName, the
// park-log fields, chunkTag hashes ALL specs).
func (p *IngestableParser) parseTopicsConfig(v *cluster.ParsedConfig, dialect Dialect, connectionString string, options map[string]string) (*Config, Dialect, error) {
	// Mutual exclusivity: the [[sql.topics]] form owns the per-topic fields; a stray
	// flat-level one is ambiguous (which shape wins?), so reject it loudly rather
	// than silently ignore it. dialect / connectionString / sql.<dialect> options
	// stay top-level (shared) and are not per-topic.
	for _, f := range []string{"sql.topic", "sql.tables", "sql.primaryKey", "sql.mappings", "sql.mapAllColumns", "sql.excludeColumns"} {
		if v.IsSet(f) {
			return nil, nil, &cluster.FieldError{
				Field: f,
				Issue: "cannot be set alongside [[sql.topics]]: move per-topic fields inside each [[sql.topics]] entry, or drop [[sql.topics]] and use the flat single-topic form",
			}
		}
	}

	var raw []topicSpecTOML
	if err := v.UnmarshalKey("sql.topics", &raw); err != nil {
		return nil, nil, fmt.Errorf("parse sql.topics: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil, &cluster.FieldError{Field: "sql.topics", Issue: "required: define at least one [[sql.topics]] entry"}
	}

	config := &Config{
		ConnectionString: connectionString,
		Options:          options,
	}
	seenTopic := make(map[string]bool, len(raw))
	tableOwner := make(map[string]string) // lowercased table -> owning topic id

	for i := range raw {
		spec, err := p.buildTopicSpec(&raw[i], i)
		if err != nil {
			return nil, nil, err
		}
		id := spec.Type.ID
		if seenTopic[id] {
			return nil, nil, &cluster.FieldError{
				Field: fmt.Sprintf("sql.topics[%d].topic", i),
				Issue: fmt.Sprintf("topic %q is already defined by an earlier [[sql.topics]] entry; each topic appears once", id),
			}
		}
		seenTopic[id] = true
		// Every table feeds exactly one topic — the routing resolvers (SpecForTable,
		// specFor, specForRelation) assume it, and a table in two specs would
		// silently drop one topic's rows. Reject the conflict here, loudly.
		for _, t := range spec.Tables {
			key := strings.ToLower(t)
			if owner, ok := tableOwner[key]; ok {
				return nil, nil, &cluster.FieldError{
					Field: fmt.Sprintf("sql.topics[%d].tables", i),
					Issue: fmt.Sprintf("table %q is already routed to topic %q; a table may feed only one topic", t, owner),
				}
			}
			tableOwner[key] = id
		}
		config.Topics = append(config.Topics, *spec)
		config.Tables = append(config.Tables, spec.Tables...)
	}

	// Mirror the singular fields to the first spec for the remaining flat-compat
	// readers (topicID/topicName, the park-log fields, the checkReplicaIdentity
	// fallback). chunkTag hashes every spec, and Parse re-syncs Config.Mappings from
	// Topics[0] after map-all expansion.
	first := &config.Topics[0]
	config.Type = first.Type
	config.Mappings = first.Mappings
	config.MapAllColumns = first.MapAllColumns
	config.ExcludeColumns = first.ExcludeColumns
	config.PrimaryKey = first.PrimaryKey

	return config, dialect, nil
}

// buildTopicSpec validates one decoded [[sql.topics]] entry and resolves its topic
// type, applying the same required-field checks and IsInternal/reserved guard the
// flat form applies. idx positions the FieldError at sql.topics[idx].<field>.
func (p *IngestableParser) buildTopicSpec(raw *topicSpecTOML, idx int) (*TopicSpec, error) {
	field := func(name string) string { return fmt.Sprintf("sql.topics[%d].%s", idx, name) }

	if raw.Topic == "" {
		return nil, &cluster.FieldError{Field: field("topic"), Issue: "required"}
	}
	tipe, err := p.resolveTopicType(raw.Topic, field("topic"))
	if err != nil {
		return nil, err
	}

	primaryKey := coerceStringSlice(raw.PrimaryKey)
	if len(raw.ExcludeColumns) > 0 && !raw.MapAllColumns {
		return nil, &cluster.FieldError{Field: field("excludeColumns"), Issue: "requires mapAllColumns = true"}
	}
	if len(primaryKey) == 0 {
		return nil, &cluster.FieldError{Field: field("primaryKey"), Issue: "required: a topic needs a primary key to build per-row entity keys"}
	}
	if len(raw.Tables) == 0 {
		return nil, &cluster.FieldError{Field: field("tables"), Issue: "required: list at least one source table for this topic"}
	}
	if len(raw.Mappings) == 0 && !raw.MapAllColumns {
		return nil, &cluster.FieldError{Field: field("mappings"), Issue: "required: define at least one mapping, or set mapAllColumns = true"}
	}

	return &TopicSpec{
		Type:           tipe,
		Tables:         raw.Tables,
		Mappings:       raw.Mappings,
		MapAllColumns:  raw.MapAllColumns,
		ExcludeColumns: raw.ExcludeColumns,
		PrimaryKey:     primaryKey,
	}, nil
}

// resolveTopicType resolves a topic id to its Type, rejecting a committed-internal /
// reserved system id first. field names the config path for the FieldError.
//
// The ingest topic must be a USER type, never a committed-internal/system type:
// emitted rows carry this type, and at apply resolveType is systemType-first, so an
// internal topic id would route user row bytes into an internal config handler that
// Fatals on the decode mismatch — a committed, deterministic entry that crash-loops
// every node. ParseType blocks creating such a type on a fresh cluster, but a
// colliding type from a pre-guard binary or a restore must not be usable as an
// ingest topic either — mirror the AddProposal / ParseType guard structurally here.
func (p *IngestableParser) resolveTopicType(topic, field string) (*cluster.Type, error) {
	if cluster.IsInternal(topic) || cluster.IsReservedSystemID(topic) {
		return nil, &cluster.FieldError{
			Field: field,
			Issue: fmt.Sprintf("type %q is a committed system-type id and cannot be used as an ingest topic", topic),
		}
	}
	tipe, err := p.typer.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil {
		return nil, &cluster.FieldError{
			Field: field,
			Issue: fmt.Sprintf("type %q not found: create the type (POST /v1/type/%s) before the ingestable", topic, topic),
			Err:   err,
		}
	}
	return tipe, nil
}

// coerceStringSlice normalizes a scalar-or-list value decoded as any (a
// [[sql.topics]] primaryKey) into a string slice, matching ParsedConfig's
// GetStringSlice: a list stringifies element-wise, a bare string splits on
// whitespace (a no-op for a single column name).
func coerceStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case string:
		return strings.Fields(t)
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			} else {
				out = append(out, fmt.Sprintf("%v", e))
			}
		}
		return out
	}
	return nil
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
