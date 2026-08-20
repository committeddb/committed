package sql

import (
	"fmt"
	"reflect"
	"strings"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// ProjectionSyncableParser parses projection syncable TOML. The canonical
// type is "projection"; "sql-projection" is accepted as a deprecation
// alias (both registered in cmd/node.go). Metrics is optional (nil skips
// instrumentation); when set it counts entity-kind misuse at parse time
// and is handed to the Projection for unmatched-rule ticks at sync time.
type ProjectionSyncableParser struct {
	Metrics *metrics.Metrics
	// StoreDir is the node's stage-store directory
	// (<dataDir>/projections), threaded from cmd/node at registration. A
	// projection with internal stages REQUIRES it — an empty StoreDir
	// rejects stage configs at Init rather than folding without state.
	StoreDir string
}

const (
	canonicalProjectionType  = "projection"
	deprecatedProjectionType = "sql-projection"
)

// projectionSection resolves which TOML table this config's projection
// vocabulary lives in. The section name follows the syncable type string
// (the {type}.topic convention the pipeline linkage relies on): canonical
// `type = "projection"` reads [projection]; the deprecated
// `type = "sql-projection"` keeps reading [sql-projection] for the
// deprecation period. When no type is in scope (direct parser callers
// hand in bare documents), whichever section spelling is present wins,
// canonical by default. A half-renamed config (type of one spelling,
// section of the other) is caught by parseProjectionConfigFields with a
// targeted error rather than a cascade of missing-field failures.
func projectionSection(v *cluster.ParsedConfig) string {
	switch v.GetString("syncable.type") {
	case deprecatedProjectionType:
		return deprecatedProjectionType
	case canonicalProjectionType:
		return canonicalProjectionType
	}
	if len(v.SectionKeys(canonicalProjectionType)) == 0 && len(v.SectionKeys(deprecatedProjectionType)) > 0 {
		return deprecatedProjectionType
	}
	return canonicalProjectionType
}

func (p *ProjectionSyncableParser) Parse(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v, storage)
	if err != nil {
		return nil, err
	}

	db, ok := config.Database.(*DB)
	if !ok {
		return nil, fmt.Errorf("expected sql.DB but was %s", reflect.TypeOf(config.Database))
	}

	projection := NewProjection(db, config, p.Metrics, v.GetString("syncable.name"))
	projection.SetStoreDir(p.StoreDir)
	if err := projection.Init(); err != nil {
		return nil, fmt.Errorf("[projection.parser] init: %w", err)
	}

	return projection, nil
}

// rawProjectionRule is the TOML decode shape: when is either a string
// (discriminator shorthand) or an array of { path, equals } tables, so
// it decodes as any and normalizeWhen resolves it.
type rawProjectionRule struct {
	When any             `mapstructure:"when"`
	Set  []ProjectionSet `mapstructure:"set"`
}

// rawProjectionSource is the TOML decode shape of one [[projection.source]]
// block: a topic (the discriminator), its correlation keyPath, its onDelete
// behavior, an optional source-level when filter, and either nested rules (a
// scalar fold) or an aggregate (a collection fold). when decodes as any so it
// resolves the same string-shorthand / explicit-clauses forms a rule's when
// does.
type rawProjectionSource struct {
	Topic     string                  `mapstructure:"topic"`
	From      string                  `mapstructure:"from"`
	KeyPath   any                     `mapstructure:"keyPath"`
	OnDelete  string                  `mapstructure:"onDelete"`
	When      any                     `mapstructure:"when"`
	Rules     []rawProjectionRule     `mapstructure:"rules"`
	Aggregate *rawProjectionAggregate `mapstructure:"aggregate"`
	Lookup    *rawProjectionLookup    `mapstructure:"lookup"`
	ForEach   string                  `mapstructure:"forEach"`
	RowOwner  bool                    `mapstructure:"rowOwner"`
}

// rawProjectionAggregate is the TOML decode shape of a source's
// [projection.source.aggregate] block. Element is an array-of-tables (each
// a plain { field, from } or an enriched { field, lookup, on, select }) rather
// than an inline map so its output field names survive viper's map-key
// lowercasing byte-exact.
type rawProjectionAggregate struct {
	Column         string                   `mapstructure:"column"`
	ElementKey     string                   `mapstructure:"elementKey"`
	ElementKeyType string                   `mapstructure:"elementKeyType"`
	Element        []ProjectionElementField `mapstructure:"element"`
	Scalar         []ProjectionScalar       `mapstructure:"scalar"`
}

// rawProjectionLookup is the TOML decode shape of a source's
// [projection.source.lookup] block: the dimension's id (referenced by
// element enrichments) and its stored fields.
type rawProjectionLookup struct {
	Name   string                   `mapstructure:"name"`
	Fields []ProjectionElementField `mapstructure:"field"`
}

// TopicsFromConfig implements cluster.SyncableTopicExtractor: a projection
// consumes every topic named across its sources (multi-source folds several
// topics into one row), plus the back-compat single-source top-level topic.
// Read straight from the config (no Init / no DDL) and best-effort — a source
// block that won't decode is skipped rather than failing enumeration — so
// config-change enumeration runs no I/O. Order-preserving, de-duplicated.
func (p *ProjectionSyncableParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	seen := map[string]bool{}
	var topics []string
	add := func(t string) {
		if t != "" && !seen[t] {
			seen[t] = true
			topics = append(topics, t)
		}
	}

	section := projectionSection(v)
	var rawSources []rawProjectionSource
	if err := v.UnmarshalKeyLenient(section+".source", &rawSources); err == nil { // topic peek — admission strictness lives in parseProjectionSources
		for _, rs := range rawSources {
			add(rs.Topic)
		}
	}
	// Back-compat single-source shape: top-level topic (used when no source
	// blocks are present).
	add(v.GetString(section + ".topic"))
	return topics
}

// DatabasesFromConfig implements cluster.SyncableDatabaseExtractor: a projection
// writes to the single destination database named at {section}.db. Read
// straight from the config so a database connection change can enumerate the
// syncables that captured its pool.
func (p *ProjectionSyncableParser) DatabasesFromConfig(v *cluster.ParsedConfig) []string {
	db := v.GetString(projectionSection(v) + ".db")
	if db == "" {
		return nil
	}
	return []string{db}
}

// parseProjectionConfigFields reads the schema-determining config fields WITHOUT
// resolving the destination database, so a schema comparison (the config-change
// guard, via SchemaFromConfig) works even when the config's database secret is
// unresolvable on this node. storage is used only for type resolution (a `when`
// discriminator shorthand), never for storage.Database. Database is left nil;
// ParseConfig resolves and sets it.
// projectionSectionKeys is the complete vocabulary of the flat projection
// table — [projection], or [sql-projection] under the deprecated spelling
// (lowercased; key matching is case-insensitive).
// The struct-decoded subtrees (columns/rules/source) enforce their own
// vocabularies via the strict UnmarshalKey; this set covers the keys the
// parser reads flatly — a key outside it is rejected loudly rather than
// silently ignored (the field incident: `where` and `emitTopic` returned
// 200 and were inert). GROW THIS SET when adding a config key.
var projectionSectionKeys = map[string]bool{
	"db": true, "table": true, "primarykey": true, "topic": true,
	"keypath": true, "rules": true, "columns": true, "source": true,
	"stage": true,
}

// rawProjectionStage is the TOML decode shape of one [[projection.stage]]
// block. keyPath and when decode as any for the same scalar-or-list /
// shorthand-or-clauses reasons the source shapes do.
type rawProjectionStage struct {
	Name        string         `mapstructure:"name"`
	From        string         `mapstructure:"from"`
	KeyPath     any            `mapstructure:"keyPath"`
	When        any            `mapstructure:"when"`
	DeleteWhen  any            `mapstructure:"deleteWhen"`
	Reduce      string         `mapstructure:"reduce"`
	OrderBy     string         `mapstructure:"orderBy"`
	OrderByType string         `mapstructure:"orderByType"`
	TieBy       string         `mapstructure:"tieBy"`
	TieByType   string         `mapstructure:"tieByType"`
	Join        []rawStageJoin `mapstructure:"join"`
	Emit        []StageEmit    `mapstructure:"emit"`
	ForEach     string         `mapstructure:"forEach"`
	ElementKey  string         `mapstructure:"elementKey"`
}

// rawStageJoin is the TOML decode shape of one [[{section}.stage.join]]
// block: on decodes as any so a single path stays a scalar and a
// composite key is a list — the keyPath idiom.
type rawStageJoin struct {
	Topic  string       `mapstructure:"topic"`
	From   string       `mapstructure:"from"`
	On     any          `mapstructure:"on"`
	Absent bool         `mapstructure:"absent"`
	Where  []WhenClause `mapstructure:"where"`
}

// parseProjectionStages decodes the [[{section}.stage]] blocks.
func parseProjectionStages(v *cluster.ParsedConfig, storage cluster.DatabaseStorage, section string) ([]ProjectionStage, error) {
	var raw []rawProjectionStage
	if err := v.UnmarshalKey(section+".stage", &raw); err != nil {
		return nil, fmt.Errorf("parse %s.stage: %w", section, err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	stages := make([]ProjectionStage, 0, len(raw))
	for i, rs := range raw {
		when, err := normalizeWhen(rs.When, storage, rs.From)
		if err != nil {
			return nil, fmt.Errorf("stage %d (%q): when: %w", i+1, rs.Name, err)
		}
		deleteWhen, err := normalizeWhen(rs.DeleteWhen, storage, rs.From)
		if err != nil {
			return nil, fmt.Errorf("stage %d (%q): deleteWhen: %w", i+1, rs.Name, err)
		}
		stages = append(stages, ProjectionStage{
			Name:        rs.Name,
			From:        rs.From,
			KeyPath:     pathOrList(rs.KeyPath),
			When:        when,
			DeleteWhen:  deleteWhen,
			Reduce:      strings.ToLower(rs.Reduce),
			OrderBy:     rs.OrderBy,
			OrderByType: strings.ToLower(rs.OrderByType),
			TieBy:       rs.TieBy,
			TieByType:   strings.ToLower(rs.TieByType),
			Joins:       stageJoins(rs.Join),
			Emit:        rs.Emit,
			ForEach:     rs.ForEach,
			ElementKey:  rs.ElementKey,
		})
	}
	return stages, nil
}

func parseProjectionConfigFields(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (*ProjectionConfig, error) {
	section := projectionSection(v)

	// One config uses one spelling — both sections at once is never right.
	if len(v.SectionKeys(canonicalProjectionType)) > 0 && len(v.SectionKeys(deprecatedProjectionType)) > 0 {
		return nil, &cluster.FieldError{
			Field: deprecatedProjectionType,
			Issue: "both [projection] and [sql-projection] sections are present — a config uses one spelling; delete the deprecated [sql-projection] section",
		}
	}

	// A half-renamed config — type of one spelling, section of the other —
	// would otherwise read an absent table and die on missing required
	// fields; name the actual mistake instead.
	other := canonicalProjectionType
	if section == canonicalProjectionType {
		other = deprecatedProjectionType
	}
	if len(v.SectionKeys(section)) == 0 && len(v.SectionKeys(other)) > 0 {
		return nil, &cluster.FieldError{
			Field: other,
			Issue: fmt.Sprintf("section spelling does not match syncable type %q — rename the section to [%s] (type and section always use the same spelling)", v.GetString("syncable.type"), section),
		}
	}

	for _, k := range v.SectionKeys(section) {
		if !projectionSectionKeys[k] {
			return nil, &cluster.FieldError{
				Field: section + "." + k,
				Issue: "unknown key — not part of the projection vocabulary (check the spelling against the docs; unknown keys are rejected rather than silently ignored)",
			}
		}
	}

	var columns []ProjectionColumn
	if err := v.UnmarshalKey(section+".columns", &columns); err != nil {
		return nil, fmt.Errorf("[projection.parser] parse %s.columns: %w", section, err)
	}

	sources, err := parseProjectionSources(v, storage, section)
	if err != nil {
		return nil, fmt.Errorf("[projection.parser] %w", err)
	}
	stages, err := parseProjectionStages(v, storage, section)
	if err != nil {
		return nil, fmt.Errorf("[projection.parser] %w", err)
	}

	config := &ProjectionConfig{
		DatabaseID: v.GetString(section + ".db"),
		Table:      v.GetString(section + ".table"),
		// Scalar-or-list, like the plain syncable: primaryKey = "id" and
		// primaryKey = ["tenant_id", "visit_id"] both parse; for a composite
		// key the list order is the tombstone-decode contract (see
		// ProjectionConfig.PrimaryKey).
		PrimaryKey: v.GetStringSlice(section + ".primaryKey"),
		Columns:    columns,
		Sources:    sources,
		Stages:     stages,
	}
	config.applyDefaults()
	return config, nil
}

// SchemaFromConfig implements cluster.SyncableSchemaExtractor: it builds the
// projection's comparable destination shape + identity from the config document
// alone (no database resolution), for the config-change guard.
func (p *ProjectionSyncableParser) SchemaFromConfig(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (cluster.SyncableSchemaComparable, error) {
	config, err := parseProjectionConfigFields(v, storage)
	if err != nil {
		return nil, err
	}
	schema := schemaOf(config.ddlConfig())
	schema.ProjectionShape = config.projectionShapeFingerprint()
	return &schemaComparable{schema: schema, identity: projectionIdentity(config)}, nil
}

func (p *ProjectionSyncableParser) ParseConfig(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (*ProjectionConfig, error) {
	config, err := parseProjectionConfigFields(v, storage)
	if err != nil {
		return nil, err
	}

	db, err := storage.Database(config.DatabaseID)
	if err != nil {
		return nil, describeDatabaseErr(err, config.DatabaseID)
	}
	config.Database = db

	if err := validateProjectionConfig(config); err != nil {
		return nil, fmt.Errorf("[projection.parser] %w", err)
	}

	// Warn entity-kind misuse only for a single-source projection: a
	// multi-source fold legitimately consumes Snapshot/Revision source topics
	// (folding several normalized topics into one denormalized row is the point).
	if len(config.Sources) == 1 {
		p.warnKindMisuse(storage, config.Sources[0].Topic)
	}

	return config, nil
}

// parseProjectionSources reads either the multi-source
// `[[projection.source]]` blocks or — for back-compat — the single-source
// top-level `topic` / `keyPath` / `rules`. Exactly one shape is allowed:
// mixing them is a loud config error, not a silent precedence — the field
// incident's three keyPath probes died with 99 dead letters each because a
// top-level keyPath was silently ignored once source blocks were present.
func parseProjectionSources(v *cluster.ParsedConfig, storage cluster.DatabaseStorage, section string) ([]ProjectionSource, error) {
	var rawSources []rawProjectionSource
	if err := v.UnmarshalKey(section+".source", &rawSources); err != nil {
		return nil, fmt.Errorf("parse %s.source: %w", section, err)
	}

	if len(rawSources) > 0 {
		for _, shorthand := range []string{"topic", "keyPath", "rules"} {
			if v.Get(section+"."+shorthand) != nil {
				return nil, &cluster.FieldError{
					Field: section + "." + shorthand,
					Issue: fmt.Sprintf("cannot be combined with [[%s.source]] blocks — the single-source shorthand and the multi-source form are mutually exclusive; move this into a source block", section),
				}
			}
		}
		sources := make([]ProjectionSource, 0, len(rawSources))
		for si, rs := range rawSources {
			when, err := normalizeWhen(rs.When, storage, rs.Topic)
			if err != nil {
				return nil, fmt.Errorf("source %d (topic %q): when: %w", si+1, rs.Topic, err)
			}
			src := ProjectionSource{
				Topic:     rs.Topic,
				FromStage: rs.From,
				KeyPath:   pathOrList(rs.KeyPath),
				OnDelete:  rs.OnDelete,
				When:      when,
				ForEach:   rs.ForEach,
				RowOwner:  rs.RowOwner,
			}
			// Populate rules, aggregate, and lookup independently so a source that
			// declares more than one is caught by validation (a source has exactly
			// one kind), not silently resolved by parse order.
			if len(rs.Rules) > 0 {
				rules, err := normalizeRules(rs.Rules, storage, rs.Topic)
				if err != nil {
					return nil, fmt.Errorf("source %d (topic %q): %w", si+1, rs.Topic, err)
				}
				src.Rules = rules
			}
			if rs.Aggregate != nil {
				src.Aggregate = &ProjectionAggregate{
					Column:         rs.Aggregate.Column,
					Element:        rs.Aggregate.Element,
					ElementKey:     rs.Aggregate.ElementKey,
					ElementKeyType: strings.ToLower(rs.Aggregate.ElementKeyType),
					Scalars:        normalizeScalars(rs.Aggregate.Scalar),
				}
			}
			if rs.Lookup != nil {
				src.Lookup = &ProjectionLookup{
					Name:   rs.Lookup.Name,
					Fields: rs.Lookup.Fields,
				}
			}
			sources = append(sources, src)
		}
		return sources, nil
	}

	// Back-compat single-source: top-level topic/keyPath/rules.
	topic := v.GetString(section + ".topic")
	keyPath := pathOrList(v.Get(section + ".keyPath"))
	var rawRules []rawProjectionRule
	if err := v.UnmarshalKey(section+".rules", &rawRules); err != nil {
		return nil, fmt.Errorf("parse %s.rules: %w", section, err)
	}
	rules, err := normalizeRules(rawRules, storage, topic)
	if err != nil {
		return nil, err
	}
	return []ProjectionSource{{Topic: topic, KeyPath: keyPath, Rules: rules}}, nil
}

// normalizeScalars lower-cases each scalar's fn so config casing never
// matters downstream (validation and the dialects match exact strings).
func normalizeScalars(scalars []ProjectionScalar) []ProjectionScalar {
	out := make([]ProjectionScalar, len(scalars))
	for i, sc := range scalars {
		sc.Fn = strings.ToLower(sc.Fn)
		out[i] = sc
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stageJoins maps the raw join blocks onto the engine shape.
func stageJoins(raw []rawStageJoin) []StageJoin {
	if len(raw) == 0 {
		return nil
	}
	out := make([]StageJoin, len(raw))
	for i, rj := range raw {
		out[i] = StageJoin{Topic: rj.Topic, From: rj.From, On: pathOrList(rj.On), Absent: rj.Absent, Where: rj.Where}
	}
	return out
}

// pathOrList coerces a config value that may be a scalar or a list into
// []string — keyPath = "$.id" and keyPath = ["$.tenant_id", "$.visit_id"]
// both parse. Unlike GetStringSlice's scalar arm (which whitespace-splits,
// fine for column names), a scalar stays ONE value: a jsonpath may
// legitimately contain a space inside a quoted segment.
func pathOrList(v any) []string {
	switch x := v.(type) {
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, fmt.Sprintf("%v", e))
		}
		return out
	}
	return nil
}

// normalizeRules turns raw TOML rules into ProjectionRules, resolving each
// rule's when against the given topic's discriminator (for the shorthand form).
func normalizeRules(rawRules []rawProjectionRule, storage cluster.DatabaseStorage, topic string) ([]ProjectionRule, error) {
	rules := make([]ProjectionRule, 0, len(rawRules))
	for i, raw := range rawRules {
		when, err := normalizeWhen(raw.When, storage, topic)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
		rules = append(rules, ProjectionRule{When: when, Set: raw.Set})
	}
	return rules, nil
}

// normalizeWhen turns the raw TOML when into match clauses. Two forms:
//
//	when = "tenant.created"                          — discriminator shorthand
//	when = [ { path = "$.x", equals = "y" }, … ]     — explicit clauses
//
// The shorthand resolves against the topic type's declared
// discriminator (the contract type-kinds records for exactly this
// consumer): equality on the discriminator path.
func normalizeWhen(raw any, storage cluster.DatabaseStorage, topic string) ([]WhenClause, error) {
	switch w := raw.(type) {
	case nil:
		// No when → the rule matches every event of its source. The topic is the
		// discriminator (a source only ever sees its own topic's events), so a
		// snapshot source with one event shape — the multi-source fold case —
		// needs no in-payload predicate. matchWhen(nil) is vacuously true.
		return nil, nil
	case string:
		discriminator, err := discriminatorFor(storage, topic)
		if err != nil {
			return nil, fmt.Errorf("when shorthand %q: %w", w, err)
		}
		return []WhenClause{{Path: discriminator, Equals: w}}, nil
	case []any:
		clauses := make([]WhenClause, 0, len(w))
		for _, item := range w {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("when entries must be tables of { path = \"$.…\", equals = <literal> }; got %T", item)
			}
			var clause WhenClause
			for k, val := range m {
				// Field names match case-insensitively (decode
				// tolerance); the path VALUE stays byte-exact.
				switch {
				case strings.EqualFold(k, "path"):
					s, ok := val.(string)
					if !ok {
						return nil, fmt.Errorf("when path must be a string; got %T", val)
					}
					clause.Path = s
				case strings.EqualFold(k, "equals"):
					clause.Equals = val
				case strings.EqualFold(k, "notEquals"):
					clause.NotEquals = val
				case strings.EqualFold(k, "greaterThan"):
					clause.GreaterThan = val
				case strings.EqualFold(k, "lessThan"):
					clause.LessThan = val
				case strings.EqualFold(k, "null"):
					b, ok := val.(bool)
					if !ok {
						return nil, fmt.Errorf("when null must be a boolean; got %T", val)
					}
					// An explicit false would silently mean nothing
					// ("is not null" does not exist), so reject it
					// rather than let a misread config parse.
					if !b {
						return nil, fmt.Errorf("when null = false is not a predicate; omit the clause, match a concrete value with equals, or exclude one with notEquals")
					}
					clause.Null = true
				default:
					return nil, fmt.Errorf("when entry has unknown key %q (expected path and one of equals, null, notEquals, greaterThan, or lessThan)", k)
				}
			}
			clauses = append(clauses, clause)
		}
		return clauses, nil
	default:
		return nil, fmt.Errorf("when must be a string (discriminator shorthand) or an array of { path, equals } tables; got %T", raw)
	}
}

// discriminatorFor resolves the topic type's declared discriminator.
// Every failure names the explicit { path, equals } form as the way
// out, so a config blocked on type metadata is never stuck.
func discriminatorFor(storage cluster.DatabaseStorage, topic string) (string, error) {
	resolver, ok := storage.(cluster.TypeResolver)
	if !ok {
		return "", fmt.Errorf("storage cannot resolve types; declare the clause explicitly as { path, equals }")
	}
	t, err := resolver.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil || t == nil {
		return "", fmt.Errorf("topic %q has no resolvable type; propose the type first or declare the clause explicitly as { path, equals }", topic)
	}
	if t.Discriminator == "" {
		return "", fmt.Errorf("type %q declares no discriminator; add discriminator = \"$.…\" to the type or declare the clause explicitly as { path, equals }", topic)
	}
	return t.Discriminator, nil
}

// warnKindMisuse applies the config-time entity-kind misuse matrix for
// the projection shape: a projection on a snapshot- or revision-kind topic
// is dead weight — both are total updates with nothing to fold (a revision
// is a snapshot whose history is retained); the plain sql syncable upserts
// them directly. Advisory only (warn + metric, the config still runs), and
// unspecified-kind topics never warn.
func (p *ProjectionSyncableParser) warnKindMisuse(storage cluster.DatabaseStorage, topic string) {
	resolver, ok := storage.(cluster.TypeResolver)
	if !ok {
		return
	}
	t, err := resolver.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil || t == nil {
		return
	}
	if t.EntityKind != cluster.EntityKindSnapshot && t.EntityKind != cluster.EntityKindRevision {
		return
	}

	zap.L().Warn("[projection.parser] projection on a snapshot- or revision-kind topic: those are total updates with nothing to fold — use the plain sql syncable instead, see README § Entity kinds",
		zap.String("topic", topic),
		zap.String("entity_kind", t.EntityKind.String()),
	)
	if p.Metrics != nil {
		p.Metrics.EntityKindMisuse("projection", topic, t.EntityKind.String())
	}
}
