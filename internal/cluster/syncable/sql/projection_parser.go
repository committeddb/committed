package sql

import (
	"fmt"
	"reflect"
	"strings"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// ProjectionSyncableParser parses sql-projection syncable TOML.
// Metrics is optional (nil skips instrumentation); when set it counts
// entity-kind misuse at parse time and is handed to the Projection for
// unmatched-rule ticks at sync time.
type ProjectionSyncableParser struct {
	Metrics *metrics.Metrics
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
	if err := projection.Init(); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] init: %w", err)
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

// rawProjectionSource is the TOML decode shape of one [[sql-projection.source]]
// block: a topic (the discriminator), its correlation keyPath, its onDelete
// behavior, an optional source-level when filter, and either nested rules (a
// scalar fold) or an aggregate (a collection fold). when decodes as any so it
// resolves the same string-shorthand / explicit-clauses forms a rule's when
// does.
type rawProjectionSource struct {
	Topic     string                  `mapstructure:"topic"`
	KeyPath   string                  `mapstructure:"keyPath"`
	OnDelete  string                  `mapstructure:"onDelete"`
	When      any                     `mapstructure:"when"`
	Rules     []rawProjectionRule     `mapstructure:"rules"`
	Aggregate *rawProjectionAggregate `mapstructure:"aggregate"`
	Lookup    *rawProjectionLookup    `mapstructure:"lookup"`
}

// rawProjectionAggregate is the TOML decode shape of a source's
// [sql-projection.source.aggregate] block. Element is an array-of-tables (each
// a plain { field, from } or an enriched { field, lookup, on, select }) rather
// than an inline map so its output field names survive viper's map-key
// lowercasing byte-exact.
type rawProjectionAggregate struct {
	Column         string                   `mapstructure:"column"`
	ElementKey     string                   `mapstructure:"elementKey"`
	ElementKeyType string                   `mapstructure:"elementKeyType"`
	Element        []ProjectionElementField `mapstructure:"element"`
}

// rawProjectionLookup is the TOML decode shape of a source's
// [sql-projection.source.lookup] block: the dimension's id (referenced by
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

	var rawSources []rawProjectionSource
	if err := v.UnmarshalKey("sql-projection.source", &rawSources); err == nil {
		for _, rs := range rawSources {
			add(rs.Topic)
		}
	}
	// Back-compat single-source shape: top-level topic (used when no source
	// blocks are present).
	add(v.GetString("sql-projection.topic"))
	return topics
}

func (p *ProjectionSyncableParser) ParseConfig(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (*ProjectionConfig, error) {
	sqlDB := v.GetString("sql-projection.db")
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, err
	}

	table := v.GetString("sql-projection.table")
	primaryKey := v.GetString("sql-projection.primaryKey")

	var columns []ProjectionColumn
	if err := v.UnmarshalKey("sql-projection.columns", &columns); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] parse sql-projection.columns: %w", err)
	}

	sources, err := parseProjectionSources(v, storage)
	if err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] %w", err)
	}

	config := &ProjectionConfig{
		Database:   db,
		Table:      table,
		PrimaryKey: primaryKey,
		Columns:    columns,
		Sources:    sources,
	}
	config.applyDefaults()
	if err := validateProjectionConfig(config); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] %w", err)
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
// `[[sql-projection.source]]` blocks or — for back-compat — the single-source
// top-level `topic` / `keyPath` / `rules`. Exactly one shape is used: source
// blocks win when present.
func parseProjectionSources(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) ([]ProjectionSource, error) {
	var rawSources []rawProjectionSource
	if err := v.UnmarshalKey("sql-projection.source", &rawSources); err != nil {
		return nil, fmt.Errorf("parse sql-projection.source: %w", err)
	}

	if len(rawSources) > 0 {
		sources := make([]ProjectionSource, 0, len(rawSources))
		for si, rs := range rawSources {
			when, err := normalizeWhen(rs.When, storage, rs.Topic)
			if err != nil {
				return nil, fmt.Errorf("source %d (topic %q): when: %w", si+1, rs.Topic, err)
			}
			src := ProjectionSource{
				Topic:    rs.Topic,
				KeyPath:  rs.KeyPath,
				OnDelete: rs.OnDelete,
				When:     when,
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
	topic := v.GetString("sql-projection.topic")
	keyPath := v.GetString("sql-projection.keyPath")
	var rawRules []rawProjectionRule
	if err := v.UnmarshalKey("sql-projection.rules", &rawRules); err != nil {
		return nil, fmt.Errorf("parse sql-projection.rules: %w", err)
	}
	rules, err := normalizeRules(rawRules, storage, topic)
	if err != nil {
		return nil, err
	}
	return []ProjectionSource{{Topic: topic, KeyPath: keyPath, Rules: rules}}, nil
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
				case strings.EqualFold(k, "null"):
					b, ok := val.(bool)
					if !ok {
						return nil, fmt.Errorf("when null must be a boolean; got %T", val)
					}
					// An explicit false would silently mean nothing
					// (and "is not null" does not exist — the when
					// language is equality-only), so reject it rather
					// than let a misread config parse.
					if !b {
						return nil, fmt.Errorf("when null = false is not a predicate (no negation); omit the clause or match a concrete value with equals")
					}
					clause.Null = true
				default:
					return nil, fmt.Errorf("when entry has unknown key %q (expected path and one of equals or null)", k)
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

	zap.L().Warn("[sql-projection.parser] projection on a snapshot- or revision-kind topic: those are total updates with nothing to fold — use the plain sql syncable instead, see README § Entity kinds",
		zap.String("topic", topic),
		zap.String("entity_kind", t.EntityKind.String()),
	)
	if p.Metrics != nil {
		p.Metrics.EntityKindMisuse("sql-projection", topic, t.EntityKind.String())
	}
}
