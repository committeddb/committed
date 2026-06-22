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

func (p *ProjectionSyncableParser) ParseConfig(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (*ProjectionConfig, error) {
	sqlDB := v.GetString("sql-projection.db")
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, err
	}

	topic := v.GetString("sql-projection.topic")
	table := v.GetString("sql-projection.table")
	primaryKey := v.GetString("sql-projection.primaryKey")
	keyPath := v.GetString("sql-projection.keyPath")

	var columns []ProjectionColumn
	if err := v.UnmarshalKey("sql-projection.columns", &columns); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] parse sql-projection.columns: %w", err)
	}

	var rawRules []rawProjectionRule
	if err := v.UnmarshalKey("sql-projection.rules", &rawRules); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] parse sql-projection.rules: %w", err)
	}

	rules := make([]ProjectionRule, 0, len(rawRules))
	for i, raw := range rawRules {
		when, err := normalizeWhen(raw.When, storage, topic)
		if err != nil {
			return nil, fmt.Errorf("[sql-projection.parser] rule %d: %w", i+1, err)
		}
		rules = append(rules, ProjectionRule{When: when, Set: raw.Set})
	}

	config := &ProjectionConfig{
		Database:   db,
		Topic:      topic,
		Table:      table,
		PrimaryKey: primaryKey,
		KeyPath:    keyPath,
		Columns:    columns,
		Rules:      rules,
	}
	config.applyDefaults()
	if err := validateProjectionConfig(config); err != nil {
		return nil, fmt.Errorf("[sql-projection.parser] %w", err)
	}

	p.warnKindMisuse(storage, topic)

	return config, nil
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
		return nil, fmt.Errorf("when is required (a rule must declare what it matches)")
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
