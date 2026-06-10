package sql

import (
	"fmt"
	"reflect"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// SyncableParser parses sql syncable TOML. Metrics is optional (nil
// skips instrumentation); when set, entity-kind-misuse parses are
// counted on committed.entity_kind.misuse alongside the warning log.
type SyncableParser struct {
	Metrics *metrics.Metrics
}

func (p *SyncableParser) Parse(v *viper.Viper, storage cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v, storage)
	if err != nil {
		return nil, err
	}

	db, ok := config.Database.(*DB)
	if !ok {
		return nil, fmt.Errorf("expected sql.DB but was %s", reflect.TypeOf(config.Database))
	}

	syncable := New(db, config)
	err = syncable.Init()
	if err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] init: %w", err)
	}

	return syncable, nil
}

func (p *SyncableParser) ParseConfig(v *viper.Viper, storage cluster.DatabaseStorage) (*Config, error) {
	sqlDB := v.GetString("sql.db")
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, err
	}

	topic := v.GetString("sql.topic")
	table := v.GetString("sql.table")
	primaryKey := v.GetString("sql.primaryKey")
	keyColumn := v.GetString("sql.keyColumn")

	var mappings []Mapping
	if err := v.UnmarshalKey("sql.mappings", &mappings); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] parse sql.mappings: %w", err)
	}
	if err := validateMappings(mappings); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] %w", err)
	}

	p.warnKindMisuse(storage, topic, mappings)

	var indexes []Index
	if err := v.UnmarshalKey("sql.indexes", &indexes); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] parse sql.indexes: %w", err)
	}

	config := &Config{
		Database:   db,
		Topic:      topic,
		Table:      table,
		Mappings:   mappings,
		Indexes:    indexes,
		PrimaryKey: primaryKey,
		KeyColumn:  keyColumn,
	}

	return config, nil
}

// warnKindMisuse applies the config-time entity-kind misuse matrix for
// this syncable shape. The sql syncable is a key-addressed upsert (LWW
// per key) — the right sink for snapshot topics, but a known bug class
// on event topics when it maps leaf fields: events are partial by
// design, so each variant either dead-letters on the jsonpaths it
// doesn't carry or clobbers columns it didn't mean to write. A config
// with a whole-payload "$" mapping is exempt — that is the
// conventional event-log shape (envelope columns + the full document)
// and exactly how an event topic should land in SQL. Advisory only
// (warn + metric, the config still runs), and unspecified-kind topics
// are grandfathered — they never warn. Fires wherever the config is
// parsed: at propose time and again on every worker rebuild, so the
// signal lives near the running config.
func (p *SyncableParser) warnKindMisuse(storage cluster.DatabaseStorage, topic string, mappings []Mapping) {
	for _, m := range mappings {
		if m.JsonPath == wholePayloadPath {
			return
		}
	}
	resolver, ok := storage.(cluster.TypeResolver)
	if !ok {
		return
	}
	t, err := resolver.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil || t == nil {
		// The topic's type isn't known (yet) — nothing to check. A
		// syncable may legitimately be configured before its topic.
		return
	}
	if t.EntityKind != cluster.EntityKindEvent {
		return
	}

	zap.L().Warn("[sql.syncable-parser] leaf-mapped upsert syncable on an event-kind topic: partial events dead-letter or clobber leaf-mapped columns — map the whole payload with jsonPath = \"$\" instead, see README § Entity kinds",
		zap.String("topic", topic),
		zap.String("entity_kind", t.EntityKind.String()),
	)
	if p.Metrics != nil {
		p.Metrics.EntityKindMisuse("sql", topic, t.EntityKind.String())
	}
}
