package sql

import (
	"fmt"
	"reflect"

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

func (p *SyncableParser) Parse(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (cluster.Syncable, error) {
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

// TopicsFromConfig implements cluster.SyncableTopicExtractor: a plain sql
// syncable consumes the single topic named at sql.topic. Read straight from the
// config (no Init / no DDL), so config-change enumeration runs no I/O.
func (p *SyncableParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	topic := v.GetString("sql.topic")
	if topic == "" {
		return nil
	}
	return []string{topic}
}

// DatabasesFromConfig implements cluster.SyncableDatabaseExtractor: a plain sql
// syncable writes to the single destination database named at sql.db. Read
// straight from the config so a database connection change can enumerate the
// syncables that captured its pool.
func (p *SyncableParser) DatabasesFromConfig(v *cluster.ParsedConfig) []string {
	db := v.GetString("sql.db")
	if db == "" {
		return nil
	}
	return []string{db}
}

func (p *SyncableParser) ParseConfig(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (*Config, error) {
	sqlDB := v.GetString("sql.db")
	if sqlDB == "" {
		return nil, &cluster.FieldError{Field: "sql.db", Issue: "required (name a [database] config)"}
	}
	db, err := storage.Database(sqlDB)
	if err != nil {
		return nil, &cluster.FieldError{
			Field: "sql.db",
			Issue: fmt.Sprintf("database %q not found", sqlDB),
			Err:   err,
		}
	}

	topic := v.GetString("sql.topic")
	if topic == "" {
		return nil, &cluster.FieldError{Field: "sql.topic", Issue: "required"}
	}
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

	// A destination table and at least one column mapping are needed for every
	// syncable, regardless of entity kind: without them Init emits malformed DDL
	// (`CREATE TABLE  ()`) / `INSERT INTO t () VALUES ()` that fails at POST when
	// the DB is reachable and only degrades (no worker) otherwise. Reject up front
	// with a field-scoped 400 instead of a raw SQL error.
	if table == "" {
		return nil, &cluster.FieldError{Field: "sql.table", Issue: "required: name the destination table"}
	}
	if len(mappings) == 0 {
		return nil, &cluster.FieldError{Field: "sql.mappings", Issue: "required: define at least one column mapping"}
	}

	// primaryKey is structurally required only for a snapshot-kind topic — the
	// key-addressed, last-writer-wins-per-key upsert this syncable performs.
	// Without it the upsert has no ON CONFLICT / unique-key target, so rows
	// duplicate instead of overwriting, and every delete Actual dead-letters (no
	// key to delete by — RTBF loss). Append/fold/history kinds (event,
	// standalone, revision, command), unspecified/grandfathered topics, and
	// topics whose type isn't resolvable at parse time legitimately land without
	// one, so we require it for snapshot only (best-effort, mirroring
	// warnKindMisuse's resolution).
	if primaryKey == "" && requiresPrimaryKey(storage, topic) {
		return nil, &cluster.FieldError{
			Field: "sql.primaryKey",
			Issue: "required for a snapshot-kind topic: this syncable upserts last-writer-wins per key, so without a primary key rows duplicate instead of overwriting and every delete is dropped",
		}
	}

	// A keyless (append/history) syncable gets a dedup sidecar named
	// <table>__committed_applied so replay doesn't duplicate rows. If the table
	// name is long enough that the sidecar name exceeds the 63-char identifier
	// limit, the database would silently truncate it and collide with the base
	// table — reject up front with a field-scoped 400.
	if primaryKey == "" {
		if name := AppliedSidecarName(table); len(name) > maxSidecarIdentifierLen {
			return nil, &cluster.FieldError{
				Field: "sql.table",
				Issue: fmt.Sprintf(
					"too long for a keyless syncable: its dedup sidecar %q is %d chars, over the %d-char identifier limit — use a table name of at most %d chars, or configure a primaryKey",
					name, len(name), maxSidecarIdentifierLen, maxSidecarIdentifierLen-len("__committed_applied")),
			}
		}
	}

	p.warnKindMisuse(storage, topic, mappings)

	var indexes []Index
	if err := v.UnmarshalKey("sql.indexes", &indexes); err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] parse sql.indexes: %w", err)
	}

	// Config identifiers are quoted before they reach SQL, but a control-char /
	// empty identifier — or a free-text SQLType that can't be quoted — should fail
	// as a field-scoped 400 here, not as a deferred driver error at Init.
	if err := validateConfigIdentifiers(table, primaryKey, keyColumn, mappings, indexes); err != nil {
		return nil, err
	}

	policy, err := cluster.ParseCheckpointPolicy(v)
	if err != nil {
		return nil, fmt.Errorf("[sql.syncable-parser] %w", err)
	}

	config := &Config{
		Database:   db,
		DatabaseID: sqlDB,
		Topic:      topic,
		Table:      table,
		Mappings:   mappings,
		Indexes:    indexes,
		PrimaryKey: primaryKey,
		KeyColumn:  keyColumn,
		Checkpoint: policy,
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

// requiresPrimaryKey reports whether the topic's declared entity kind makes a
// primary key structurally mandatory for this syncable. Only EntityKindSnapshot
// — the key-addressed, LWW-per-key upsert sink — does: for it an empty primary
// key silently duplicates rows and dead-letters every delete. Append/fold/
// history kinds and unspecified or unresolvable topics are grandfathered
// (best-effort, mirroring warnKindMisuse: an unknown resolver or unknown type
// means no enforcement, since a syncable may be configured before its topic).
func requiresPrimaryKey(storage cluster.DatabaseStorage, topic string) bool {
	resolver, ok := storage.(cluster.TypeResolver)
	if !ok {
		return false
	}
	t, err := resolver.ResolveType(cluster.LatestTypeRef(topic))
	if err != nil || t == nil {
		return false
	}
	return t.EntityKind == cluster.EntityKindSnapshot
}
