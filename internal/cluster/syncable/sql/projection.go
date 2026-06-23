package sql

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/PaesslerAG/jsonpath"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// ProjectionColumn declares one column of the projection table. Unlike
// the plain syncable's Mapping, a column carries no jsonPath — what
// lands in it is decided per event by the rules.
type ProjectionColumn struct {
	Name    string `mapstructure:"name"`
	SQLType string `mapstructure:"type"`
}

// WhenClause is one match condition: exactly one of Equals (the value
// at Path must equal it) or Null (the value at Path must be JSON null)
// must be set. Null is a flag for the same reason as
// ProjectionSet.Null: TOML has no null literal, so `equals = null`
// cannot be written. A rule matches when every one of its clauses
// holds (AND); express OR as another rule. A missing Path is "no
// match", never an error — events of other shapes simply don't match —
// and that invariant is why a Null clause matches only a *present*
// null: {"allocs": null} matches, an absent field does not. There is
// no negation ("is not null"); the when language is equality-only.
//
// The jsonpath deliberately lives in a value, not a TOML key: viper
// lowercases map keys at read time, which would silently corrupt
// camelCase paths like $.eventType.
type WhenClause struct {
	Path   string `mapstructure:"path"`
	Equals any    `mapstructure:"equals"`
	Null   bool   `mapstructure:"null"`
}

// ProjectionSet is one column write of a matched rule. Exactly one of
// From (a jsonpath into the event payload), Value (a literal), or Null
// (write SQL NULL) must be set. Null is a flag rather than a Value
// literal because TOML has no null — `value = null` cannot be written,
// so clearing a column gets its own form. Writes are absolute — never
// relative to the current row — which is what makes redelivery
// converge: delivery is at-least-once and idempotent re-apply is the
// recovery mechanism, so aggregations (col = col + 1) are a
// correctness boundary, not a missing feature.
type ProjectionSet struct {
	Column string `mapstructure:"column"`
	From   string `mapstructure:"from"`
	Value  any    `mapstructure:"value"`
	Null   bool   `mapstructure:"null"`
}

// ProjectionRule fires when all of its When clauses hold, upserting
// its Set columns for the event's key. Rules execute in manifest
// order; when two matched rules set the same column, the last rule
// wins (deterministic — rule order is manifest order).
type ProjectionRule struct {
	When []WhenClause
	Set  []ProjectionSet
}

// onDelete behaviors for a projection source: delete-row drops the folded row
// (the spine source); clear NULLs the columns this source owns but keeps the row
// (a contributor); ignore drops the delete entirely.
const (
	onDeleteRow    = "delete-row"
	onDeleteClear  = "clear"
	onDeleteIgnore = "ignore"
)

// ProjectionSource is one input topic of a projection: the rules that fold its
// events into the shared row, the keyPath that correlates them by the shared
// aggregate key, and what its delete does to that row. The topic is the
// discriminator — events of other topics never reach this source's rules.
//
// KeyPath is the jsonpath that locates the entity key in this source's event
// payload; the key binds the primary-key column of every rule upsert. Defaults
// to $.<primaryKey>. The projected key must equal the entity's log Key for
// delete Actuals to remove/clear the right row.
type ProjectionSource struct {
	Topic    string
	KeyPath  string
	OnDelete string
	Rules    []ProjectionRule
}

// ProjectionConfig declares a stateful fold from one or more source topics into
// one current-state table: one row per aggregate key, maintained by per-source
// rules that fire per event. A single-source config is the common case; multiple
// sources fold several normalized topics into one denormalized row (the topic is
// each event's discriminator). See README § SQL projections.
type ProjectionConfig struct {
	Database   cluster.Database
	Table      string
	PrimaryKey string
	Columns    []ProjectionColumn
	Sources    []ProjectionSource

	// Single-source shorthand. The README single-topic form (and existing
	// configs) set these top-level fields; applyDefaults folds them into one
	// Source. A multi-source config sets Sources directly and leaves these empty.
	Topic   string
	KeyPath string
	Rules   []ProjectionRule
}

// applyDefaults folds the single-source shorthand into Sources and fills each
// source's derivable fields; called by both ParseConfig and Init so directly
// constructed configs behave like parsed ones.
func (c *ProjectionConfig) applyDefaults() {
	if len(c.Sources) == 0 && (c.Topic != "" || len(c.Rules) > 0) {
		c.Sources = []ProjectionSource{{Topic: c.Topic, KeyPath: c.KeyPath, Rules: c.Rules}}
	}
	for i := range c.Sources {
		s := &c.Sources[i]
		if s.OnDelete == "" {
			s.OnDelete = onDeleteRow // back-compat: a delete drops the row
		}
		if s.KeyPath == "" && c.PrimaryKey != "" {
			s.KeyPath = "$." + c.PrimaryKey
		}
	}
}

// ddlConfig synthesizes the plain-syncable Config shape that the
// dialect's CreateDDL and CreateDeleteSQL already understand: one
// Mapping per declared column (jsonPath unused — DDL reads only
// Column/SQLType). Reusing the dialects this way adds zero
// dialect-interface surface, which is also what keeps existing
// `type = "sql"` syncables byte-for-byte unaffected.
func (c *ProjectionConfig) ddlConfig() *Config {
	mappings := make([]Mapping, 0, len(c.Columns))
	for _, col := range c.Columns {
		mappings = append(mappings, Mapping{Column: col.Name, SQLType: col.SQLType})
	}
	return &Config{Table: c.Table, Mappings: mappings, PrimaryKey: c.PrimaryKey}
}

// ruleConfig synthesizes the per-rule upsert Config: the primary-key
// column first, then the rule's set columns in manifest order. Feeding
// it to the dialect's CreateSQL yields exactly the rule-restricted
// upsert the design calls for. The pk self-assignment in the update
// clause (pk = EXCLUDED.pk / pk = ?) is harmless: on conflict the
// values are equal by definition.
func (c *ProjectionConfig) ruleConfig(r ProjectionRule) *Config {
	mappings := make([]Mapping, 0, len(r.Set)+1)
	mappings = append(mappings, Mapping{Column: c.PrimaryKey})
	for _, s := range r.Set {
		mappings = append(mappings, Mapping{Column: s.Column})
	}
	return &Config{Table: c.Table, Mappings: mappings, PrimaryKey: c.PrimaryKey}
}

// validateProjectionConfig rejects every config that could otherwise
// fail only at sync time. It is storage-free so Init can re-validate
// directly constructed configs exactly like parsed ones. Rule indexes
// in errors are 1-based to match the operator's view of the manifest.
func validateProjectionConfig(c *ProjectionConfig) error {
	if c.Table == "" {
		return fmt.Errorf("table is required")
	}
	if c.PrimaryKey == "" {
		return fmt.Errorf("primaryKey is required")
	}
	if len(c.Columns) == 0 {
		return fmt.Errorf("at least one column is required")
	}
	declared := make(map[string]bool, len(c.Columns))
	for _, col := range c.Columns {
		if col.Name == "" {
			return fmt.Errorf("column with empty name")
		}
		if col.SQLType == "" {
			return fmt.Errorf("column %q: type is required", col.Name)
		}
		if declared[col.Name] {
			return fmt.Errorf("column %q declared twice", col.Name)
		}
		declared[col.Name] = true
	}
	if !declared[c.PrimaryKey] {
		return fmt.Errorf("primaryKey %q is not a declared column", c.PrimaryKey)
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source (a topic and its rules) is required")
	}
	topics := make(map[string]bool, len(c.Sources))
	for si, src := range c.Sources {
		// "src %q" prefixes scope rule errors to their source for a multi-source
		// config; the original single-source error substrings are preserved
		// inside (the parser tests match on substrings).
		where := fmt.Sprintf("source %d (topic %q)", si+1, src.Topic)
		if src.Topic == "" {
			return fmt.Errorf("source %d: topic is required", si+1)
		}
		if topics[src.Topic] {
			return fmt.Errorf("%s: topic declared twice (one source per topic)", where)
		}
		topics[src.Topic] = true
		switch src.OnDelete {
		case onDeleteRow, onDeleteClear, onDeleteIgnore:
		default:
			return fmt.Errorf("%s: onDelete %q is invalid (want %q, %q, or %q)", where, src.OnDelete, onDeleteRow, onDeleteClear, onDeleteIgnore)
		}
		if len(src.Rules) == 0 {
			return fmt.Errorf("%s: at least one rule is required", where)
		}
		ownsColumn := false
		for i, r := range src.Rules {
			// An empty When is allowed: the rule matches every event of its
			// source (the topic discriminates). Present clauses are validated.
			for _, cl := range r.When {
				if cl.Path == "" {
					return fmt.Errorf("%s rule %d: when entry needs a path", where, i+1)
				}
				if (cl.Equals != nil) == cl.Null {
					return fmt.Errorf("%s rule %d: when entry for %q: exactly one of equals or null is required", where, i+1, cl.Path)
				}
				if cl.Equals != nil && !isScalar(cl.Equals) {
					return fmt.Errorf("%s rule %d: when entry for %q: equals must be a scalar literal, got %T", where, i+1, cl.Path, cl.Equals)
				}
			}
			if len(r.Set) == 0 {
				return fmt.Errorf("%s rule %d: set is required", where, i+1)
			}
			seen := make(map[string]bool, len(r.Set))
			for _, s := range r.Set {
				if s.Column == "" {
					return fmt.Errorf("%s rule %d: set entry with empty column", where, i+1)
				}
				if !declared[s.Column] {
					return fmt.Errorf("%s rule %d sets unknown column %q", where, i+1, s.Column)
				}
				if s.Column == c.PrimaryKey {
					return fmt.Errorf("%s rule %d may not set the primary-key column %q (the key binds from keyPath)", where, i+1, s.Column)
				}
				forms := 0
				for _, set := range []bool{s.From != "", s.Value != nil, s.Null} {
					if set {
						forms++
					}
				}
				if forms != 1 {
					return fmt.Errorf("%s rule %d column %q: exactly one of from, value, or null is required", where, i+1, s.Column)
				}
				if s.Value != nil && !isScalar(s.Value) {
					return fmt.Errorf("%s rule %d column %q: value must be a scalar literal, got %T", where, i+1, s.Column, s.Value)
				}
				if seen[s.Column] {
					return fmt.Errorf("%s rule %d sets column %q twice (within a rule each column is set once; across rules the last matching rule wins)", where, i+1, s.Column)
				}
				seen[s.Column] = true
				ownsColumn = true
			}
		}
		if src.OnDelete == onDeleteClear && !ownsColumn {
			return fmt.Errorf("%s: onDelete = %q needs at least one column set by its rules to clear", where, onDeleteClear)
		}
	}
	return nil
}

// isScalar reports whether a TOML-decoded literal is a scalar (string,
// number, bool). Tables and arrays are rejected at config time: as a
// when target they would compare structurally against decoded JSON
// (silently shape-sensitive), and as a set value they have no defined
// column binding.
func isScalar(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return false
	}
	return true
}

// Projection is the stateful half of the SQL story: where the plain
// sql syncable lands one row per event (history), a Projection folds
// events into one row per entity (current state) — the fold lives in
// the one place that sees every event exactly in log order. The log
// stays the source of truth: the table is disposable and rebuildable
// by replaying from index 0, and amendments are a fresh table + fresh
// syncable, not ALTER (DDL here is CREATE TABLE IF NOT EXISTS only).
type Projection struct {
	db      *gosql.DB
	config  *ProjectionConfig
	dialect Dialect
	// name is the syncable's TOML name, used as the metric attribute
	// for unmatched-rule ticks (the config ID never reaches this
	// layer).
	name    string
	metrics *metrics.Metrics
	// sources is keyed by topic id: each holds that source's prepared rule
	// upserts, keyPath, onDelete behavior, and (for onDelete=clear) a prepared
	// column-clear UPDATE. The topic is the discriminator — an entity routes to
	// its source's rules, so two sources fold into one row without clobbering.
	sources map[string]*projectionSource
	// delete is the shared prepared DELETE-by-key. It serves sources whose
	// onDelete is "delete-row" (and so any RTBF delete on such a topic). Always
	// prepared: primaryKey is mandatory. Self-healing closure: if an entity's
	// creating event was scrubbed before a fresh replay, surviving events build a
	// partial row and the scrub's delete Actual removes it.
	delete *Delete
}

// projectionSource is the prepared, per-topic runtime of one ProjectionSource.
type projectionSource struct {
	topic    string
	keyPath  string
	onDelete string
	rules    []*projectionStmt
	// clear is the prepared "UPDATE … SET ownedCols = NULL WHERE pk = ?", set
	// only when onDelete == "clear".
	clear    *gosql.Stmt
	clearSQL string
}

// projectionStmt pairs one rule with its prepared upsert.
type projectionStmt struct {
	rule ProjectionRule
	SQL  string
	Stmt *gosql.Stmt
}

// ownedColumns returns the distinct columns this source's rules set, in
// first-seen order — the columns onDelete = "clear" NULLs.
func (s ProjectionSource) ownedColumns() []string {
	seen := make(map[string]bool)
	var cols []string
	for _, r := range s.Rules {
		for _, set := range r.Set {
			if !seen[set.Column] {
				seen[set.Column] = true
				cols = append(cols, set.Column)
			}
		}
	}
	return cols
}

// NewProjection constructs a Projection. m may be nil (no metrics);
// name is the syncable's TOML name for metric attribution.
func NewProjection(d *DB, config *ProjectionConfig, m *metrics.Metrics, name string) *Projection {
	return &Projection{db: d.DB, config: config, dialect: d.dialect, metrics: m, name: name}
}

// ValidateReplace implements cluster.ConfigChangeValidator: it rejects a
// re-POST whose materialized table schema differs from prior's, which
// CREATE TABLE IF NOT EXISTS would silently ignore. Returns a
// *SchemaChangeError (a cluster.RebuildRequiredError) or nil.
func (p *Projection) ValidateReplace(prior cluster.Syncable) error {
	return validateSchemaReplace(prior, p.materializedSchema())
}

// materializedSchema is the projection's table shape: its declared columns +
// primary key (projections create no indexes), exactly the ddlConfig CreateDDL
// runs.
func (p *Projection) materializedSchema() SyncableSchema {
	return schemaOf(p.config.ddlConfig())
}

// Teardown implements cluster.Teardownable: it drops the projection's
// destination table (DROP TABLE IF EXISTS), the destructive mirror of Init's
// CREATE. It is idempotent — dropping an already-absent table is a no-op — and
// reconstructable from the persisted config alone (it needs only the table
// name + DB handle), which is what the delete/rebuild paths rely on. It never
// touches prepared statements or the connection pool; call Close for those.
func (p *Projection) Teardown() error {
	dropString := p.dialect.DropDDL(p.config.ddlConfig())
	if _, err := p.db.Exec(dropString); err != nil {
		return fmt.Errorf("teardown [%s]: %w", dropString, err)
	}
	return nil
}

func (p *Projection) Init() error {
	// Re-validate even though ParseConfig already did: directly
	// constructed configs (tests, future callers) must hit the same
	// wall before any DDL reaches the destination database.
	p.config.applyDefaults()
	if err := validateProjectionConfig(p.config); err != nil {
		return err
	}

	ddlConfig := p.config.ddlConfig()
	ddlString := p.dialect.CreateDDL(ddlConfig)
	if _, err := p.db.Exec(ddlString); err != nil {
		return fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	// Prepare per-source rule upserts (and any clear) first, then the shared
	// row-delete last — the same prepare order as the original single-source
	// projection.
	p.sources = make(map[string]*projectionSource, len(p.config.Sources))
	for si, src := range p.config.Sources {
		ps := &projectionSource{topic: src.Topic, keyPath: src.KeyPath, onDelete: src.OnDelete}
		for i, r := range src.Rules {
			sqlString := p.dialect.CreateSQL(p.config.ruleConfig(r))
			stmt, err := p.db.Prepare(sqlString)
			if err != nil {
				return fmt.Errorf("prepare source %d (topic %q) rule %d sql [%s]: %w", si+1, src.Topic, i+1, sqlString, err)
			}
			ps.rules = append(ps.rules, &projectionStmt{rule: r, SQL: sqlString, Stmt: stmt})
		}
		if src.OnDelete == onDeleteClear {
			ps.clearSQL = p.dialect.CreateClearSQL(ddlConfig, src.ownedColumns())
			clearStmt, err := p.db.Prepare(ps.clearSQL)
			if err != nil {
				return fmt.Errorf("prepare source %d (topic %q) clear sql [%s]: %w", si+1, src.Topic, ps.clearSQL, err)
			}
			ps.clear = clearStmt
		}
		p.sources[src.Topic] = ps
	}

	// Shared row-delete (onDelete=delete-row, and any RTBF delete on such a topic).
	deleteString := p.dialect.CreateDeleteSQL(ddlConfig)
	deleteStmt, err := p.db.Prepare(deleteString)
	if err != nil {
		return fmt.Errorf("prepare delete sql [%s]: %w", deleteString, err)
	}
	p.delete = &Delete{deleteString, deleteStmt}

	return nil
}

func (p *Projection) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// Skip an Actual that carries no entity for one of our source topics before
	// BeginTx, so a non-matching Actual costs no transaction.
	relevant := false
	for _, e := range a.Entities {
		if _, ok := p.sources[e.Type.ID]; ok {
			relevant = true
			break
		}
	}
	if !relevant {
		return false, nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	for _, e := range a.Entities {
		src, ok := p.sources[e.Type.ID]
		if !ok {
			continue
		}
		if err := p.applyEntity(ctx, tx, src, e); err != nil {
			_ = tx.Rollback()
			return false, err
		}
	}

	// CAVEAT: tx.Commit() takes no context — see the matching comment
	// in Syncable.Sync (sql.go): a hung commit is uninterruptible, a
	// database/sql limitation.
	if err := tx.Commit(); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return false, rollbackErr
		}
		return false, err
	}

	return true, nil
}

func (p *Projection) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	for _, a := range as {
		for _, e := range a.Entities {
			src, ok := p.sources[e.Type.ID]
			if !ok {
				continue
			}
			if err := p.applyEntity(ctx, tx, src, e); err != nil {
				_ = tx.Rollback()
				return false, err
			}
		}
	}

	zap.L().Debug("sql projection batch committing", zap.Int("batch_size", len(as)))
	if err := tx.Commit(); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return false, rollbackErr
		}
		return false, err
	}

	return true, nil
}

// applyEntity applies one entity from source src to an open transaction. A
// delete follows the source's onDelete (see applyDelete); the sentinel payload
// is never unmarshaled. Any other entity is matched against that source's rules
// and every matching rule's upsert executes in manifest order — each rule sets
// only its own columns, so two sources fold into one row without clobbering.
// Zero matching rules → zero SQL (no ghost rows) plus an unmatched metric tick.
// Returns cluster.Permanent for non-retryable failures so the worker skips
// rather than retries. The caller owns the transaction.
func (p *Projection) applyEntity(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	if e.IsDelete() {
		return p.applyDelete(ctx, tx, src, e)
	}

	var jsonData any
	if err := json.Unmarshal(e.Data, &jsonData); err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.apply] unmarshal entity data: %w", err))
	}

	var matched []*projectionStmt
	for _, r := range src.rules {
		if matchWhen(r.rule.When, jsonData) {
			matched = append(matched, r)
		}
	}
	if len(matched) == 0 {
		// No ghost rows: zero matching rules → zero SQL. The tick is
		// the signal that a new event variant shipped without a rule.
		zap.L().Debug("[sql-projection] event matched no rules",
			zap.String("syncable", p.name), zap.String("topic", src.topic))
		if p.metrics != nil {
			p.metrics.SyncRulesUnmatched(p.name, src.topic)
		}
		return nil
	}

	// Key resolution is deliberately lazy — after matching — so an
	// unmatched foreign event missing the keyPath is a non-event, not
	// a dead letter. A matched event without a key is a permanent
	// misconfiguration.
	key, err := jsonpath.Get(src.keyPath, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.apply] keyPath [%s]: %w", src.keyPath, err))
	}

	for _, r := range matched {
		values := make([]any, 0, len(r.rule.Set)+1)
		values = append(values, key)
		for _, s := range r.rule.Set {
			switch {
			case s.From != "":
				v, err := jsonpath.Get(s.From, jsonData)
				if err != nil {
					return cluster.Permanent(fmt.Errorf("[sql-projection.apply] jsonpath [%s]: %w", s.From, err))
				}
				values = append(values, bindable(v))
			case s.Null:
				values = append(values, nil)
			default:
				values = append(values, s.Value)
			}
		}
		args := p.dialect.BindArgs(values)
		if _, err := tx.StmtContext(ctx, r.Stmt).ExecContext(ctx, args...); err != nil {
			wrapped := fmt.Errorf("[sql-projection.apply] exec [%s]: %w", r.SQL, err)
			if p.dialect.IsPermanent(err) {
				return cluster.Permanent(wrapped)
			}
			return wrapped
		}
	}
	return nil
}

// applyDelete honors a delete entity per its source's onDelete: ignore drops it;
// clear NULLs the source's owned columns for the keyed row (the folded row
// survives); delete-row removes the row entirely. The single bound argument is
// the entity Key, so the sentinel payload is never unmarshaled.
func (p *Projection) applyDelete(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	var stmt *gosql.Stmt
	var sqlStr string
	switch src.onDelete {
	case onDeleteIgnore:
		return nil
	case onDeleteClear:
		stmt, sqlStr = src.clear, src.clearSQL
	default: // onDeleteRow
		stmt, sqlStr = p.delete.Stmt, p.delete.SQL
	}
	if stmt == nil {
		return cluster.Permanent(fmt.Errorf(
			"[sql-projection.apply] cannot honor delete for key %q (topic %q): no statement prepared",
			string(e.Key), src.topic))
	}
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, string(e.Key)); err != nil {
		wrapped := fmt.Errorf("[sql-projection.apply] exec [%s]: %w", sqlStr, err)
		if p.dialect.IsPermanent(err) {
			return cluster.Permanent(wrapped)
		}
		return wrapped
	}
	return nil
}

func (p *Projection) Close() error {
	// Close every prepared statement; report the first error but
	// always attempt the rest so nothing leaks when one close fails.
	var err error
	for _, src := range p.sources {
		for _, r := range src.rules {
			if cerr := r.Stmt.Close(); err == nil {
				err = cerr
			}
		}
		if src.clear != nil {
			if cerr := src.clear.Close(); err == nil {
				err = cerr
			}
		}
	}
	if p.delete != nil {
		if cerr := p.delete.Stmt.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// matchWhen reports whether every clause holds against the unmarshaled
// payload. A missing path is "no match", never an error: when is a
// filter, and events of other shapes simply don't match. That holds
// for null clauses too — jsonpath distinguishes a present null (nil,
// no error) from an absent field (error), and only the former matches.
func matchWhen(clauses []WhenClause, jsonData any) bool {
	for _, c := range clauses {
		v, err := jsonpath.Get(c.Path, jsonData)
		if err != nil {
			return false
		}
		if c.Null {
			if v != nil {
				return false
			}
			continue
		}
		if !literalEquals(c.Equals, v) {
			return false
		}
	}
	return true
}

// literalEquals compares a TOML literal against a decoded JSON value.
// Numbers need normalizing: TOML integers decode as int64 while JSON
// numbers decode as float64, and == across those types is always
// false.
func literalEquals(want, got any) bool {
	if wf, ok := toFloat(want); ok {
		gf, ok2 := toFloat(got)
		return ok2 && wf == gf
	}
	return reflect.DeepEqual(want, got)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// bindable converts a jsonpath result into something database/sql can
// bind. Scalars pass through; objects and arrays (e.g. an allocs
// subtree headed for a JSONB column) re-marshal to JSON text — drivers
// cannot bind a Go map. This is a re-marshal of the decoded value, so
// the whole-payload byte-exactness caveat applies: key order and
// number formatting normalize, and integers above 2^53 lose precision.
// For byte-exact documents use the plain syncable's "$" mapping.
func bindable(v any) any {
	switch v.(type) {
	case map[string]any, []any:
		bs, err := json.Marshal(v)
		if err != nil {
			return v // unmarshalable shapes don't exist post-Unmarshal; let the driver report
		}
		return string(bs)
	}
	return v
}
