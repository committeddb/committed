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

// onDelete behaviors for a projection source. The first three are for rule
// (scalar-fold) sources: delete-row drops the folded row (the spine source);
// clear NULLs the columns this source owns but keeps the row (a contributor);
// ignore drops the delete entirely. remove-from-aggregate is for an aggregate
// (collection-fold) source: it removes the deleted child's element from the
// parent's array column, leaving the row.
const (
	onDeleteRow                 = "delete-row"
	onDeleteClear               = "clear"
	onDeleteIgnore              = "ignore"
	onDeleteRemoveFromAggregate = "remove-from-aggregate"
)

// elementKeyType values for an aggregate's elementKey. The sidecar always
// stores the key as text (so binding never mismatches a typed column); this
// flag only chooses how the array orders: "number" sorts 1,2,…,10 (numeric
// cast), "text" sorts lexically 1,10,2. Default is text.
const (
	elementKeyTypeText   = "text"
	elementKeyTypeNumber = "number"
)

// ProjectionElementField is one field of an aggregate's stored per-child object
// or of a dimension's stored object. Field is the output JSON key. A field is
// either *plain* — From, a jsonpath into the payload — or *enriched* — Lookup
// (a lookup source's name), On (the element field holding the foreign key), and
// Select (the dimension field to pull). Enriched fields are resolved by a join
// at materialize, not stored. It is an array-of-tables (not an inline map) for
// the same reason jsonpaths live in values elsewhere — viper lowercases map
// keys, which would silently corrupt a camelCase Field name.
type ProjectionElementField struct {
	Field  string `mapstructure:"field"`
	From   string `mapstructure:"from"`
	Lookup string `mapstructure:"lookup"`
	On     string `mapstructure:"on"`
	Select string `mapstructure:"select"`
}

// enriched reports whether this field is resolved from a dimension (Lookup set)
// rather than read straight from the payload (From set).
func (f ProjectionElementField) enriched() bool { return f.Lookup != "" }

// ProjectionLookup declares a dimension source: a topic whose entities populate
// a keyed dimension table (Name → referenced by element enrichments), storing
// Fields (each a plain field/from) keyed by the source's keyPath. It writes no
// BFF column — it is read by aggregate elements that enrich from it.
type ProjectionLookup struct {
	Name   string
	Fields []ProjectionElementField
}

// ProjectionAggregate folds a source's child entities into one JSON-array
// column on the parent row. Column is the array column; Element is the per-child
// object to store; ElementKey is a jsonpath to each child's identity within the
// array (its sort key, and what makes a re-delivered child replace rather than
// duplicate). ElementKeyType ("text" or "number") chooses lexical vs numeric
// ordering. The array is materialized from a sidecar table (one row per child),
// so it is a pure function of the child set and a delete — which carries only
// the child Key — removes exactly that child's element.
type ProjectionAggregate struct {
	Column         string
	Element        []ProjectionElementField
	ElementKey     string
	ElementKeyType string
}

// ProjectionSource is one input of a projection. The topic is the discriminator
// — events of other topics never reach this source. A source folds its events
// exactly one of three ways: scalar columns (Rules), one collection column
// (Aggregate), or a dimension table other sources enrich from (Lookup). When,
// if set, restricts which of the topic's events this source consumes, so several
// sources can split one topic into different columns (e.g. principals where
// category=actor into top_cast, category=director into directors); an empty When
// consumes every event of the topic.
//
// KeyPath is the jsonpath that locates the correlation key in this source's
// event payload — for a rule source it binds the primary-key column of every
// upsert; for an aggregate source it picks the parent row a child folds into.
// Defaults to $.<primaryKey>. The projected key must equal the entity's log Key
// for a rule source's delete Actuals to remove/clear the right row.
type ProjectionSource struct {
	Topic     string
	KeyPath   string
	OnDelete  string
	When      []WhenClause
	Rules     []ProjectionRule
	Aggregate *ProjectionAggregate
	Lookup    *ProjectionLookup
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
		// A lookup source writes a dimension table, not the BFF row, so the row
		// onDelete behaviors and the $.<primaryKey> keyPath default don't apply —
		// its keyPath is the dimension key and must be explicit.
		if s.Lookup == nil {
			if s.OnDelete == "" {
				if s.Aggregate != nil {
					s.OnDelete = onDeleteRemoveFromAggregate // a child delete leaves the row
				} else {
					s.OnDelete = onDeleteRow // back-compat: a delete drops the row
				}
			}
			if s.KeyPath == "" && c.PrimaryKey != "" {
				s.KeyPath = "$." + c.PrimaryKey
			}
		}
		if s.Aggregate != nil && s.Aggregate.ElementKeyType == "" {
			s.Aggregate.ElementKeyType = elementKeyTypeText
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
	// lookups maps each declared lookup source's name to its source index, so an
	// aggregate's enrichment can be checked to reference a real dimension. Built
	// in a pre-pass because enrichments may reference a lookup declared later.
	lookups := make(map[string]int)
	for si, src := range c.Sources {
		if src.Lookup == nil {
			continue
		}
		if src.Lookup.Name == "" {
			return fmt.Errorf("source %d (topic %q): lookup name is required", si+1, src.Topic)
		}
		if prev, ok := lookups[src.Lookup.Name]; ok {
			return fmt.Errorf("source %d (topic %q): lookup %q already declared by source %d", si+1, src.Topic, src.Lookup.Name, prev+1)
		}
		lookups[src.Lookup.Name] = si
	}
	// owner records which source writes each column. A column is owned by one
	// source (so two sources never clobber each other); the same source claiming
	// a column across its rules is fine. This — not topic uniqueness — is the
	// real guard, which is what lets several sources split one topic into
	// different columns (filtered aggregates).
	owner := make(map[string]int)
	for si, src := range c.Sources {
		// "source N (topic X)" prefixes scope errors to their source for a
		// multi-source config; the original single-source error substrings are
		// preserved inside (the parser tests match on substrings).
		where := fmt.Sprintf("source %d (topic %q)", si+1, src.Topic)
		if src.Topic == "" {
			return fmt.Errorf("source %d: topic is required", si+1)
		}
		kinds := 0
		for _, has := range []bool{len(src.Rules) > 0, src.Aggregate != nil, src.Lookup != nil} {
			if has {
				kinds++
			}
		}
		if kinds != 1 {
			return fmt.Errorf("%s: a source needs exactly one of rules, an aggregate, or a lookup", where)
		}
		// Source-level when is an optional filter (empty consumes the whole
		// topic); its clauses are validated like a rule's.
		if err := validateWhenClauses(src.When, where); err != nil {
			return err
		}
		if src.Lookup != nil {
			if err := validateLookup(src, where); err != nil {
				return err
			}
			continue
		}
		if src.Aggregate != nil {
			if err := validateAggregate(c, src, where, declared, owner, si, lookups); err != nil {
				return err
			}
			continue
		}
		switch src.OnDelete {
		case onDeleteRow, onDeleteClear, onDeleteIgnore:
		default:
			return fmt.Errorf("%s: onDelete %q is invalid (want %q, %q, or %q)", where, src.OnDelete, onDeleteRow, onDeleteClear, onDeleteIgnore)
		}
		ownsColumn := false
		for i, r := range src.Rules {
			if err := validateWhenClauses(r.When, fmt.Sprintf("%s rule %d", where, i+1)); err != nil {
				return err
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
				if err := claimColumn(owner, s.Column, si, where); err != nil {
					return err
				}
				ownsColumn = true
			}
		}
		if src.OnDelete == onDeleteClear && !ownsColumn {
			return fmt.Errorf("%s: onDelete = %q needs at least one column set by its rules to clear", where, onDeleteClear)
		}
	}
	return nil
}

// validateWhenClauses checks a set of match clauses (a rule's or a source's).
// An empty set is allowed — it matches every event of the source's topic (the
// topic is the discriminator). where prefixes each error to its location.
func validateWhenClauses(clauses []WhenClause, where string) error {
	for _, cl := range clauses {
		if cl.Path == "" {
			return fmt.Errorf("%s: when entry needs a path", where)
		}
		if (cl.Equals != nil) == cl.Null {
			return fmt.Errorf("%s: when entry for %q: exactly one of equals or null is required", where, cl.Path)
		}
		if cl.Equals != nil && !isScalar(cl.Equals) {
			return fmt.Errorf("%s: when entry for %q: equals must be a scalar literal, got %T", where, cl.Path, cl.Equals)
		}
	}
	return nil
}

// claimColumn records that source si writes col, rejecting a second source that
// writes the same column. The same source re-claiming a column (its rules set
// it more than once, last-write-wins) is fine.
func claimColumn(owner map[string]int, col string, si int, where string) error {
	if prev, ok := owner[col]; ok && prev != si {
		return fmt.Errorf("%s: column %q is already written by source %d (each column is owned by one source)", where, col, prev+1)
	}
	owner[col] = si
	return nil
}

// validateAggregate checks one aggregate source: a valid delete behavior, a
// declared non-primary-key column it solely owns, a non-empty elementKey and a
// known elementKeyType, and at least one element field (each with a name and a
// from jsonpath, names distinct).
func validateAggregate(c *ProjectionConfig, src ProjectionSource, where string, declared map[string]bool, owner map[string]int, si int, lookups map[string]int) error {
	ag := src.Aggregate
	switch src.OnDelete {
	case onDeleteRemoveFromAggregate, onDeleteIgnore:
	default:
		return fmt.Errorf("%s: onDelete %q is invalid for an aggregate source (want %q or %q)", where, src.OnDelete, onDeleteRemoveFromAggregate, onDeleteIgnore)
	}
	if ag.Column == "" {
		return fmt.Errorf("%s: aggregate column is required", where)
	}
	if !declared[ag.Column] {
		return fmt.Errorf("%s: aggregate column %q is not a declared column", where, ag.Column)
	}
	if ag.Column == c.PrimaryKey {
		return fmt.Errorf("%s: aggregate column %q may not be the primary-key column", where, ag.Column)
	}
	if err := claimColumn(owner, ag.Column, si, where); err != nil {
		return err
	}
	if ag.ElementKey == "" {
		return fmt.Errorf("%s: aggregate elementKey is required", where)
	}
	switch ag.ElementKeyType {
	case elementKeyTypeText, elementKeyTypeNumber:
	default:
		return fmt.Errorf("%s: aggregate elementKeyType %q is invalid (want %q or %q)", where, ag.ElementKeyType, elementKeyTypeText, elementKeyTypeNumber)
	}
	if len(ag.Element) == 0 {
		return fmt.Errorf("%s: aggregate element needs at least one field", where)
	}
	// First pass: each field is plain (from) XOR enriched (lookup/on/select);
	// collect the plain field names — an enriched field's `on` must name one
	// (the foreign key is stored, the dimension value is joined at materialize).
	plain := make(map[string]bool)
	seen := make(map[string]bool, len(ag.Element))
	for _, f := range ag.Element {
		if f.Field == "" {
			return fmt.Errorf("%s: aggregate element field with empty name", where)
		}
		if seen[f.Field] {
			return fmt.Errorf("%s: aggregate element field %q declared twice", where, f.Field)
		}
		seen[f.Field] = true
		if f.enriched() {
			if f.From != "" {
				return fmt.Errorf("%s: aggregate element field %q has both from and lookup (a field is one or the other)", where, f.Field)
			}
			if f.On == "" || f.Select == "" {
				return fmt.Errorf("%s: aggregate element field %q: an enriched field needs on and select", where, f.Field)
			}
			if _, ok := lookups[f.Lookup]; !ok {
				return fmt.Errorf("%s: aggregate element field %q references unknown lookup %q", where, f.Field, f.Lookup)
			}
		} else {
			if f.From == "" {
				return fmt.Errorf("%s: aggregate element field %q needs a from jsonpath (or lookup/on/select)", where, f.Field)
			}
			if f.On != "" || f.Select != "" {
				return fmt.Errorf("%s: aggregate element field %q: on/select are only for enriched (lookup) fields", where, f.Field)
			}
			plain[f.Field] = true
		}
	}
	if len(plain) == 0 {
		return fmt.Errorf("%s: aggregate element needs at least one plain (from) field", where)
	}
	// Second pass: an enriched field's `on` must name a plain element field.
	for _, f := range ag.Element {
		if f.enriched() && !plain[f.On] {
			return fmt.Errorf("%s: aggregate element field %q: on %q is not a plain element field", where, f.Field, f.On)
		}
	}
	return nil
}

// validateLookup checks one lookup (dimension) source: at least one stored
// field, each a plain field/from (a dimension field cannot itself enrich), names
// distinct. The dimension key is the entity's own Key, so there is no keyPath.
func validateLookup(src ProjectionSource, where string) error {
	lk := src.Lookup
	if len(lk.Fields) == 0 {
		return fmt.Errorf("%s: lookup %q needs at least one field", where, lk.Name)
	}
	seen := make(map[string]bool, len(lk.Fields))
	for _, f := range lk.Fields {
		if f.Field == "" {
			return fmt.Errorf("%s: lookup %q field with empty name", where, lk.Name)
		}
		if f.enriched() {
			return fmt.Errorf("%s: lookup %q field %q may not itself enrich (no lookup/on/select)", where, lk.Name, f.Field)
		}
		if f.From == "" {
			return fmt.Errorf("%s: lookup %q field %q needs a from jsonpath", where, lk.Name, f.Field)
		}
		if seen[f.Field] {
			return fmt.Errorf("%s: lookup %q field %q declared twice", where, lk.Name, f.Field)
		}
		seen[f.Field] = true
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
	// sources is keyed by topic id to the sources that consume it. The topic is
	// the discriminator; an entity routes to every source on its topic, and each
	// source's When (and, for rule sources, per-rule when) decides whether it
	// folds the event. Several sources may share a topic — that is how one topic
	// splits into different columns (filtered aggregates).
	sources map[string][]*projectionSource
	// delete is the shared prepared DELETE-by-key. It serves sources whose
	// onDelete is "delete-row" (and so any RTBF delete on such a topic). Always
	// prepared: primaryKey is mandatory. Self-healing closure: if an entity's
	// creating event was scrubbed before a fresh replay, surviving events build a
	// partial row and the scrub's delete Actual removes it.
	delete *Delete
}

// projectionSource is the prepared runtime of one ProjectionSource. Exactly one
// of rules (scalar fold), agg (collection fold), or lkp (dimension) is set.
type projectionSource struct {
	topic    string
	keyPath  string
	onDelete string
	// when is the source-level filter (empty = consume every event of the
	// topic), evaluated on upsert before the rules/aggregate apply.
	when  []WhenClause
	rules []*projectionStmt
	// clear is the prepared "UPDATE … SET ownedCols = NULL WHERE pk = ?", set
	// only when onDelete == "clear".
	clear    *gosql.Stmt
	clearSQL string
	// agg is the prepared aggregate runtime, set only for a collection-fold
	// source (nil otherwise).
	agg *aggregateRuntime
	// lkp is the prepared lookup (dimension) runtime, set only for a lookup
	// source (nil otherwise).
	lkp *lookupRuntime
}

// aggregateRuntime is the prepared runtime of one ProjectionAggregate: the
// stored (plain) element fields plus the statements that maintain the sidecar
// and re-materialize the parent's array column from it. Enriched fields are not
// stored — they are joined in by the materialize/rebuild SQL.
type aggregateRuntime struct {
	column     string
	elementKey string
	fields     []ProjectionElementField // plain fields only (stored in the sidecar)
	sidecar    string

	upsertSidecar    *gosql.Stmt
	upsertSidecarSQL string
	deleteSidecar    *gosql.Stmt
	deleteSidecarSQL string
	materialize      *gosql.Stmt
	materializeSQL   string
	rebuild          *gosql.Stmt
	rebuildSQL       string
	lookup           *gosql.Stmt
	lookupSQL        string
}

// lookupRuntime is the prepared runtime of one ProjectionLookup: the dimension
// key/fields, the statements maintaining the dimension table, and the dependent
// aggregates to re-materialize when a dimension row changes (the fan-out).
type lookupRuntime struct {
	name      string
	fields    []ProjectionElementField
	dimension string

	upsertDim    *gosql.Stmt
	upsertDimSQL string
	deleteDim    *gosql.Stmt
	deleteDimSQL string

	dependents []*aggregateDependent
}

// aggregateDependent is one aggregate that enriches from a lookup: when a
// dimension key changes, affected finds the parents whose elements reference it
// (on onField) and rebuild re-materializes each (shared with the aggregate's own
// rebuild).
type aggregateDependent struct {
	onField     string
	affected    *gosql.Stmt
	affectedSQL string
	rebuild     *gosql.Stmt
	rebuildSQL  string
}

// sidecarName is the backing table for an aggregate column: <table>__<column>.
// One per aggregate source; teardown drops them alongside the projection table.
func sidecarName(table, column string) string {
	return table + "__" + column
}

// dimensionName is the backing table for a lookup source: <table>__lookup_<name>.
func dimensionName(table, lookup string) string {
	return table + "__lookup_" + lookup
}

// aggregateSpec builds the dialect-facing spec for one aggregate source,
// grouping its enriched element fields by (lookup, on) into one join apiece
// (first-seen order, so the materialize SQL is stable).
func (c *ProjectionConfig) aggregateSpec(ag *ProjectionAggregate) AggregateSpec {
	spec := AggregateSpec{
		Table:       c.Table,
		PrimaryKey:  c.PrimaryKey,
		Column:      ag.Column,
		Sidecar:     sidecarName(c.Table, ag.Column),
		NumericSort: ag.ElementKeyType == elementKeyTypeNumber,
	}
	type joinKey struct{ lookup, on string }
	at := map[joinKey]int{}
	for _, f := range ag.Element {
		if !f.enriched() {
			continue
		}
		k := joinKey{f.Lookup, f.On}
		i, ok := at[k]
		if !ok {
			i = len(spec.Enrichments)
			at[k] = i
			spec.Enrichments = append(spec.Enrichments, AggregateEnrichment{
				Dimension: dimensionName(c.Table, f.Lookup),
				OnField:   f.On,
			})
		}
		spec.Enrichments[i].Selects = append(spec.Enrichments[i].Selects,
			AggregateEnrichmentField{Output: f.Field, Source: f.Select})
	}
	return spec
}

// lookupSpec builds the dialect-facing spec for one lookup source.
func (c *ProjectionConfig) lookupSpec(lk *ProjectionLookup) LookupSpec {
	return LookupSpec{Dimension: dimensionName(c.Table, lk.Name)}
}

// dimensionConfig synthesizes the plain Config whose CreateSQL / CreateDeleteSQL
// give the dimension's upsert and delete — an ordinary key-on-conflict shape,
// reusing the dialect's builders (and MySQL arg-doubling).
func dimensionConfig(dimension string) *Config {
	return &Config{
		Table:      dimension,
		PrimaryKey: LookupKey,
		Mappings:   []Mapping{{Column: LookupKey}, {Column: LookupFields}},
	}
}

// sidecarConfig synthesizes the plain Config whose CreateSQL / CreateDeleteSQL
// give the sidecar's upsert and delete — both ordinary key-on-conflict shapes,
// so they reuse the dialect's existing builders (and MySQL arg-doubling) rather
// than adding sidecar-specific dialect surface.
func sidecarConfig(sidecar string) *Config {
	return &Config{
		Table:      sidecar,
		PrimaryKey: SidecarChildKey,
		Mappings: []Mapping{
			{Column: SidecarChildKey},
			{Column: SidecarParentKey},
			{Column: SidecarElementKey},
			{Column: SidecarElement},
		},
	}
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
	p.config.applyDefaults()
	// Drop each aggregate source's sidecar, then the projection table. Order is
	// not load-bearing (DROP IF EXISTS is independent), but dropping sidecars
	// first keeps teardown's footprint a strict subset of Init's.
	for _, src := range p.config.Sources {
		var housekeeping string
		switch {
		case src.Aggregate != nil:
			housekeeping = sidecarName(p.config.Table, src.Aggregate.Column)
		case src.Lookup != nil:
			housekeeping = dimensionName(p.config.Table, src.Lookup.Name)
		default:
			continue
		}
		drop := p.dialect.DropDDL(&Config{Table: housekeeping})
		if _, err := p.db.Exec(drop); err != nil {
			return fmt.Errorf("teardown [%s]: %w", drop, err)
		}
	}
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

	// Prepare per-source statements. enrichRefs collects, per lookup name, the
	// aggregates that enrich from it (their spec + on-field), so the second pass
	// can wire each lookup's fan-out to its dependents' rebuilds.
	p.sources = make(map[string][]*projectionSource, len(p.config.Sources))
	var lookupSources []*projectionSource
	enrichRefs := map[string][]enrichRef{}
	for si, src := range p.config.Sources {
		ps := &projectionSource{topic: src.Topic, keyPath: src.KeyPath, onDelete: src.OnDelete, when: src.When}
		switch {
		case src.Aggregate != nil:
			spec := p.config.aggregateSpec(src.Aggregate)
			agg, err := p.initAggregate(si, src, spec)
			if err != nil {
				return err
			}
			ps.agg = agg
			// Register one ref per distinct (lookup, on) this aggregate enriches.
			seen := map[[2]string]bool{}
			for _, f := range src.Aggregate.Element {
				if !f.enriched() {
					continue
				}
				k := [2]string{f.Lookup, f.On}
				if seen[k] {
					continue
				}
				seen[k] = true
				enrichRefs[f.Lookup] = append(enrichRefs[f.Lookup], enrichRef{agg: agg, spec: spec, onField: f.On})
			}
		case src.Lookup != nil:
			lkp, err := p.initLookup(si, src)
			if err != nil {
				return err
			}
			ps.lkp = lkp
			lookupSources = append(lookupSources, ps)
		default:
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
		}
		p.sources[src.Topic] = append(p.sources[src.Topic], ps)
	}

	// Second pass: wire each lookup's fan-out. For every aggregate that enriches
	// from this lookup, prepare the affected-parents query and point it at that
	// aggregate's rebuild, so a dimension change re-materializes its dependents.
	for _, ps := range lookupSources {
		for _, ref := range enrichRefs[ps.lkp.name] {
			affSQL := p.dialect.CreateAggregateAffectedParentsSQL(ref.spec, ref.onField)
			affStmt, err := p.db.Prepare(affSQL)
			if err != nil {
				return fmt.Errorf("prepare lookup %q affected-parents sql [%s]: %w", ps.lkp.name, affSQL, err)
			}
			ps.lkp.dependents = append(ps.lkp.dependents, &aggregateDependent{
				onField:     ref.onField,
				affected:    affStmt,
				affectedSQL: affSQL,
				rebuild:     ref.agg.rebuild,
				rebuildSQL:  ref.agg.rebuildSQL,
			})
		}
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

// enrichRef records, for the fan-out wiring, that aggregate agg (with the given
// spec) enriches on element field onField from some lookup.
type enrichRef struct {
	agg     *aggregateRuntime
	spec    AggregateSpec
	onField string
}

// plainElementFields returns the element fields read straight from the payload
// (the ones stored in the sidecar); enriched fields are joined in at materialize.
func plainElementFields(fields []ProjectionElementField) []ProjectionElementField {
	out := make([]ProjectionElementField, 0, len(fields))
	for _, f := range fields {
		if !f.enriched() {
			out = append(out, f)
		}
	}
	return out
}

// initLookup creates one lookup source's dimension table and prepares its upsert
// and delete (ordinary key shapes reusing the dialect's CreateSQL /
// CreateDeleteSQL). The fan-out wiring (dependents) is attached in Init's second
// pass, once every aggregate is built.
func (p *Projection) initLookup(si int, src ProjectionSource) (*lookupRuntime, error) {
	lk := src.Lookup
	spec := p.config.lookupSpec(lk)
	where := fmt.Sprintf("source %d (topic %q) lookup %q", si+1, src.Topic, lk.Name)

	ddl := p.dialect.CreateLookupDimensionDDL(spec)
	if _, err := p.db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("%s dimension ddl [%s]: %w", where, ddl, err)
	}

	rt := &lookupRuntime{
		name:      lk.Name,
		fields:    lk.Fields,
		dimension: spec.Dimension,
	}
	dimConfig := dimensionConfig(spec.Dimension)
	var err error
	rt.upsertDimSQL = p.dialect.CreateSQL(dimConfig)
	if rt.upsertDim, err = p.db.Prepare(rt.upsertDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension upsert [%s]: %w", where, rt.upsertDimSQL, err)
	}
	rt.deleteDimSQL = p.dialect.CreateDeleteSQL(dimConfig)
	if rt.deleteDim, err = p.db.Prepare(rt.deleteDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension delete [%s]: %w", where, rt.deleteDimSQL, err)
	}
	return rt, nil
}

// initAggregate creates one aggregate source's sidecar table and prepares the
// five statements that maintain it and re-materialize the parent column: the
// sidecar upsert and delete (ordinary key shapes, reusing the dialect's
// CreateSQL / CreateDeleteSQL), the parent-key lookup (read back a deleted
// child's parent), and the materialize / rebuild (re-aggregate the parent's
// array from the sidecar on upsert / delete).
func (p *Projection) initAggregate(si int, src ProjectionSource, spec AggregateSpec) (*aggregateRuntime, error) {
	ag := src.Aggregate
	where := fmt.Sprintf("source %d (topic %q) aggregate %q", si+1, src.Topic, ag.Column)

	ddl := p.dialect.CreateAggregateSidecarDDL(spec)
	if _, err := p.db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("%s sidecar ddl [%s]: %w", where, ddl, err)
	}

	rt := &aggregateRuntime{
		column:     ag.Column,
		elementKey: ag.ElementKey,
		fields:     plainElementFields(ag.Element), // enriched fields are joined in, not stored
		sidecar:    spec.Sidecar,
	}
	scConfig := sidecarConfig(spec.Sidecar)
	prepare := func(label, sqlString string) (*gosql.Stmt, error) {
		stmt, err := p.db.Prepare(sqlString)
		if err != nil {
			return nil, fmt.Errorf("%s prepare %s [%s]: %w", where, label, sqlString, err)
		}
		return stmt, nil
	}

	var err error
	rt.upsertSidecarSQL = p.dialect.CreateSQL(scConfig)
	if rt.upsertSidecar, err = prepare("sidecar upsert", rt.upsertSidecarSQL); err != nil {
		return nil, err
	}
	rt.deleteSidecarSQL = p.dialect.CreateDeleteSQL(scConfig)
	if rt.deleteSidecar, err = prepare("sidecar delete", rt.deleteSidecarSQL); err != nil {
		return nil, err
	}
	rt.lookupSQL = p.dialect.CreateAggregateParentLookupSQL(spec)
	if rt.lookup, err = prepare("parent lookup", rt.lookupSQL); err != nil {
		return nil, err
	}
	rt.materializeSQL = p.dialect.CreateAggregateMaterializeSQL(spec)
	if rt.materialize, err = prepare("materialize", rt.materializeSQL); err != nil {
		return nil, err
	}
	rt.rebuildSQL = p.dialect.CreateAggregateRebuildSQL(spec)
	if rt.rebuild, err = prepare("rebuild", rt.rebuildSQL); err != nil {
		return nil, err
	}
	return rt, nil
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
		for _, src := range p.sources[e.Type.ID] {
			if err := p.applyEntity(ctx, tx, src, e); err != nil {
				_ = tx.Rollback()
				return false, err
			}
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
			for _, src := range p.sources[e.Type.ID] {
				if err := p.applyEntity(ctx, tx, src, e); err != nil {
					_ = tx.Rollback()
					return false, err
				}
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
		// A delete carries no payload, so the source-level when cannot be
		// evaluated — route it to every source on the topic. An aggregate's
		// remove is keyed by the child Key in its sidecar, so it self-selects:
		// the one source that folded this child removes its element, the others
		// are no-ops. (For a split by when, this is how the right column shrinks.)
		if src.lkp != nil {
			return p.removeFromDimension(ctx, tx, src, e)
		}
		if src.agg != nil {
			if src.onDelete == onDeleteIgnore {
				return nil
			}
			return p.removeFromAggregate(ctx, tx, src, e)
		}
		return p.applyDelete(ctx, tx, src, e)
	}

	var jsonData any
	if err := json.Unmarshal(e.Data, &jsonData); err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.apply] unmarshal entity data: %w", err))
	}

	// Source-level when prefilter: a source consumes only the events it matches,
	// so several sources can split one topic into different columns. An empty
	// when matches every event of the topic.
	if !matchWhen(src.when, jsonData) {
		return nil
	}

	if src.lkp != nil {
		return p.applyLookup(ctx, tx, src, jsonData, e)
	}
	if src.agg != nil {
		return p.applyAggregate(ctx, tx, src, jsonData, e)
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

// applyAggregate folds one child upsert into its parent's array column. It
// records the child in the sidecar (keyed by the child's entity Key, so a
// re-delivered child replaces rather than duplicates) and then re-materializes
// the parent's column from the sidecar — an upsert, so a child arriving before
// its spine lands the collection on a fresh partial row. The parent key binds
// both materialize placeholders.
func (p *Projection) applyAggregate(ctx context.Context, tx *gosql.Tx, src *projectionSource, jsonData any, e *cluster.Entity) error {
	ag := src.agg

	parentKey, err := jsonpath.Get(src.keyPath, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] keyPath [%s]: %w", src.keyPath, err))
	}
	elementKey, err := jsonpath.Get(ag.elementKey, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] elementKey [%s]: %w", ag.elementKey, err))
	}
	element := make(map[string]any, len(ag.fields))
	for _, f := range ag.fields {
		v, err := jsonpath.Get(f.From, jsonData)
		if err != nil {
			return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] element field %q from [%s]: %w", f.Field, f.From, err))
		}
		element[f.Field] = v
	}
	elementJSON, err := json.Marshal(element)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] marshal element: %w", err))
	}

	// Sidecar columns are text/JSON; bind the keys as strings (elementKey is
	// stored as text and ordered with an optional numeric cast) so a numeric
	// jsonpath value never mismatches the column type.
	pk := bindable(parentKey)
	scValues := []any{string(e.Key), pk, keyString(elementKey), string(elementJSON)}
	if err := p.aggExec(ctx, tx, ag.upsertSidecar, ag.upsertSidecarSQL, p.dialect.BindArgs(scValues)...); err != nil {
		return err
	}
	// Both materialize placeholders bind the parent key (insert value + subquery
	// filter); the dialect repeats the placeholder so the arg shape is uniform.
	return p.aggExec(ctx, tx, ag.materialize, ag.materializeSQL, pk, pk)
}

// removeFromAggregate honors a child delete: recover the child's parent from the
// sidecar (a no-op if this source never folded the child — which is how a split
// self-selects), delete the sidecar row, and rebuild the parent's array from
// what remains. The rebuild is an UPDATE, so emptying an absent parent is a
// no-op, never a ghost row.
func (p *Projection) removeFromAggregate(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	ag := src.agg
	childKey := string(e.Key)

	var parentKey string
	row := tx.StmtContext(ctx, ag.lookup).QueryRowContext(ctx, childKey)
	switch err := row.Scan(&parentKey); err {
	case nil:
	case gosql.ErrNoRows:
		return nil // this source never folded the child — nothing to remove
	default:
		wrapped := fmt.Errorf("[sql-projection.aggregate] exec [%s]: %w", ag.lookupSQL, err)
		if p.dialect.IsPermanent(err) {
			return cluster.Permanent(wrapped)
		}
		return wrapped
	}

	if err := p.aggExec(ctx, tx, ag.deleteSidecar, ag.deleteSidecarSQL, childKey); err != nil {
		return err
	}
	return p.aggExec(ctx, tx, ag.rebuild, ag.rebuildSQL, parentKey, parentKey)
}

// aggExec runs one prepared aggregate statement, classifying a permanent error
// the same way the rule path does.
func (p *Projection) aggExec(ctx context.Context, tx *gosql.Tx, stmt *gosql.Stmt, sqlStr string, args ...any) error {
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...); err != nil {
		wrapped := fmt.Errorf("[sql-projection.aggregate] exec [%s]: %w", sqlStr, err)
		if p.dialect.IsPermanent(err) {
			return cluster.Permanent(wrapped)
		}
		return wrapped
	}
	return nil
}

// applyLookup folds one dimension-entity upsert: store its key → fields object
// in the dimension table, then fan out — re-materialize every parent whose
// folded children reference this key, so a value that arrives after the facts
// that reference it fills in (and a changed value updates them).
func (p *Projection) applyLookup(ctx context.Context, tx *gosql.Tx, src *projectionSource, jsonData any, e *cluster.Entity) error {
	lk := src.lkp

	fields := make(map[string]any, len(lk.fields))
	for _, f := range lk.fields {
		v, err := jsonpath.Get(f.From, jsonData)
		if err != nil {
			return cluster.Permanent(fmt.Errorf("[sql-projection.lookup] field %q from [%s]: %w", f.Field, f.From, err))
		}
		fields[f.Field] = v
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.lookup] marshal fields: %w", err))
	}

	// The dimension key is the entity's own Key — the value aggregate elements
	// reference in `on`, and the only key a (payload-less) delete can use.
	key := string(e.Key)
	dimValues := []any{key, string(fieldsJSON)}
	if err := p.aggExec(ctx, tx, lk.upsertDim, lk.upsertDimSQL, p.dialect.BindArgs(dimValues)...); err != nil {
		return err
	}
	return p.fanOut(ctx, tx, lk, key)
}

// removeFromDimension honors a dimension-entity delete: drop the dimension row,
// then fan out — the parents that referenced it re-materialize, their enriched
// fields now null (the materialize LEFT JOIN finds no dimension row).
func (p *Projection) removeFromDimension(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	lk := src.lkp
	key := string(e.Key)
	if err := p.aggExec(ctx, tx, lk.deleteDim, lk.deleteDimSQL, key); err != nil {
		return err
	}
	return p.fanOut(ctx, tx, lk, key)
}

// fanOut re-materializes every parent whose folded children reference the
// changed dimension key. For each dependent aggregate it collects the affected
// parent keys (fully draining the query before any rebuild — a tx holds one
// connection, so a rebuild cannot run while the cursor is open) and rebuilds
// each. Synchronous in the dimension change's transaction; bounded by the
// fan-out degree (see projection-fanout-deferred for the batched option).
func (p *Projection) fanOut(ctx context.Context, tx *gosql.Tx, lk *lookupRuntime, dimKey string) error {
	for _, dep := range lk.dependents {
		rows, err := tx.StmtContext(ctx, dep.affected).QueryContext(ctx, dimKey)
		if err != nil {
			wrapped := fmt.Errorf("[sql-projection.lookup] exec [%s]: %w", dep.affectedSQL, err)
			if p.dialect.IsPermanent(err) {
				return cluster.Permanent(wrapped)
			}
			return wrapped
		}
		var parents []string
		for rows.Next() {
			var pk string
			if err := rows.Scan(&pk); err != nil {
				_ = rows.Close()
				return fmt.Errorf("[sql-projection.lookup] scan affected parent: %w", err)
			}
			parents = append(parents, pk)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("[sql-projection.lookup] affected parents: %w", err)
		}
		_ = rows.Close()

		for _, pk := range parents {
			if err := p.aggExec(ctx, tx, dep.rebuild, dep.rebuildSQL, pk, pk); err != nil {
				return err
			}
		}
	}
	return nil
}

// keyString renders a correlation/element key as the text the sidecar stores. A
// string passes through; nil is empty; a JSON number (float64) prints without a
// decimal point for integral values (ordering 1 → "1", not "1.000000").
func keyString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (p *Projection) Close() error {
	// Close every prepared statement; report the first error but
	// always attempt the rest so nothing leaks when one close fails.
	var err error
	closeStmt := func(s *gosql.Stmt) {
		if s != nil {
			if cerr := s.Close(); err == nil {
				err = cerr
			}
		}
	}
	for _, list := range p.sources {
		for _, src := range list {
			for _, r := range src.rules {
				closeStmt(r.Stmt)
			}
			closeStmt(src.clear)
			if src.agg != nil {
				closeStmt(src.agg.upsertSidecar)
				closeStmt(src.agg.deleteSidecar)
				closeStmt(src.agg.lookup)
				closeStmt(src.agg.materialize)
				closeStmt(src.agg.rebuild)
			}
			if src.lkp != nil {
				closeStmt(src.lkp.upsertDim)
				closeStmt(src.lkp.deleteDim)
				// dependents' rebuild stmts belong to the aggregate runtimes (closed
				// above); only the affected-parents queries are the lookup's own.
				for _, dep := range src.lkp.dependents {
					closeStmt(dep.affected)
				}
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
