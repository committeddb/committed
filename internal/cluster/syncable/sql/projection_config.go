package sql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/PaesslerAG/jsonpath"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/sqlident"
	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// ProjectionColumn declares one column of the projection table. Unlike
// the plain syncable's Mapping, a column carries no jsonPath — what
// lands in it is decided per event by the rules.
type ProjectionColumn struct {
	Name    string `mapstructure:"name"`
	SQLType string `mapstructure:"type"`
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
	// The fourth arm: scalar lookup enrichment — resolve this column from a
	// declared lookup dimension, joining On (a column this SAME rule sets
	// from the payload — the same-rule constraint is a correctness
	// invariant: the FK and its resolved value always move atomically, so
	// they can never desync through partial rule application) to the
	// dimension's key, selecting one of its declared fields. Semantics are
	// the join-equivalence invariant: the column always equals what
	// `LEFT JOIN dim ON dim.key = on` would return right now — resolved at
	// apply, kept fresh by dimension fan-out (a rename updates every
	// referencing row; a dimension delete NULLs it; a spine row applied
	// before its dimension row starts NULL and heals when it arrives).
	Lookup string `mapstructure:"lookup"`
	On     string `mapstructure:"on"`
	Select string `mapstructure:"select"`
	// The fifth arm: a computed column — a closed-function expression over
	// the event payload (see expr.go for the language and its exact-decimal
	// semantics). Compiled at validation; a config that validates can only
	// fail on data at apply time.
	Expr string `mapstructure:"expr"`

	// compiled is Expr's admission-checked AST, populated by
	// validateProjectionConfig so the apply path never re-parses.
	compiled stages.Node
}

// IsEnrichment reports whether this set entry is the lookup-enrichment arm.
func (s ProjectionSet) IsEnrichment() bool { return s.Lookup != "" }

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
	// onDeleteRows is the forEach analog of delete-row: a parent tombstone
	// (which carries only the parent entity key) cascades to every row the
	// parent fanned out, found via the fan-out sidecar.
	onDeleteRows = "delete-rows"
)

// scalarFn values for an aggregate scalar's fn. Config casing is normalized
// at parse (normalizeScalars lower-cases), so these are the canonical
// internal spellings the validation and dialects match.
const (
	scalarFnCount         = "count"
	scalarFnSum           = "sum"
	scalarFnMin           = "min"
	scalarFnMax           = "max"
	scalarFnCountDistinct = "countdistinct"
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
	// Scalars are cross-row aggregate columns computed over the SAME child
	// set the array column folds (count/sum/min/max/countDistinct) — the
	// same sidecar with a different fold, recomputed absolutely at every
	// child change (never incremented), so redelivery and rebuild converge
	// identically to the array column. When Scalars are present the array
	// Column is optional (a pure count needs no array).
	Scalars []ProjectionScalar
}

// ProjectionScalar is one scalar aggregate column: Fn over the element
// field Of (count needs no Of), optionally restricted to elements whose
// fields match every Where clause (filtered count). Columns are declared
// table columns, written by the same materialize/rebuild statements that
// write the array column.
type ProjectionScalar struct {
	Column string `mapstructure:"column"`
	Fn     string `mapstructure:"fn"`
	Of     string `mapstructure:"of"`
	// OfType ("text" or "number", default text) chooses how min/max order
	// and countDistinct compare the Of field — the elementKeyType precedent.
	// sum always folds numerically; count folds rows and takes neither.
	OfType string        `mapstructure:"ofType"`
	Where  []ScalarWhere `mapstructure:"where"`
}

// ScalarWhere is one clause over an element field, restricting which
// children a filtered scalar counts: exactly one of Equals or Null
// (Null matches SQL IS NULL — a JSON null OR an absent field, the
// mirrored-source reading of DeletedAtUtc IS NULL).
type ScalarWhere struct {
	Field  string `mapstructure:"field"`
	Equals any    `mapstructure:"equals"`
	Null   bool   `mapstructure:"null"`
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
// KeyPath is the jsonpath(s) that locate the correlation key in this source's
// event payload, positionally aligned with the projection's PrimaryKey — for a
// rule source they bind the primary-key columns of every upsert; for an
// aggregate source the single path picks the parent row a child folds into.
// Defaults to one $.<col> per primaryKey column. The projected key values must
// equal the entity's log Key (for a composite key, its components in encoding
// order) for a source's delete Actuals to remove/clear the right row.
type ProjectionSource struct {
	Topic     string
	KeyPath   []string
	OnDelete  string
	When      []WhenClause
	Rules     []ProjectionRule
	Aggregate *ProjectionAggregate
	Lookup    *ProjectionLookup
	// FromStage names the internal stage this source consumes instead of a
	// topic (the chaining terminal): the stage's output deltas — keyed
	// objects, upserts and retractions — drive this source's rules exactly
	// as a topic's entities would. Exactly one of Topic or FromStage.
	FromStage string
	// ForEach turns a rules source into a fan-out: the (deliberately
	// multi-valued) path selects N elements from each event, and the
	// source's rules apply once PER ELEMENT — keyPath and every from/expr
	// path resolve against the element, with a `$parent.` prefix reaching
	// the enclosing event payload. Row identity is the element's key, so
	// one event maintains N rows; a re-emitted event reconciles (rows for
	// vanished elements delete) via the fan-out sidecar, and a parent
	// tombstone cascades per onDelete = "delete-rows".
	ForEach string
}

// ProjectionConfig declares a stateful fold from one or more source topics into
// one current-state table: one row per aggregate key, maintained by per-source
// rules that fire per event. A single-source config is the common case; multiple
// sources fold several normalized topics into one denormalized row (the topic is
// each event's discriminator). See README § SQL projections.
type ProjectionConfig struct {
	Database cluster.Database
	// DatabaseID is the id of the [database] config Database was resolved from
	// (the {section}.db value), threaded by the parser for identity
	// comparison (see Config.DatabaseID). Empty for directly-constructed configs.
	DatabaseID string
	Table      string
	// PrimaryKey is the destination key, one or several columns (the config
	// accepts primaryKey = "id" and primaryKey = ["tenant_id", "visit_id"]).
	// For a composite key the LIST ORDER is part of the producer↔consumer
	// contract: delete tombstones carry the composite entity-key encoding
	// (cluster.CompositeKey), which decodes POSITIONALLY — the columns here
	// must be in the same order as the producer's key components. Composite
	// projections fold via set rules; aggregate/lookup sources and lookup
	// enrichment keep the single-key model (validation rejects the mix).
	PrimaryKey []string
	Columns    []ProjectionColumn
	Sources    []ProjectionSource
	Stages     []ProjectionStage

	// Single-source shorthand. The README single-topic form (and existing
	// configs) set these top-level fields; applyDefaults folds them into one
	// Source. A multi-source config sets Sources directly and leaves these empty.
	Topic   string
	KeyPath []string
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
				switch {
				case s.Aggregate != nil:
					s.OnDelete = onDeleteRemoveFromAggregate // a child delete leaves the row
				case s.ForEach != "":
					s.OnDelete = onDeleteRows // a parent delete cascades to its fanned rows
				default:
					s.OnDelete = onDeleteRow // back-compat: a delete drops the row
				}
			}
			// Default one $.<col> path per key column, positionally aligned
			// with PrimaryKey.
			if len(s.KeyPath) == 0 && len(c.PrimaryKey) > 0 {
				s.KeyPath = make([]string, len(c.PrimaryKey))
				for k, pk := range c.PrimaryKey {
					s.KeyPath[k] = "$." + pk
				}
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

// projectionShapeFingerprint is a canonical, order-independent description of the
// parts of the destination shape that ddlConfig/schemaOf do NOT capture: each
// aggregate column's element fields, elementKey, and elementKeyType, and each
// lookup dimension's fields. materializedSchema folds it into SyncableSchema so
// materializedSchemaChange rejects a re-POST that changes the aggregate/lookup
// shape (which CREATE TABLE IF NOT EXISTS would silently no-op) as a rebuild.
// Sorted so a benign source/field reorder is not flagged; empty when the config
// has no aggregate or lookup source (so plain projections are unaffected).
func (c *ProjectionConfig) projectionShapeFingerprint() string {
	var aggs, lookups, spines []string
	for _, s := range c.Sources {
		if a := s.Aggregate; a != nil {
			aggs = append(aggs, fmt.Sprintf("agg(col=%s,key=%s,keyType=%s,elem=%s)",
				a.Column, a.ElementKey, a.ElementKeyType, fingerprintFields(a.Element)))
		}
		if l := s.Lookup; l != nil {
			lookups = append(lookups, fmt.Sprintf("lookup(name=%s,fields=%s)",
				l.Name, fingerprintFields(l.Fields)))
		}
		// Spine enrichments are value-shape too: a changed (lookup, on,
		// select) silently changes what the column MEANS, which CREATE TABLE
		// IF NOT EXISTS would never surface — so it trips the same
		// materialized-schema-change rebuild gate as aggregate/lookup shape.
		for _, r := range s.Rules {
			for _, e := range r.Set {
				if e.IsEnrichment() {
					spines = append(spines, fmt.Sprintf("spine(col=%s,lookup=%s,on=%s,select=%s)",
						e.Column, e.Lookup, e.On, e.Select))
				}
			}
		}
	}
	sort.Strings(aggs)
	sort.Strings(lookups)
	sort.Strings(spines)
	shape := strings.Join(aggs, ";") + "|" + strings.Join(lookups, ";") + "|" + strings.Join(spines, ";")
	// Stage definitions are value-shape too — and more: editing them RESETS
	// the stage store (the fingerprint mismatch), and a reset store folding
	// forward from a head checkpoint would silently serve partial state for
	// every quiet key. Folding the stage fingerprint into the shape makes a
	// stage edit trip the same rebuild-required gate, pairing every store
	// reset with a replay from index 0.
	if len(c.Stages) > 0 {
		shape += "|stages:" + stageFingerprint(c)
	}
	return shape
}

// fingerprintFields canonicalizes a set of element/dimension fields (order-
// independent) into a stable string carrying every shape-determining attribute.
func fingerprintFields(fields []ProjectionElementField) string {
	fs := make([]string, 0, len(fields))
	for _, f := range fields {
		fs = append(fs, fmt.Sprintf("%s{from=%s,lookup=%s,on=%s,select=%s}",
			f.Field, f.From, f.Lookup, f.On, f.Select))
	}
	sort.Strings(fs)
	return strings.Join(fs, ",")
}

// ruleConfig synthesizes the per-rule upsert Config: the primary-key
// column first, then the rule's set columns in manifest order. Feeding
// it to the dialect's CreateSQL yields exactly the rule-restricted
// upsert the design calls for. The pk self-assignment in the update
// clause (pk = EXCLUDED.pk / pk = ?) is harmless: on conflict the
// values are equal by definition.
func (c *ProjectionConfig) ruleConfig(r ProjectionRule) *Config {
	mappings := make([]Mapping, 0, len(r.Set)+len(c.PrimaryKey))
	for _, pk := range c.PrimaryKey {
		mappings = append(mappings, Mapping{Column: pk})
	}
	for _, s := range r.Set {
		mappings = append(mappings, Mapping{Column: s.Column})
	}
	return &Config{Table: c.Table, Mappings: mappings, PrimaryKey: c.PrimaryKey}
}

// validateSpineEnrichment checks one lookup-arm set entry: the lookup names a
// declared dimension, select names one of its declared fields, and on names a
// column set from the payload (from/value) BY THE SAME RULE — the same-rule
// constraint that makes the FK and its resolved value atomic (see
// ProjectionSet.Lookup).
func validateSpineEnrichment(c *ProjectionConfig, s ProjectionSet, r ProjectionRule, lookups map[string]int, where string) error {
	if s.On == "" || s.Select == "" {
		return fmt.Errorf("%s: lookup requires both on (the FK column this rule sets) and select (the dimension field)", where)
	}
	li, ok := lookups[s.Lookup]
	if !ok {
		return fmt.Errorf("%s: lookup %q is not a declared lookup source", where, s.Lookup)
	}
	fieldOK := false
	for _, f := range c.Sources[li].Lookup.Fields {
		if f.Field == s.Select {
			fieldOK = true
			break
		}
	}
	if !fieldOK {
		return fmt.Errorf("%s: select %q is not a declared field of lookup %q", where, s.Select, s.Lookup)
	}
	onOK := false
	for _, sib := range r.Set {
		if sib.Column == s.On && (sib.From != "" || sib.Value != nil) {
			onOK = true
			break
		}
	}
	if !onOK {
		return fmt.Errorf("%s: on %q must be a column this same rule sets with from/value (the FK and its resolved value must move atomically)", where, s.On)
	}
	// The canonical-join-space contract: the on column's declared type IS the
	// space both join sites (apply-time resolve, dimension fan-out) compare
	// in, coerced Go-side. Integer and text families have one deterministic
	// rendering per value; scale-typed numerics and floats have many
	// ("42" / "42.00" / "42.0" are one value, several spellings), which is a
	// silent-miss trap — and nobody keys entities by fractional numbers, so
	// an on column typed that way is a schema mistake worth a loud no.
	for _, col := range c.Columns {
		if col.Name != s.On {
			continue
		}
		if !onColumnTypeOK(col.SQLType) {
			return fmt.Errorf("%s: on column %q has type %q — an enrichment join column holds another topic's entity key, so it must be an integer-family or text-family type (fractional/scale types have ambiguous renderings and would make dimension fan-out silently miss)", where, s.On, col.SQLType)
		}
	}
	return nil
}

// onColumnTypeOK reports whether a declared type is a valid enrichment join
// column: integer-family or text-family (one deterministic rendering per
// value — the canonical-join-space requirement).
func onColumnTypeOK(sqlType string) bool {
	switch leadingTypeToken(sqlType) {
	case "INT", "INTEGER", "INT2", "INT4", "INT8",
		"SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
		"TEXT", "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR", "NCHAR",
		"TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return true
	}
	return false
}

// validateProjectionConfig rejects every config that could otherwise
// fail only at sync time. It is storage-free so Init can re-validate
// directly constructed configs exactly like parsed ones. Rule indexes
// in errors are 1-based to match the operator's view of the manifest.
func validateProjectionConfig(c *ProjectionConfig) error {
	if c.Table == "" {
		return fmt.Errorf("table is required")
	}
	// Table and column names are quoted before they reach SQL (the projection
	// table, its aggregate sidecars, and lookup dimensions all derive from them),
	// but a control-char / empty identifier — or a free-text column type that can't
	// be quoted — should fail here as a config error, not a deferred driver error.
	if !sqlident.ValidIdent(c.Table) {
		return fmt.Errorf("table is not a valid SQL identifier: %q", c.Table)
	}
	if len(c.PrimaryKey) == 0 {
		return fmt.Errorf("primaryKey is required")
	}
	pkSeen := make(map[string]bool, len(c.PrimaryKey))
	for _, pk := range c.PrimaryKey {
		if pk == "" {
			return fmt.Errorf("primaryKey has an empty column name")
		}
		if pkSeen[pk] {
			return fmt.Errorf("primaryKey column %q listed twice", pk)
		}
		pkSeen[pk] = true
	}
	if len(c.Columns) == 0 {
		return fmt.Errorf("at least one column is required")
	}
	declared := make(map[string]bool, len(c.Columns))
	for _, col := range c.Columns {
		if col.Name == "" {
			return fmt.Errorf("column with empty name")
		}
		if !sqlident.ValidIdent(col.Name) {
			return fmt.Errorf("column %q is not a valid SQL identifier", col.Name)
		}
		if col.SQLType == "" {
			return fmt.Errorf("column %q: type is required", col.Name)
		}
		if !sqlident.ValidTypeExpr(col.SQLType) {
			return fmt.Errorf("column %q has an invalid SQL type %q: only letters, digits, spaces, underscores, parentheses and commas are allowed", col.Name, col.SQLType)
		}
		if declared[col.Name] {
			return fmt.Errorf("column %q declared twice", col.Name)
		}
		declared[col.Name] = true
	}
	for _, pk := range c.PrimaryKey {
		if !declared[pk] {
			return fmt.Errorf("primaryKey %q is not a declared column", pk)
		}
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source (a topic and its rules) is required")
	}
	// Composite keys fold via set rules only: aggregate sidecars and lookup
	// dimensions carry their own single-key identity models (a sidecar's
	// parent binding and a dimension's fan-out address rows by ONE key/on
	// column), and silently mis-keying either would be the silent-divergence
	// class. Reject the mix loudly until composite support is designed for
	// them.
	if len(c.PrimaryKey) > 1 {
		for si, src := range c.Sources {
			if src.Aggregate != nil || src.Lookup != nil {
				return fmt.Errorf("source %d (topic %q): aggregate and lookup sources require a single-column primaryKey (composite-keyed projections fold via set rules only)", si+1, src.Topic)
			}
			for ri, r := range src.Rules {
				for _, s := range r.Set {
					if s.IsEnrichment() {
						return fmt.Errorf("source %d (topic %q) rule %d column %q: lookup enrichment requires a single-column primaryKey", si+1, src.Topic, ri+1, s.Column)
					}
				}
			}
		}
	}
	if err := validateProjectionStages(c); err != nil {
		return err
	}
	forEachTopics := map[string]bool{}
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
		// The lookup name becomes part of its dimension table (dimensionName), which
		// is quoted — but reject a control-char / empty name here as a config error.
		if !sqlident.ValidIdent(src.Lookup.Name) {
			return fmt.Errorf("source %d (topic %q): lookup name is not a valid SQL identifier: %q", si+1, src.Topic, src.Lookup.Name)
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
		if src.FromStage != "" {
			where = fmt.Sprintf("source %d (from %q)", si+1, src.FromStage)
		}
		if src.Topic == "" && src.FromStage == "" {
			return fmt.Errorf("source %d: a source consumes a topic or a stage (topic or from is required)", si+1)
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
		for _, kp := range src.KeyPath {
			if err := rejectMultiValued(kp, where+": keyPath"); err != nil {
				return err
			}
		}
		if src.FromStage != "" {
			if src.Topic != "" {
				return fmt.Errorf("%s: exactly one of topic or from (a source consumes a topic or a stage, not both)", where)
			}
			fi := stageNamed(c, src.FromStage)
			if fi < 0 {
				return fmt.Errorf("%s: from %q names no declared stage", where, src.FromStage)
			}
			if len(c.PrimaryKey) != len(c.Stages[fi].KeyPath) {
				return fmt.Errorf("%s: this table's primaryKey has %d column(s) but stage %q keys by %d path(s) — a stage-fed source's rows are keyed BY the stage's key, so the arities must match", where, len(c.PrimaryKey), src.FromStage, len(c.Stages[fi].KeyPath))
			}
			if src.Aggregate != nil || src.Lookup != nil || src.ForEach != "" {
				return fmt.Errorf("%s: a stage-fed source folds via rules only", where)
			}
			if len(src.Rules) == 0 {
				return fmt.Errorf("%s: a stage-fed source needs rules", where)
			}
			switch src.OnDelete {
			case onDeleteRow, onDeleteIgnore:
			default:
				return fmt.Errorf("%s: onDelete %q is invalid for a stage-fed source (want %q or %q — a stage retraction removes or ignores)", where, src.OnDelete, onDeleteRow, onDeleteIgnore)
			}
		}
		if src.ForEach != "" {
			if src.Aggregate != nil || src.Lookup != nil {
				return fmt.Errorf("%s: forEach applies to a rules source (drop the aggregate/lookup block)", where)
			}
			if len(src.Rules) == 0 {
				return fmt.Errorf("%s: a forEach source needs rules — they apply once per element", where)
			}
			if len(c.PrimaryKey) > 1 {
				return fmt.Errorf("%s: forEach requires a single-column primaryKey (fanned rows key by the element)", where)
			}
			if !multiValuedPath(src.ForEach) {
				return fmt.Errorf("%s: forEach path [%s] is single-valued — forEach fans an array into rows; a plain rules source already gives one row per event", where, src.ForEach)
			}
			switch src.OnDelete {
			case onDeleteRows, onDeleteIgnore:
			default:
				return fmt.Errorf("%s: onDelete %q is invalid for a forEach source (want %q — cascade to the fanned rows — or %q)", where, src.OnDelete, onDeleteRows, onDeleteIgnore)
			}
			// The reconciliation sidecar is named by topic, so two forEach
			// sources sharing one topic would collide on it (and their
			// reconciliation sets would stomp each other).
			if forEachTopics[src.Topic] {
				return fmt.Errorf("%s: a second forEach source on topic %q — one forEach source per topic (split elements with rule whens instead)", where, src.Topic)
			}
			forEachTopics[src.Topic] = true
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
		// A rule source's keyPaths align positionally with the primaryKey
		// columns (applyDefaults fills one $.<col> per column when absent), so
		// an explicit list of the wrong arity is a loud config error, not a
		// misaligned bind.
		if len(src.KeyPath) != len(c.PrimaryKey) {
			return fmt.Errorf("%s: keyPath has %d path(s) but primaryKey has %d column(s) — one path per key column, in the same order", where, len(src.KeyPath), len(c.PrimaryKey))
		}
		if src.ForEach == "" { // a forEach source's onDelete is validated above
			switch src.OnDelete {
			case onDeleteRow, onDeleteClear, onDeleteIgnore:
			default:
				return fmt.Errorf("%s: onDelete %q is invalid (want %q, %q, or %q)", where, src.OnDelete, onDeleteRow, onDeleteClear, onDeleteIgnore)
			}
		}
		ownsColumn := false
		// enrichedAs records each enriched column's (lookup, on, select) tuple:
		// the same column enriched from two DIFFERENT tuples across rules would
		// let one dimension's fan-out stomp the other's value regardless of
		// which rule last wrote the row — the join-equivalence invariant cannot
		// even be stated for that shape. Identical tuples are fine (they dedupe
		// into one fan-out statement at Init).
		enrichedAs := make(map[string]string)
		for i, r := range src.Rules {
			if err := validateWhenClauses(r.When, fmt.Sprintf("%s rule %d", where, i+1)); err != nil {
				return err
			}
			if len(r.Set) == 0 {
				return fmt.Errorf("%s rule %d: set is required", where, i+1)
			}
			seen := make(map[string]bool, len(r.Set))
			for k, s := range r.Set {
				if s.Column == "" {
					return fmt.Errorf("%s rule %d: set entry with empty column", where, i+1)
				}
				if !declared[s.Column] {
					return fmt.Errorf("%s rule %d sets unknown column %q", where, i+1, s.Column)
				}
				if slices.Contains(c.PrimaryKey, s.Column) {
					return fmt.Errorf("%s rule %d may not set the primary-key column %q (the key binds from keyPath)", where, i+1, s.Column)
				}
				if (s.On != "" || s.Select != "") && s.Lookup == "" {
					return fmt.Errorf("%s rule %d column %q: on/select require lookup", where, i+1, s.Column)
				}
				forms := 0
				for _, set := range []bool{s.From != "", s.Value != nil, s.Null, s.Lookup != "", s.Expr != ""} {
					if set {
						forms++
					}
				}
				if forms != 1 {
					return fmt.Errorf("%s rule %d column %q: exactly one of from, value, null, expr, or lookup is required", where, i+1, s.Column)
				}
				if s.Expr != "" {
					compiled, err := compileExpr(s.Expr)
					if err != nil {
						return fmt.Errorf("%s rule %d column %q: expr: %w", where, i+1, s.Column, err)
					}
					r.Set[k].compiled = compiled // r.Set shares the config's backing array
				}
				if s.Value != nil && !isScalar(s.Value) {
					return fmt.Errorf("%s rule %d column %q: value must be a scalar literal, got %T", where, i+1, s.Column, s.Value)
				}
				if s.From != "" {
					if err := rejectMultiValued(s.From, fmt.Sprintf("%s rule %d column %q", where, i+1, s.Column)); err != nil {
						return err
					}
				}
				if s.Lookup != "" {
					if err := validateSpineEnrichment(c, s, r, lookups, fmt.Sprintf("%s rule %d column %q", where, i+1, s.Column)); err != nil {
						return err
					}
					tuple := s.Lookup + "\x00" + s.On + "\x00" + s.Select
					if prev, ok := enrichedAs[s.Column]; ok && prev != tuple {
						return fmt.Errorf("%s rule %d column %q: enriched from a different (lookup, on, select) than an earlier rule — one column, one dimension join (fan-outs from two dimensions would stomp each other)", where, i+1, s.Column)
					}
					enrichedAs[s.Column] = tuple
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
	return validateProjectionJSONPaths(c)
}

// validateJSONPath compiles a configured jsonpath so a syntactically-invalid one
// is rejected at ParseSyncable (a clean 400) instead of failing per-row at
// runtime — where an extraction path dead-letters every row (config-shaped, but
// classified permanent) and a when-clause path silently never matches (the rule
// never fires → wrong/empty output, no error anywhere). jsonpath.Get compiles the
// path internally on every call; jsonpath.New is the same compile without the
// evaluation. The path is config, not secret, so it may appear in the error.
func validateJSONPath(path, where string) error {
	// A `$parent.` prefix is the forEach element scope's way of reaching the
	// enclosing event payload; validate the remainder as a rooted path (the
	// apply layer resolves the scope — see resolveScopedPath).
	checkPath := path
	if rest, ok := strings.CutPrefix(checkPath, "$parent"); ok {
		checkPath = "$" + rest
	}
	if _, err := jsonpath.New(checkPath); err != nil {
		return fmt.Errorf("%s: invalid jsonpath %q: %w", where, path, err)
	}
	return nil
}

// validateProjectionJSONPaths compiles every configured jsonpath in a projection —
// each source's keyPath, its when-clause paths (source- and rule-level), each
// rule set's from, an aggregate's elementKey and its plain element froms, and a
// lookup's plain field froms. Runs after the structural validation above, so a
// path reaching here is already non-empty where required; a from left empty
// (a value/null set, or an enriched field) is legitimately skipped.
func validateProjectionJSONPaths(c *ProjectionConfig) error {
	for si, src := range c.Sources {
		where := fmt.Sprintf("source %d (topic %q)", si+1, src.Topic)
		for _, kp := range src.KeyPath {
			if err := validateJSONPath(kp, where+" keyPath"); err != nil {
				return err
			}
		}
		for _, cl := range src.When {
			if err := validateJSONPath(cl.Path, where+" when"); err != nil {
				return err
			}
		}
		for ri, r := range src.Rules {
			rwhere := fmt.Sprintf("%s rule %d", where, ri+1)
			for _, cl := range r.When {
				if err := validateJSONPath(cl.Path, rwhere+" when"); err != nil {
					return err
				}
			}
			for _, s := range r.Set {
				if s.From == "" {
					continue // a value/null set carries no path
				}
				if err := validateJSONPath(s.From, fmt.Sprintf("%s column %q from", rwhere, s.Column)); err != nil {
					return err
				}
			}
		}
		if ag := src.Aggregate; ag != nil {
			if ag.ElementKey != "" {
				if err := validateJSONPath(ag.ElementKey, where+" aggregate elementKey"); err != nil {
					return err
				}
			}
			if err := validateElementFieldPaths(ag.Element, where+" aggregate"); err != nil {
				return err
			}
		}
		if src.Lookup != nil {
			if err := validateElementFieldPaths(src.Lookup.Fields, where+" lookup"); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateElementFieldPaths compiles the from jsonpath of each PLAIN element
// field (an enriched field pulls from a dimension via lookup/on/select and has no
// from path to validate).
func validateElementFieldPaths(fields []ProjectionElementField, where string) error {
	for _, f := range fields {
		if f.enriched() || f.From == "" {
			continue
		}
		if err := validateJSONPath(f.From, fmt.Sprintf("%s field %q from", where, f.Field)); err != nil {
			return err
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
	if ag.Column == "" && len(ag.Scalars) == 0 {
		return fmt.Errorf("%s: aggregate needs an array column, scalar entries, or both", where)
	}
	if ag.Column != "" {
		if !declared[ag.Column] {
			return fmt.Errorf("%s: aggregate column %q is not a declared column", where, ag.Column)
		}
		if slices.Contains(c.PrimaryKey, ag.Column) {
			return fmt.Errorf("%s: aggregate column %q may not be the primary-key column", where, ag.Column)
		}
		if err := claimColumn(owner, ag.Column, si, where); err != nil {
			return err
		}
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
			if err := rejectMultiValued(f.From, fmt.Sprintf("%s: aggregate element field %q", where, f.Field)); err != nil {
				return err
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
	// Scalars: each is a declared, non-key column this source solely owns;
	// fn from the closed set; of/where reference PLAIN element fields (their
	// values are stored in typed sidecar columns — an enriched field's value
	// lives in the dimension, not the sidecar, so it cannot feed a fold).
	scalarCols := make(map[string]bool, len(ag.Scalars))
	for i, sc := range ag.Scalars {
		swhere := fmt.Sprintf("%s scalar %d", where, i+1)
		if sc.Column == "" {
			return fmt.Errorf("%s: column is required", swhere)
		}
		if !declared[sc.Column] {
			return fmt.Errorf("%s: column %q is not a declared column", swhere, sc.Column)
		}
		if slices.Contains(c.PrimaryKey, sc.Column) {
			return fmt.Errorf("%s: column %q may not be the primary-key column", swhere, sc.Column)
		}
		if sc.Column == ag.Column {
			return fmt.Errorf("%s: column %q is already the aggregate's array column", swhere, sc.Column)
		}
		if scalarCols[sc.Column] {
			return fmt.Errorf("%s: column %q declared twice", swhere, sc.Column)
		}
		scalarCols[sc.Column] = true
		if err := claimColumn(owner, sc.Column, si, where); err != nil {
			return err
		}
		switch sc.OfType {
		case "", elementKeyTypeText, elementKeyTypeNumber:
		default:
			return fmt.Errorf("%s: ofType %q is invalid (want %q or %q)", swhere, sc.OfType, elementKeyTypeText, elementKeyTypeNumber)
		}
		switch sc.Fn {
		case scalarFnCount:
			if sc.Of != "" {
				return fmt.Errorf("%s: count folds rows, not a field — drop of %q (countDistinct folds a field)", swhere, sc.Of)
			}
			if sc.OfType != "" {
				return fmt.Errorf("%s: ofType is only for of-folding fns (min, max, countDistinct)", swhere)
			}
		case scalarFnSum, scalarFnMin, scalarFnMax, scalarFnCountDistinct:
			if sc.Of == "" {
				return fmt.Errorf("%s: %s needs of naming a plain element field", swhere, sc.Fn)
			}
			if !plain[sc.Of] {
				return fmt.Errorf("%s: of %q is not a plain element field", swhere, sc.Of)
			}
		default:
			return fmt.Errorf("%s: fn %q is invalid (want %s, %s, %s, %s, or %s)", swhere, sc.Fn,
				scalarFnCount, scalarFnSum, scalarFnMin, scalarFnMax, scalarFnCountDistinct)
		}
		for _, cl := range sc.Where {
			if cl.Field == "" || !plain[cl.Field] {
				return fmt.Errorf("%s: where field %q is not a plain element field", swhere, cl.Field)
			}
			if (cl.Equals != nil) == cl.Null {
				return fmt.Errorf("%s: where for field %q needs exactly one of equals or null", swhere, cl.Field)
			}
			if cl.Equals != nil && !isScalar(cl.Equals) {
				return fmt.Errorf("%s: where for field %q needs a scalar equals literal", swhere, cl.Field)
			}
		}
	}
	return nil
}

// validateProjectionStages checks the internal-stage declarations: unique
// private names, one input each (a topic id, or a PRIOR stage's name —
// manifest order IS the intra-config DAG, so self/forward references are
// rejected), a single single-valued keyPath, and emit fields whose arms
// match the stage's reduce (reshape: from/expr; aggregate: sum/min/max/
// count). Expression arms compile here (the closed language, division
// domination included).
func validateProjectionStages(c *ProjectionConfig) error {
	if len(c.Stages) == 0 {
		return nil
	}
	return stages.ValidateShapes(c.Stages)
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
		if err := rejectMultiValued(f.From, fmt.Sprintf("%s: lookup %q field %q", where, lk.Name, f.Field)); err != nil {
			return err
		}
		if seen[f.Field] {
			return fmt.Errorf("%s: lookup %q field %q declared twice", where, lk.Name, f.Field)
		}
		seen[f.Field] = true
	}
	return nil
}

// sidecarName is the backing table for an aggregate column: <table>__<column>.
// One per aggregate source; teardown drops them alongside the projection table.
// ForEachSidecarName names a forEach source's reconciliation sidecar from
// its topic, sanitized to identifier characters — stable across config
// edits that reorder sources (unlike a source index). Exported so
// operators (and tests) can locate the sidecar a forEach source maintains.
func ForEachSidecarName(table, topic string) string {
	san := make([]byte, 0, len(topic))
	for i := 0; i < len(topic); i++ {
		c := topic[i]
		switch {
		case c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_':
			san = append(san, c)
		case c >= 'A' && c <= 'Z':
			san = append(san, c+'a'-'A')
		default:
			san = append(san, '_')
		}
	}
	name := sidecarName(table, "foreach_"+string(san))
	// MySQL caps identifiers at 64 bytes (PostgreSQL silently truncates at
	// 63, which is worse — two long names can collide), and the sidecar
	// DDL derives further identifiers from this name (its parent-key
	// index), so the cap leaves suffix room. A long table+topic pair gets
	// a deterministic hash suffix instead: stable across restarts, unique
	// per full name.
	if len(name) > 50 {
		sum := sha256.Sum256([]byte(name))
		name = name[:40] + "_" + hex.EncodeToString(sum[:])[:9]
	}
	return name
}

func sidecarName(table, column string) string {
	return table + "__" + column
}

// dimensionName is the backing table for a lookup source: <table>__lookup_<name>.
// ruleEnrichments maps a rule's enriched columns to their dialect-facing
// specs: the dimension table (derived from the projection table + lookup
// name), the selected field, and the column's declared type (the cast the
// engines that need one apply to the extracted text). Empty map for a rule
// with no enrichment entries — the caller keeps the plain CreateSQL path.
func (c *ProjectionConfig) ruleEnrichments(r ProjectionRule) map[string]SpineEnrichment {
	out := map[string]SpineEnrichment{}
	for _, s := range r.Set {
		if !s.IsEnrichment() {
			continue
		}
		castType := ""
		for _, col := range c.Columns {
			if col.Name == s.Column {
				castType = col.SQLType
				break
			}
		}
		out[s.Column] = SpineEnrichment{
			DimTable:    dimensionName(c.Table, s.Lookup),
			SelectField: s.Select,
			CastType:    castType,
		}
	}
	return out
}

func dimensionName(table, lookup string) string {
	return table + "__lookup_" + lookup
}

// aggregateSpec builds the dialect-facing spec for one aggregate source,
// grouping its enriched element fields by (lookup, on) into one join apiece
// (first-seen order, so the materialize SQL is stable).
func (c *ProjectionConfig) aggregateSpec(ag *ProjectionAggregate) AggregateSpec {
	// The sidecar's identity column: the array column, or — for a
	// scalars-only aggregate — the first scalar column. Renaming that
	// column re-homes the sidecar (rebuilt from the log like any
	// projection state).
	identity := ag.Column
	if identity == "" {
		identity = ag.Scalars[0].Column
	}
	spec := AggregateSpec{
		Table: c.Table,
		// Aggregates are single-key by validation (composite primaryKey +
		// aggregate sources is rejected at parse), so the spec's scalar key
		// is the one configured column.
		PrimaryKey:  c.PrimaryKey[0],
		Column:      ag.Column,
		Sidecar:     sidecarName(c.Table, identity),
		NumericSort: ag.ElementKeyType == elementKeyTypeNumber,
	}
	for _, sc := range ag.Scalars {
		as := AggregateScalar{
			Column:    sc.Column,
			Fn:        sc.Fn,
			Of:        sc.Of,
			OfNumeric: sc.OfType == elementKeyTypeNumber,
		}
		if len(sc.Where) > 0 {
			as.Where = make([]AggregateScalarWhere, 0, len(sc.Where))
			for _, w := range sc.Where {
				as.Where = append(as.Where, AggregateScalarWhere(w))
			}
		}
		spec.Scalars = append(spec.Scalars, as)
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
		PrimaryKey: []string{LookupKey},
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
		PrimaryKey: []string{SidecarChildKey},
		Mappings: []Mapping{
			{Column: SidecarChildKey},
			{Column: SidecarParentKey},
			{Column: SidecarElementKey},
			{Column: SidecarElement},
		},
	}
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
