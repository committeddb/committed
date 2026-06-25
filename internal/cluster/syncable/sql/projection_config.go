package sql

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
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
