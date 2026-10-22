package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// A projection's plan is its stages' plan (stages.Plan) and its terminal:
// the destination table and the sources that write it, in the same
// spelling-free form — the single-source shorthand folded in, every
// default stated (each source's keyPath and onDelete, an aggregate's
// elementKeyType, a scalar's ofType), when clauses and expressions as the
// stages' plan has them, column types as the schema guard compares them.
//
// The table sources still run on their own path, not the plan; the plan
// states what they compute so that it can be tested, fingerprinted, and
// later executed.

// tablePlanVersion is the version of the terminal's encoding.
const tablePlanVersion = 1

// ProjectionPlan is a projection's plan.
type ProjectionPlan struct {
	Stages *stages.Plan `json:"stages"`
	Table  TablePlan    `json:"table"`
}

// TablePlan is the terminal: the destination table and its sources.
type TablePlan struct {
	Version    int           `json:"version"`
	Name       string        `json:"name"`
	PrimaryKey []string      `json:"primaryKey"`
	Columns    []TableColumn `json:"columns"`
	Sources    []TableSource `json:"sources"`
}

// TableColumn is one destination column.
type TableColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// TableSource is one source, in the vocabulary of ProjectionSource: a
// topic or a stage (Block), optionally fanned, filtered, and keyed, that
// writes columns by rules, an aggregate column, or a lookup dimension.
type TableSource struct {
	Topic     string             `json:"topic,omitempty"`
	Block     string             `json:"block,omitempty"`
	Fan       string             `json:"fan,omitempty"`
	Where     []stages.Predicate `json:"where,omitempty"`
	Key       []string           `json:"key,omitempty"`
	Normalize string             `json:"normalize,omitempty"`
	OnDelete  string             `json:"onDelete,omitempty"`
	RowOwner  bool               `json:"rowOwner,omitempty"`
	Rules     []TableRule        `json:"rules,omitempty"`
	Aggregate *TableAggregate    `json:"aggregate,omitempty"`
	Lookup    *TableLookup       `json:"lookup,omitempty"`
}

// TableRule is one rule: its sets apply where its clauses hold. Rules
// keep their order (a later rule's write to a column wins).
type TableRule struct {
	Where []stages.Predicate `json:"where,omitempty"`
	Set   []TableSet         `json:"set"`
}

// TableSet is one column write: a path, a literal, null, an expression,
// or a lookup enrichment.
type TableSet struct {
	Column string          `json:"column"`
	From   string          `json:"from,omitempty"`
	Value  *stages.Literal `json:"value,omitempty"`
	Null   bool            `json:"null,omitempty"`
	Expr   string          `json:"expr,omitempty"`
	Lookup string          `json:"lookup,omitempty"`
	On     string          `json:"on,omitempty"`
	Select string          `json:"select,omitempty"`
}

// TableAggregate is an aggregate column and its scalars.
type TableAggregate struct {
	Column         string        `json:"column,omitempty"`
	ElementKey     string        `json:"elementKey,omitempty"`
	ElementKeyType string        `json:"elementKeyType"`
	Element        []TableField  `json:"element,omitempty"`
	Scalars        []TableScalar `json:"scalars,omitempty"`
}

// TableField is one stored field of an aggregate element or a dimension.
type TableField struct {
	Field  string `json:"field"`
	From   string `json:"from,omitempty"`
	Lookup string `json:"lookup,omitempty"`
	On     string `json:"on,omitempty"`
	Select string `json:"select,omitempty"`
}

// TableScalar is one scalar aggregate column.
type TableScalar struct {
	Column string             `json:"column"`
	Fn     string             `json:"fn"`
	Of     string             `json:"of,omitempty"`
	OfType string             `json:"ofType,omitempty"`
	Where  []TableScalarWhere `json:"where,omitempty"`
}

// TableScalarWhere is one clause of a filtered scalar.
type TableScalarWhere struct {
	Field  string          `json:"field"`
	Equals *stages.Literal `json:"equals,omitempty"`
	Null   bool            `json:"null,omitempty"`
}

// TableLookup is a lookup dimension.
type TableLookup struct {
	Name   string       `json:"name"`
	Fields []TableField `json:"fields"`
}

// compilePlan compiles an admitted config (applyDefaults has run) to its
// plan.
func (c *ProjectionConfig) compilePlan() (*ProjectionPlan, error) {
	sp, err := stages.CompilePlan(c.Stages)
	if err != nil {
		return nil, err
	}
	t := TablePlan{Version: tablePlanVersion, Name: c.Table, PrimaryKey: append([]string(nil), c.PrimaryKey...)}
	for _, col := range c.Columns {
		t.Columns = append(t.Columns, TableColumn{Name: col.Name, Type: normalizeType(col.SQLType)})
	}
	for si := range c.Sources {
		ts, err := compileTableSource(&c.Sources[si])
		if err != nil {
			return nil, fmt.Errorf("source %d: %w", si+1, err)
		}
		t.Sources = append(t.Sources, ts)
	}
	return &ProjectionPlan{Stages: sp, Table: t}, nil
}

func compileTableSource(s *ProjectionSource) (TableSource, error) {
	where, err := stages.Predicates(s.When)
	if err != nil {
		return TableSource{}, err
	}
	ts := TableSource{
		Topic: s.Topic, Block: s.FromStage, Fan: s.ForEach, Where: where,
		Key: append([]string(nil), s.KeyPath...), Normalize: s.Normalize, OnDelete: s.OnDelete, RowOwner: s.RowOwner,
	}
	for ri := range s.Rules {
		r := &s.Rules[ri]
		rw, err := stages.Predicates(r.When)
		if err != nil {
			return TableSource{}, fmt.Errorf("rule %d: %w", ri+1, err)
		}
		tr := TableRule{Where: rw}
		for _, set := range r.Set {
			ts := TableSet{Column: set.Column, From: set.From, Null: set.Null, Lookup: set.Lookup, On: set.On, Select: set.Select}
			if set.Value != nil {
				ts.Value = stages.NewLiteral(set.Value)
			}
			if set.Expr != "" {
				n := set.compiled
				if n == nil {
					if n, err = compileExpr(set.Expr); err != nil {
						return TableSource{}, fmt.Errorf("rule %d column %q: %w", ri+1, set.Column, err)
					}
				}
				ts.Expr = stages.Render(n)
			}
			tr.Set = append(tr.Set, ts)
		}
		ts.Rules = append(ts.Rules, tr)
	}
	if a := s.Aggregate; a != nil {
		ta := &TableAggregate{Column: a.Column, ElementKey: a.ElementKey, ElementKeyType: a.ElementKeyType, Element: tableFields(a.Element)}
		for _, sc := range a.Scalars {
			tsc := TableScalar{Column: sc.Column, Fn: sc.Fn, Of: sc.Of, OfType: sc.OfType}
			if sc.Of != "" && tsc.OfType == "" {
				tsc.OfType = elementKeyTypeText // ofType defaults to text where it applies
			}
			for _, w := range sc.Where {
				tw := TableScalarWhere{Field: w.Field, Null: w.Null}
				if w.Equals != nil {
					tw.Equals = stages.NewLiteral(w.Equals)
				}
				tsc.Where = append(tsc.Where, tw)
			}
			ta.Scalars = append(ta.Scalars, tsc)
		}
		ts.Aggregate = ta
	}
	if l := s.Lookup; l != nil {
		ts.Lookup = &TableLookup{Name: l.Name, Fields: tableFields(l.Fields)}
	}
	return ts, nil
}

func tableFields(fs []ProjectionElementField) []TableField {
	if len(fs) == 0 {
		return nil
	}
	out := make([]TableField, 0, len(fs))
	for _, f := range fs {
		out = append(out, TableField{Field: f.Field, From: f.From, Lookup: f.Lookup, On: f.On, Select: f.Select})
	}
	return out
}

// Encode returns the plan's canonical encoding (compact JSON, no HTML
// escaping, as stages.Plan.Encode).
func (p *ProjectionPlan) Encode() []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(p); err != nil {
		panic(fmt.Sprintf("sql: encode projection plan: %v", err))
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// Fingerprint identifies the plan: the hash of its canonical encoding.
func (p *ProjectionPlan) Fingerprint() string {
	sum := sha256.Sum256(p.Encode())
	return hex.EncodeToString(sum[:])
}
