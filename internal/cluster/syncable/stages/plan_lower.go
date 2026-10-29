package stages

import "fmt"

// Stages lowers the plan to the declared stages today's evaluator runs
// (BuildGraph), admitted and compiled. Engine-written blocks lower back
// into the declarations they came from — a field-addressed join, a topic
// merge side — since BuildGraph writes them again; every other block is
// one stage. An error means the plan does not lower, which is a compiler
// fault, never a config error.
func (p *Plan) Stages() ([]Stage, error) {
	synthetic := map[string]*Block{}
	out := make([]Stage, 0, len(p.Blocks))
	for i := range p.Blocks {
		b := &p.Blocks[i]
		if b.Synthetic != "" {
			synthetic[b.Name] = b
			continue
		}
		st, err := lowerBlock(b, synthetic)
		if err != nil {
			return nil, fmt.Errorf("lower block %q: %w", b.Name, err)
		}
		out = append(out, st)
	}
	if err := ValidateShapes(out); err != nil {
		return nil, fmt.Errorf("lowered stages do not admit: %w", err)
	}
	return out, nil
}

func lowerBlock(b *Block, synthetic map[string]*Block) (Stage, error) {
	st := Stage{Name: b.Name, ElementKey: b.ElementKey, Having: b.Having, When: whenClauses(b.Where)}
	switch len(b.From) {
	case 0:
	case 1:
		st.From = b.From[0]
	default:
		st.FromList = append([]string(nil), b.From...)
	}
	for _, m := range b.Merge {
		side := MergeEntry{Stage: m.Block, As: m.As}
		if lift := synthetic[m.Block]; lift != nil {
			if lift.Synthetic != SyntheticLift || lift.Key == nil || len(lift.From) != 1 {
				return Stage{}, fmt.Errorf("merge side %q is not a lift", m.Block)
			}
			side = MergeEntry{Topic: lift.From[0], As: m.As, KeyPath: lift.Key.Parts, KeyType: compactKeyTypes(lift.Key.Types), Normalize: lift.Key.Normalize}
		}
		st.Merge = append(st.Merge, side)
	}
	switch len(b.Fan) {
	case 0:
	case 1:
		if len(b.Fan[0].Where) > 0 {
			return Stage{}, fmt.Errorf("a single fan step has no where")
		}
		st.ForEach = b.Fan[0].Path
	default:
		for _, f := range b.Fan {
			st.Fan = append(st.Fan, FanArm{ForEach: f.Path, When: whenClauses(f.Where)})
		}
	}
	for i := range b.Joins {
		j, err := lowerJoin(&b.Joins[i], synthetic)
		if err != nil {
			return Stage{}, err
		}
		st.Joins = append(st.Joins, j)
	}
	if b.Key != nil {
		st.KeyPath = b.Key.Parts
		st.KeyType = compactKeyTypes(b.Key.Types)
		st.Normalize = b.Key.Normalize
	}
	r := &b.Reduce
	switch r.Kind {
	case ReduceReshape:
	case ReduceLatest, ReduceAggregate, ReduceLiveSet, ReduceWindow:
		st.Reduce = r.Kind
	default:
		return Stage{}, fmt.Errorf("unknown reduce %q", r.Kind)
	}
	st.OrderBy, st.TieBy = r.OrderPath, r.TiePath
	st.OrderByType, st.TieByType = compactKeyType(r.OrderType), compactKeyType(r.TieType)
	st.WindowOrderBy, st.WindowTieBy = sortKeyTexts(r.OrderBy), sortKeyTexts(r.TieBy)
	st.PartitionBy = r.PartitionBy
	st.DeleteWhen = whenClauses(r.DeleteWhen)
	for _, c := range b.Columns {
		e := Emit{Field: c.Name, From: c.From, Expr: c.Expr, Distinct: c.Distinct, Where: whenClauses(c.Where)}
		switch c.Fold {
		case "":
		case "count":
			e.Count = true
		case "sum":
			e.Sum = c.Arg
		case "min":
			e.Min = c.Arg
		case "max":
			e.Max = c.Arg
		case "collect":
			e.Collect = c.Arg
		default:
			return Stage{}, fmt.Errorf("column %q: unknown fold %q", c.Name, c.Fold)
		}
		st.Emit = append(st.Emit, e)
	}
	return st, nil
}

func lowerJoin(js *JoinStep, synthetic map[string]*Block) (Join, error) {
	j := Join{
		Topic: js.Topic, From: js.Block, On: js.On, Field: js.Field, Absent: js.Absent, Optional: js.Optional,
		Fan: js.Fan, As: js.As, Where: whenClauses(js.Where), AsOf: js.AsOf,
		OrderBy: sortKeyTexts(js.OrderBy), TieBy: sortKeyTexts(js.TieBy),
	}
	switch rekey := synthetic[js.Block]; {
	case rekey != nil:
		// A field-addressed join, declared in its re-key block's key space.
		if rekey.Synthetic != SyntheticRekey || rekey.Key == nil || len(rekey.Key.Parts) != 1 || len(rekey.From) != 1 {
			return Join{}, fmt.Errorf("join target %q is not a re-key", js.Block)
		}
		j.From, j.Field = rekey.From[0], rekey.Key.Parts[0]
		j.OnType, j.Normalize = compactKeyTypes(rekey.Key.Types), rekey.Key.Normalize
	case js.Block != "" && js.Field == "" && !js.Fan && len(js.OrderBy) == 0:
		// A join on a block's key inherits its key space; it declares none.
	default:
		j.OnType, j.Normalize = compactKeyTypes(js.OnTypes), js.Normalize
	}
	return j, nil
}

// compactKeyTypes declares key types the shortest way: none when every
// part is text (the default), else one per part.
func compactKeyTypes(kts []string) []string {
	for _, kt := range kts {
		if kt != KeyTypeText {
			return append([]string(nil), kts...)
		}
	}
	return nil
}

func compactKeyType(kt string) string {
	if kt == KeyTypeText {
		return ""
	}
	return kt
}

func sortKeyTexts(ks []SortKey) []string {
	if len(ks) == 0 {
		return nil
	}
	out := make([]string, 0, len(ks))
	for _, k := range ks {
		out = append(out, k.Expr+sortKeySuffix(k.Desc, k.NullsFirst))
	}
	return out
}

func whenClauses(ps []Predicate) []WhenClause {
	if len(ps) == 0 {
		return nil
	}
	out := make([]WhenClause, 0, len(ps))
	for _, p := range ps {
		c := WhenClause{Path: p.Path}
		var v any
		if p.Value != nil {
			v = p.Value.v
		}
		switch p.Op {
		case OpExpr:
			c = WhenClause{Expr: p.Expr}
		case OpNull:
			c.Null = true
		case OpNotNull:
			c.NotNull = true
		case OpNotEquals:
			c.NotEquals = v
		case OpGreaterThan:
			c.GreaterThan = v
		case OpLessThan:
			c.LessThan = v
		default:
			c.Equals = v
		}
		out = append(out, c)
	}
	return out
}
