package stages

import "fmt"

// CompilePlan compiles declared stages to their plan. The stages are
// admitted first (ValidateShapes, on a copy: the caller's stages are not
// touched), so an error is the admission error.
func CompilePlan(declared []Stage) (*Plan, error) {
	sts := cloneStages(declared)
	if err := ValidateShapes(sts); err != nil {
		return nil, err
	}
	expanded := expandSynthetic(sts)
	p := &Plan{Version: PlanVersion, Blocks: make([]Block, 0, len(expanded))}
	for i := range expanded {
		b, err := compileBlock(expanded, i)
		if err != nil {
			return nil, fmt.Errorf("stage %q: %w", expanded[i].Name, err)
		}
		p.Blocks = append(p.Blocks, b)
	}
	return p, nil
}

func compileBlock(sts []Stage, i int) (Block, error) {
	st := &sts[i]
	b := Block{Name: st.Name, Synthetic: st.synthetic, From: append([]string(nil), st.Sources()...), ElementKey: st.ElementKey, Having: renderOpt(st.havingCompiled)}
	for _, m := range st.Merge {
		as := m.As
		if as == "" {
			as = m.Stage
		}
		b.Merge = append(b.Merge, MergeSide{Block: m.Stage, As: as})
	}
	if st.ForEach != "" {
		b.Fan = []FanStep{{Path: st.ForEach}}
	}
	for _, arm := range st.Fan {
		b.Fan = append(b.Fan, FanStep{Path: arm.ForEach, Where: predicates(arm.When)})
	}
	for j := range st.Joins {
		js, err := compileJoin(sts, &st.Joins[j])
		if err != nil {
			return Block{}, err
		}
		b.Joins = append(b.Joins, js)
	}
	b.Where = predicates(st.When)
	if len(st.Merge) == 0 && st.Reduce != "window" {
		parts, err := KeyParts(st.KeyPath)
		if err != nil {
			return Block{}, err
		}
		b.Key = &Key{Parts: parts, Types: resolveKeyTypes(st.KeyType, len(parts)), Normalize: st.Normalize}
	}
	r, err := compileReduce(st)
	if err != nil {
		return Block{}, err
	}
	b.Reduce = r
	for k := range st.Emit {
		b.Columns = append(b.Columns, compileColumn(&st.Emit[k]))
	}
	return b, nil
}

func compileJoin(sts []Stage, j *Join) (JoinStep, error) {
	js := JoinStep{
		Topic: j.Topic, Block: j.From, On: append([]string(nil), j.On...), Field: j.Field,
		Absent: j.Absent, Optional: j.Optional, Fan: j.Fan, As: j.As, Where: predicates(j.Where),
		Normalize: j.Normalize, OnTypes: resolveKeyTypes(j.OnType, len(j.On)), AsOf: renderOpt(j.asOfExpr),
	}
	if j.From != "" && j.Field == "" && !j.Fan && !j.ordered() {
		// A join on an earlier block's key reads that block's key space
		// (BuildGraph resolves it the same way).
		fi := IndexOf(sts, j.From)
		if fi < 0 {
			return JoinStep{}, fmt.Errorf("join from %q names no stage", j.From)
		}
		_, kt, norm := ResolvedKeySpace(sts, fi)
		js.OnTypes, js.Normalize = resolveKeyTypes(kt, len(j.On)), norm
	}
	js.OrderBy = sortKeys(j.orderKeys)
	js.TieBy = sortKeys(j.tieKeys)
	return js, nil
}

func compileReduce(st *Stage) (Reduce, error) {
	r := Reduce{Kind: st.Reduce, DeleteWhen: predicates(st.DeleteWhen)}
	switch st.Reduce {
	case "":
		r.Kind = ReduceReshape
	case ReduceLatest:
		if len(st.winOrder) == 0 {
			r.OrderPath, r.OrderType = st.OrderBy, resolveKeyType(st.OrderByType)
			r.TiePath, r.TieType = st.TieBy, resolveKeyType(st.TieByType)
		}
	case ReduceWindow:
		parts, err := KeyParts(st.PartitionBy)
		if err != nil {
			return Reduce{}, err
		}
		r.PartitionBy = parts
	}
	r.OrderBy = sortKeys(st.winOrder)
	r.TieBy = sortKeys(st.winTie)
	return r, nil
}

func compileColumn(e *Emit) Column {
	c := Column{Name: e.Field, From: e.From, Distinct: e.Distinct, Where: predicates(e.Where)}
	switch {
	case e.From != "":
	case e.Count:
		c.Fold = "count"
	case e.Sum != "":
		c.Fold, c.Arg = "sum", renderOpt(e.compiled)
	case e.Min != "":
		c.Fold, c.Arg = "min", renderOpt(e.compiled)
	case e.Max != "":
		c.Fold, c.Arg = "max", renderOpt(e.compiled)
	case e.Collect != "":
		c.Fold, c.Arg = "collect", renderOpt(e.compiled)
	default:
		c.Expr = renderOpt(e.compiled)
	}
	return c
}

// KeyParts prints key parts canonically: a path as written, an expression
// rendered.
func KeyParts(kps []string) ([]string, error) {
	out := make([]string, len(kps))
	for i, kp := range kps {
		n, isExpr, err := ClassifyKeyPath(kp)
		if err != nil {
			return nil, err
		}
		out[i] = kp
		if isExpr {
			out[i] = Render(n)
		}
	}
	return out, nil
}

// resolveKeyTypes states each of n key parts' type (KeyTypeAt).
func resolveKeyTypes(kts []string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = resolveKeyType(KeyTypeAt(kts, i))
	}
	return out
}

func resolveKeyType(kt string) string {
	if kt == "" {
		return KeyTypeText
	}
	return kt
}

func sortKeys(ks []winKey) []SortKey {
	if len(ks) == 0 {
		return nil
	}
	out := make([]SortKey, 0, len(ks))
	for _, k := range ks {
		out = append(out, SortKey{Expr: Render(k.expr), Desc: k.desc, NullsFirst: k.nullsFirst})
	}
	return out
}

// Predicates compiles a conjunction of when clauses to the plan's form,
// compiling any expression arm admission has not compiled already.
func Predicates(cls []WhenClause) ([]Predicate, error) {
	var out []Predicate
	for i := range cls {
		c := &cls[i]
		p := Predicate{Path: c.Path}
		switch {
		case c.Expr != "":
			n := c.compiled
			if n == nil {
				var err error
				if n, err = Compile(c.Expr); err != nil {
					return nil, err
				}
			}
			p = Predicate{Op: OpExpr, Expr: Render(n)}
		case c.Null:
			p.Op = OpNull
		case c.NotNull:
			p.Op = OpNotNull
		case c.NotEquals != nil:
			p.Op, p.Value = OpNotEquals, NewLiteral(c.NotEquals)
		case c.GreaterThan != nil:
			p.Op, p.Value = OpGreaterThan, NewLiteral(c.GreaterThan)
		case c.LessThan != nil:
			p.Op, p.Value = OpLessThan, NewLiteral(c.LessThan)
		default:
			p.Op, p.Value = OpEquals, NewLiteral(c.Equals)
		}
		out = append(out, p)
	}
	sortPredicates(out)
	return out, nil
}

// predicates is Predicates over admitted clauses, whose expressions are
// compiled.
func predicates(cls []WhenClause) []Predicate {
	ps, err := Predicates(cls)
	if err != nil {
		panic(fmt.Sprintf("stages: an admitted when clause does not compile: %v", err))
	}
	return ps
}

func renderOpt(n Node) string {
	if n == nil {
		return ""
	}
	return Render(n)
}

// cloneStages copies stages deeply enough that admitting the copy (which
// compiles into the clauses, joins, and emits) leaves the original as it
// was.
func cloneStages(sts []Stage) []Stage {
	out := make([]Stage, len(sts))
	for i := range sts {
		st := sts[i]
		st.When = cloneWhen(st.When)
		st.DeleteWhen = cloneWhen(st.DeleteWhen)
		st.Joins = append([]Join(nil), st.Joins...)
		for j := range st.Joins {
			st.Joins[j].Where = cloneWhen(st.Joins[j].Where)
		}
		st.Emit = append([]Emit(nil), st.Emit...)
		for k := range st.Emit {
			st.Emit[k].Where = cloneWhen(st.Emit[k].Where)
		}
		st.Fan = append([]FanArm(nil), st.Fan...)
		for a := range st.Fan {
			st.Fan[a].When = cloneWhen(st.Fan[a].When)
		}
		st.Merge = append([]MergeEntry(nil), st.Merge...)
		out[i] = st
	}
	return out
}

func cloneWhen(cls []WhenClause) []WhenClause {
	return append([]WhenClause(nil), cls...)
}
