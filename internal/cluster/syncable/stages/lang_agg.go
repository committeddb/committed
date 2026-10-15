package stages

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
)

// Aggregate expressions — SQL's select list over a GROUP BY. In an
// aggregate stage's emit `expr` and its `having`, the language admits
// aggregate calls: sum(x), min(x), max(x), avg(x), count(*), count(x),
// each optionally `distinct` (count, sum, avg) and `filter (where …)`
// (SQL's FILTER). Every call is a fold slot, accumulated over the key's
// retained inputs in the same single pass as the stage's fold arms; the
// expression then evaluates over the slot results, so
// round(sum($.rev) / nullif(sum($.cost), 0), 4) is one emit. Outside an
// aggregate call an aggregate expression reads no input path: the value
// would come from an arbitrary input (SQL's "must appear in GROUP BY"),
// and the key itself reaches the table through the stage key. avg is an
// exact quotient, so like `/` it must sit under round/trunc to be stored.
//
// array_agg(x [order by …]) and string_agg(x, 'sep' [order by …]) fold the
// values themselves. PG's semantics hold — array_agg keeps nulls,
// string_agg skips them and takes text only, both are null over an empty
// set, an order by sorts ASC NULLS LAST / DESC NULLS FIRST by default, and
// with distinct the order by must be the argument — with one deliberate
// addition: where PG leaves the order unspecified (no order by, or a tie),
// values sort by themselves (nulls last), so equal folds are byte-equal.

// aggregateFns are the aggregate calls; distinctFns take `distinct`.
var (
	aggregateFns = map[string]bool{"sum": true, "min": true, "max": true, "avg": true, "count": true, "array_agg": true, "string_agg": true}
	distinctFns  = map[string]bool{"sum": true, "avg": true, "count": true, "array_agg": true, "string_agg": true}
	orderedFns   = map[string]bool{"array_agg": true, "string_agg": true}
)

// exprAgg is one aggregate call; slot indexes the stage's fold slots.
type exprAgg struct {
	fn       string
	arg      Node // nil for count(*)
	filter   Node // nil without FILTER
	distinct bool
	slot     int
	order    []aggOrder // array_agg/string_agg's order by
	sep      string     // string_agg's delimiter (a literal)
}

// aggOrder is one order-by key of an ordered aggregate.
type aggOrder struct {
	expr       Node
	desc       bool
	nullsFirst bool
}

func (exprAgg) node() {}

// aggResults is the payload a post-fold expression evaluates against:
// slot results by index.
type aggResults []any

// parseAggCall parses the rest of an aggregate call after its name.
func (p *exprParser) parseAggCall(fn string, pos int) (Node, error) {
	if p.aggs == nil {
		return nil, fmt.Errorf("%s(…) at position %d is an aggregate — aggregates belong in an aggregate stage's emit expr or having", fn, pos)
	}
	if p.inAgg {
		return nil, fmt.Errorf("%s(…) at position %d is nested inside another aggregate — SQL does not nest aggregates", fn, pos)
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	call := exprAgg{fn: fn}
	if t := p.peek(); fn == "count" && t.kind == "op" && t.text == "*" {
		p.next()
	} else {
		if p.atKeyword("distinct") {
			if !distinctFns[fn] {
				return nil, fmt.Errorf("%s does not take distinct (min/max are already per-value)", fn)
			}
			p.next()
			call.distinct = true
		}
		p.inAgg = true
		argStart := p.i
		arg, err := p.parseExpr()
		p.inAgg = false
		if err != nil {
			return nil, err
		}
		call.arg = arg
		argText := p.spanText(argStart)
		if fn == "string_agg" {
			if err := p.expectOp(","); err != nil {
				return nil, fmt.Errorf("string_agg takes a value and a delimiter: %w", err)
			}
			t := p.next()
			if t.kind != "str" {
				return nil, fmt.Errorf("string_agg's delimiter must be a string literal like ', ' (position %d)", t.pos)
			}
			call.sep = t.text
		}
		if orderedFns[fn] && p.atKeyword("order") {
			if err := p.parseAggOrder(&call, argText); err != nil {
				return nil, err
			}
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if p.atKeyword("filter") {
		p.next()
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		if !p.atKeyword("where") {
			t := p.peek()
			return nil, fmt.Errorf("expected where in filter at position %d, got %q", t.pos, t.text)
		}
		p.next()
		p.inAgg = true
		f, err := p.parseExpr()
		p.inAgg = false
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		call.filter = f
	}
	call.slot = len(*p.aggs)
	*p.aggs = append(*p.aggs, call)
	return call, nil
}

// parseAggOrder parses `order by e [asc|desc] [nulls first|last], …`.
// With distinct, every key must be the argument itself (PG's rule: which
// duplicate's sort key would win is otherwise undefined).
func (p *exprParser) parseAggOrder(call *exprAgg, argText string) error {
	p.next() // order
	if !p.atKeyword("by") {
		t := p.peek()
		return fmt.Errorf("expected by after order at position %d, got %q", t.pos, t.text)
	}
	p.next()
	for {
		start := p.i
		p.inAgg = true
		e, err := p.parseExpr()
		p.inAgg = false
		if err != nil {
			return err
		}
		if call.distinct && p.spanText(start) != argText {
			return fmt.Errorf("in %s with distinct, the order by must be the argument itself (as in PostgreSQL)", call.fn)
		}
		o := aggOrder{expr: e}
		switch {
		case p.atKeyword("asc"):
			p.next()
		case p.atKeyword("desc"):
			p.next()
			o.desc = true
		}
		o.nullsFirst = o.desc
		if p.atKeyword("nulls") {
			p.next()
			switch {
			case p.atKeyword("first"):
				o.nullsFirst = true
			case p.atKeyword("last"):
				o.nullsFirst = false
			default:
				t := p.peek()
				return fmt.Errorf("expected first or last after nulls at position %d, got %q", t.pos, t.text)
			}
			p.next()
		}
		call.order = append(call.order, o)
		if t := p.peek(); t.kind == "op" && t.text == "," {
			p.next()
			continue
		}
		return nil
	}
}

// spanText renders the tokens consumed since start, for comparing two
// expressions' spellings (identifiers case-folded, as the language folds
// keywords and function names).
func (p *exprParser) spanText(start int) string {
	parts := make([]string, 0, p.i-start)
	for _, t := range p.toks[start:p.i] {
		text := t.text
		if t.kind == "ident" {
			text = strings.ToLower(text)
		}
		parts = append(parts, t.kind+":"+text)
	}
	return strings.Join(parts, " ")
}

// CompileAggregate compiles an aggregate stage's emit expr or having,
// registering its aggregate calls as fold slots in *slots.
func compileAggregate(src string, slots *[]exprAgg) (Node, error) {
	toks, err := lexExpr(src)
	if err != nil {
		return nil, err
	}
	p := &exprParser{toks: toks, aggs: slots}
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != "eof" {
		return nil, fmt.Errorf("unexpected trailing %q at position %d", t.text, t.pos)
	}
	if err := checkMaterializable(n, false); err != nil {
		return nil, err
	}
	if err := rejectPathsOutsideAggregates(n); err != nil {
		return nil, err
	}
	return n, nil
}

// rejectPathsOutsideAggregates is SQL's grouping rule, narrowed: in an
// aggregate expression every input path sits inside an aggregate call.
func rejectPathsOutsideAggregates(n Node) error {
	switch x := n.(type) {
	case exprPath:
		return fmt.Errorf("path %s is outside an aggregate — in an aggregate stage every input path sits inside sum/min/max/avg/count (one key folds many inputs; the key itself reaches the table through the stage key)", x.path)
	case exprSource:
		return fmt.Errorf("source() is outside an aggregate — it names one input's source, and one key folds many inputs; use it inside an aggregate call (count(*) filter (where source() = 'returns')), or key by it")
	case exprAgg:
		return nil
	}
	for _, c := range exprChildren(n) {
		if err := rejectPathsOutsideAggregates(c); err != nil {
			return err
		}
	}
	return nil
}

// exprChildren lists a node's direct subexpressions.
func exprChildren(n Node) []Node {
	switch x := n.(type) {
	case exprNeg:
		return []Node{x.operand}
	case exprBin:
		return []Node{x.l, x.r}
	case exprCall:
		return x.args
	case exprNot:
		return []Node{x.operand}
	case exprIn:
		return append([]Node{x.operand}, x.list...)
	case exprIsNull:
		return []Node{x.operand}
	case exprLike:
		return []Node{x.operand, x.pattern}
	case exprCast:
		return []Node{x.operand}
	case exprCase:
		var out []Node
		if x.operand != nil {
			out = append(out, x.operand)
		}
		for i := range x.whens {
			out = append(out, x.whens[i], x.thens[i])
		}
		if x.els != nil {
			out = append(out, x.els)
		}
		return out
	case exprAgg:
		var out []Node
		if x.arg != nil {
			out = append(out, x.arg)
		}
		if x.filter != nil {
			out = append(out, x.filter)
		}
		for _, o := range x.order {
			out = append(out, o.expr)
		}
		return out
	case exprWin:
		return x.children()
	case exprSubscript:
		return x.children()
	}
	return nil
}

// aggAcc accumulates one fold slot over a key's inputs.
type aggAcc struct {
	n     int64 // rows (count) or summed values (avg)
	sum   *big.Rat
	ivSum *intervalVal
	best  any
	seen  map[string]bool
	items []aggItem // array_agg/string_agg values with their order keys
}

type aggItem struct {
	val any
	ord []any
}

// add folds one input into slot acc. FILTER admits only TRUE (a data
// error filters the input out, as a when arm does); a null argument skips
// (SQL); sum/avg add numbers and skip other values as the sum arm does,
// except that sum adds intervals (PG's sum(interval)).
func (s *exprAgg) add(acc *aggAcc, obj, parent any) error {
	if s.filter != nil {
		if v, err := Eval(s.filter, obj, parent); err != nil || v != true {
			return nil
		}
	}
	if s.arg == nil { // count(*)
		acc.n++
		return nil
	}
	v, err := Eval(s.arg, obj, parent)
	if err != nil {
		return err
	}
	if s.fn == "array_agg" || s.fn == "string_agg" {
		return s.addItem(acc, v, obj, parent)
	}
	if v == nil {
		return nil
	}
	if s.distinct {
		k := distinctKey(v)
		if acc.seen == nil {
			acc.seen = map[string]bool{}
		}
		if acc.seen[k] {
			return nil
		}
		acc.seen[k] = true
	}
	switch s.fn {
	case "count":
		acc.n++
	case "min":
		if acc.best == nil || collectLess(v, acc.best) {
			acc.best = v
		}
	case "max":
		if acc.best == nil || collectLess(acc.best, v) {
			acc.best = v
		}
	case "sum", "avg":
		switch t := v.(type) {
		case *big.Rat:
			if acc.ivSum != nil {
				return fmt.Errorf("%s mixes numbers and intervals", s.fn)
			}
			if acc.sum == nil {
				acc.sum = new(big.Rat)
			}
			acc.sum.Add(acc.sum, t)
			acc.n++
		case intervalVal:
			if s.fn == "avg" {
				return fmt.Errorf("avg needs numbers, got %s", describeValue(v))
			}
			if acc.sum != nil {
				return fmt.Errorf("sum mixes numbers and intervals")
			}
			if acc.ivSum == nil {
				acc.ivSum = &intervalVal{}
			}
			r, err := checkInterval(intervalVal{acc.ivSum.months + t.months, acc.ivSum.days + t.days, acc.ivSum.us + t.us})
			if err != nil {
				return err
			}
			iv := r.(intervalVal)
			acc.ivSum = &iv
		}
	}
	return nil
}

// addItem folds one value of an ordered aggregate: array_agg keeps a
// null, string_agg skips it and needs text.
func (s *exprAgg) addItem(acc *aggAcc, v, obj, parent any) error {
	if s.fn == "string_agg" {
		if v == nil {
			return nil
		}
		if _, ok := v.(string); !ok {
			return fmt.Errorf("string_agg needs text, got %s (cast it: x::text)", describeValue(v))
		}
	}
	if s.distinct {
		k := "null|"
		if v != nil {
			k = distinctKey(v)
		}
		if acc.seen == nil {
			acc.seen = map[string]bool{}
		}
		if acc.seen[k] {
			return nil
		}
		acc.seen[k] = true
	}
	ord := make([]any, len(s.order))
	for i, o := range s.order {
		ov, err := Eval(o.expr, obj, parent)
		if err != nil {
			return err
		}
		ord[i] = ov
	}
	acc.items = append(acc.items, aggItem{v, ord})
	return nil
}

// orderedItems sorts an ordered aggregate's values: by the order-by keys
// (each with its direction and null placement), then by the value itself
// (nulls last) — so ties, and an aggregate without order by, are
// deterministic.
func (s *exprAgg) orderedItems(items []aggItem) []aggItem {
	sort.SliceStable(items, func(a, b int) bool {
		for i, o := range s.order {
			if c := compareNullable(items[a].ord[i], items[b].ord[i], o.nullsFirst); c != 0 {
				if o.desc && items[a].ord[i] != nil && items[b].ord[i] != nil {
					c = -c
				}
				return c < 0
			}
		}
		return compareNullable(items[a].val, items[b].val, false) < 0
	})
	return items
}

// compareNullable orders two values by the shared value order, a null
// sorting first or last as asked.
func compareNullable(a, b any, nullsFirst bool) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		if nullsFirst {
			return -1
		}
		return 1
	case b == nil:
		if nullsFirst {
			return 1
		}
		return -1
	case collectLess(a, b):
		return -1
	case collectLess(b, a):
		return 1
	}
	return 0
}

// result is the slot's value over the whole set — SQL's empty-set
// semantics: count is 0, the others null.
func (s *exprAgg) result(acc *aggAcc) any {
	switch s.fn {
	case "count":
		return new(big.Rat).SetInt64(acc.n)
	case "min", "max":
		return acc.best
	case "sum":
		if acc.ivSum != nil {
			return *acc.ivSum
		}
		if acc.sum == nil {
			return nil
		}
		return acc.sum
	case "avg":
		if acc.sum == nil || acc.n == 0 {
			return nil
		}
		return new(big.Rat).Quo(acc.sum, new(big.Rat).SetInt64(acc.n))
	case "array_agg", "string_agg":
		if len(acc.items) == 0 {
			return nil // SQL: an aggregate over no rows is null, not empty
		}
		items := s.orderedItems(acc.items)
		if s.fn == "string_agg" {
			parts := make([]string, len(items))
			for i, it := range items {
				parts[i] = it.val.(string)
			}
			return strings.Join(parts, s.sep)
		}
		vals := make([]any, len(items))
		for i, it := range items {
			vals[i] = it.val
		}
		return vals
	}
	return nil
}

// distinctKey is a value's identity for distinct: typed (the number 5 and
// the string "5" differ), with equal values — 5 and 5.0, a date and its
// midnight timestamp, '1 mon' and '30 days' — sharing one key, as SQL's
// DISTINCT compares them.
func distinctKey(v any) string {
	switch t := v.(type) {
	case *big.Rat:
		return "n|" + t.RatString()
	case string:
		return "s|" + t
	case bool:
		return fmt.Sprintf("b|%t", t)
	case timestampVal, dateVal:
		us, _ := timeInstantUs(t)
		return fmt.Sprintf("t|%d", us)
	case intervalVal:
		return "i|" + intervalCmpValue(t).String()
	}
	return fmt.Sprintf("?|%v", v)
}

// compileForPathCheck compiles an emit expr or having for a path walk that
// runs before the emits are compiled; a compile error is reported by the
// emit validation, so it yields nil here.
func compileForPathCheck(st *Stage, src string) Node {
	var n Node
	var err error
	switch st.Reduce {
	case "aggregate":
		var scratch []exprAgg
		n, err = compileAggregate(src, &scratch)
	case "window":
		var scratch []exprWin
		n, err = compileWindowExpr(src, &scratch)
	default:
		n, err = Compile(src)
	}
	if err != nil {
		return nil
	}
	return n
}
