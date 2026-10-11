package stages

import (
	"fmt"
	"math/big"
)

// Window functions — SQL's f(…) OVER (PARTITION BY p ORDER BY o), with
// the window declared once on the stage (SQL's named WINDOW clause): a
// window stage's partitionBy/orderBy/tieBy. In its emit exprs the
// language admits row_number(), rank(), dense_rank(), lag/lead(x [, n [,
// default]]), first_value(x), last_value(x), and the running aggregates
// sum/count/min/max/avg (with filter (where …)) over PostgreSQL's default
// frame — RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so a row's
// peers (equal orderBy values) are inside its frame. Unlike an aggregate
// expression, a window expression reads the row's own fields outside the
// calls: each input keeps its row.
//
// Divergences from PostgreSQL, deliberate and listed: lag/lead's offset is
// a non-negative integer literal (PG evaluates an expression per row) of
// at most windowMaxOffset; running aggregates take no distinct or order by
// (PG implements neither for window aggregates) and no array_agg/
// string_agg (each row's state would grow with the partition); and where
// PG leaves the order within a tie unspecified, the stage's tieBy and then
// the input key decide it.

// windowFns are the window-only functions; aggregate names are running
// aggregates inside a window stage.
var windowFns = map[string]bool{
	"row_number": true, "rank": true, "dense_rank": true,
	"lag": true, "lead": true, "first_value": true, "last_value": true,
}

// tieSensitiveFns are the window functions whose result for a row
// depends on the order WITHIN a peer group — a stage using one must
// declare tieBy.
var tieSensitiveFns = map[string]bool{
	"row_number": true, "lag": true, "lead": true, "first_value": true, "last_value": true,
}

// windowMaxOffset bounds lag/lead offsets: a recompute buffers that many
// rows on either side of a change.
const windowMaxOffset = 1000

// exprWin is one window call; slot indexes the stage's row slots.
type exprWin struct {
	fn     string
	arg    Node      // the value (lag/lead/first/last) or aggregate argument; nil for count(*) and ranking
	offset int64     // lag/lead
	def    Node      // lag/lead's default (nil = null)
	filter Node      // a running aggregate's FILTER
	frame  *winFrame // an explicit frame (nil: the default frame)
	slot   int
}

func (exprWin) node() {}

func (x exprWin) children() []Node {
	var out []Node
	for _, n := range []Node{x.arg, x.def, x.filter} {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

// isAggregate reports whether the call is a running aggregate.
func (x exprWin) isAggregate() bool { return aggregateFns[x.fn] }

// agg is the running aggregate's fold, reusing the aggregate stage's
// accumulator.
func (x exprWin) agg() exprAgg { return exprAgg{fn: x.fn, arg: x.arg, filter: x.filter} }

// windowRow is the payload a window stage's emit evaluates against: the
// row's own object (paths read it) and its window slot results.
type windowRow struct {
	obj any
	res []any
}

// rowObject unwraps a window row for path resolution.
func rowObject(payload any) any {
	if w, ok := payload.(windowRow); ok {
		return w.obj
	}
	return payload
}

// parseWinCall parses a window call after its name.
func (p *exprParser) parseWinCall(fn string, pos int) (Node, error) {
	if p.wins == nil {
		return nil, fmt.Errorf("%s(…) at position %d is a window function — window functions belong in a window stage's emit expr (reduce = \"window\")", fn, pos)
	}
	if p.inWin {
		return nil, fmt.Errorf("%s(…) at position %d is nested inside another window function — SQL does not nest window functions", fn, pos)
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	call := exprWin{fn: fn}
	p.inWin = true
	defer func() { p.inWin = false }()
	switch fn {
	case "row_number", "rank", "dense_rank":
		// no arguments
	case "lag", "lead":
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		call.arg, call.offset = arg, 1
		if t := p.peek(); t.kind == "op" && t.text == "," {
			p.next()
			t := p.next()
			r, ok := new(big.Rat).SetString(t.text)
			if t.kind != "num" || !ok || !r.IsInt() || !r.Num().IsInt64() || r.Num().Int64() > windowMaxOffset {
				return nil, fmt.Errorf("%s's offset must be an integer literal from 0 to %d (position %d) — a per-row offset expression is not supported", fn, windowMaxOffset, t.pos)
			}
			call.offset = r.Num().Int64()
			if t := p.peek(); t.kind == "op" && t.text == "," {
				p.next()
				def, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				call.def = def
			}
		}
	case "first_value", "last_value":
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		call.arg = arg
	case "array_agg", "string_agg":
		return nil, fmt.Errorf("%s(…) at position %d is not a running window aggregate here — each row's state would grow with its partition; collapse with an aggregate stage instead", fn, pos)
	default: // running aggregates
		if t := p.peek(); fn == "count" && t.kind == "op" && t.text == "*" {
			p.next()
			break
		}
		if p.atKeyword("distinct") {
			return nil, fmt.Errorf("%s(distinct …) at position %d: distinct is not supported in a window aggregate (as in PostgreSQL)", fn, pos)
		}
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		call.arg = arg
		if p.atKeyword("order") {
			return nil, fmt.Errorf("%s(… order by …) at position %d: order by inside a window aggregate is not supported (as in PostgreSQL) — the stage's orderBy orders the window", fn, pos)
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if p.atKeyword("filter") {
		if !call.isAggregate() {
			return nil, fmt.Errorf("filter applies to aggregate window functions only, not %s (as in PostgreSQL)", fn)
		}
		p.next()
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		if !p.atKeyword("where") {
			t := p.peek()
			return nil, fmt.Errorf("expected where in filter at position %d, got %q", t.pos, t.text)
		}
		p.next()
		f, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		call.filter = f
	}
	if p.atKeyword("over") {
		t := p.next()
		if !call.isAggregate() && fn != "first_value" && fn != "last_value" {
			return nil, fmt.Errorf("over (…) at position %d: %s does not use a frame (PostgreSQL ignores one there) — frames refine the aggregates and first_value/last_value", t.pos, fn)
		}
		f, err := p.parseFrame()
		if err != nil {
			return nil, err
		}
		call.frame = f
	}
	call.slot = len(*p.wins)
	*p.wins = append(*p.wins, call)
	return call, nil
}

// compileWindowExpr compiles a window stage's emit expr, registering its
// window calls as row slots in *slots.
func compileWindowExpr(src string, slots *[]exprWin) (Node, error) {
	toks, err := lexExpr(src)
	if err != nil {
		return nil, err
	}
	p := &exprParser{toks: toks, wins: slots}
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
	return n, nil
}

// checkWinMaterializable applies the stored-value rule to a window call:
// avg is an exact quotient (under round/trunc to store); lag/lead/first/
// last store the argument's value, and the running aggregates follow the
// aggregate rules.
func checkWinMaterializable(x exprWin, dominated bool) error {
	if x.fn == "avg" && !dominated {
		return fmt.Errorf("avg(...) is an exact quotient with no finite decimal form to store — wrap it in round(..., scale) or trunc(...)")
	}
	if x.arg != nil {
		if err := checkMaterializable(x.arg, dominated || x.fn == "count"); err != nil {
			return err
		}
	}
	if x.def != nil {
		if err := checkMaterializable(x.def, dominated); err != nil {
			return err
		}
	}
	if x.filter != nil {
		return checkMaterializable(x.filter, true)
	}
	return nil
}

// winKey is one compiled window ordering key: an expression with its
// direction and null placement (SQL's ORDER BY item).
type winKey struct {
	expr       Node
	desc       bool
	nullsFirst bool
}

// compileWinKey compiles one orderBy/tieBy entry: `expr [asc|desc]
// [nulls first|last]`, PG's default null placement (NULLS LAST ascending,
// NULLS FIRST descending). Sort keys encode exactly, so a division must
// sit under round/trunc, as for a stored value.
func compileWinKey(src string) (winKey, error) {
	toks, err := lexExpr(src)
	if err != nil {
		return winKey{}, err
	}
	p := &exprParser{toks: toks}
	n, err := p.parseExpr()
	if err != nil {
		return winKey{}, err
	}
	k := winKey{expr: n}
	switch {
	case p.atKeyword("asc"):
		p.next()
	case p.atKeyword("desc"):
		p.next()
		k.desc = true
	}
	k.nullsFirst = k.desc
	if p.atKeyword("nulls") {
		p.next()
		switch {
		case p.atKeyword("first"):
			k.nullsFirst = true
		case p.atKeyword("last"):
			k.nullsFirst = false
		default:
			t := p.peek()
			return winKey{}, fmt.Errorf("expected first or last after nulls at position %d, got %q", t.pos, t.text)
		}
		p.next()
	}
	if t := p.peek(); t.kind != "eof" {
		return winKey{}, fmt.Errorf("unexpected trailing %q at position %d (an order key is an expression, then optionally asc|desc and nulls first|last)", t.text, t.pos)
	}
	if err := checkMaterializable(n, false); err != nil {
		return winKey{}, err
	}
	return k, nil
}

// Window frames — SQL's frame clause, refining the stage's window per call:
// `sum($.x) over (rows between 6 preceding and current row)`. ROWS counts
// physical rows in the ordered partition (peers are separate rows, in the
// stage's tieBy-then-input-key order); RANGE measures by the single orderBy
// value (a number, or a date/time with an interval offset), so peers are
// always in or out together. Offsets are constants. Without an explicit
// frame a call uses PostgreSQL's default, RANGE BETWEEN UNBOUNDED PRECEDING
// AND CURRENT ROW.

// Frame bound kinds.
const (
	boundUnboundedPreceding = iota
	boundPreceding
	boundCurrent
	boundFollowing
	boundUnboundedFollowing
)

// winBound is one frame bound: its kind and, for an offset bound, the
// offset (a row count for ROWS, a number or an interval for RANGE).
type winBound struct {
	kind int
	rows int64 // ROWS offset
	off  any   // RANGE offset: *big.Rat or intervalVal
}

func (b winBound) offset() bool { return b.kind == boundPreceding || b.kind == boundFollowing }

// winFrame is an explicit frame: ROWS or RANGE, from start to end.
type winFrame struct {
	rows       bool
	start, end winBound
}

// defaultFrame is PostgreSQL's frame when none is declared.
var defaultFrame = &winFrame{start: winBound{kind: boundUnboundedPreceding}, end: winBound{kind: boundCurrent}}

// frameOf is a call's effective frame.
func (x exprWin) frameOf() *winFrame {
	if x.frame != nil {
		return x.frame
	}
	return defaultFrame
}

// parseFrame parses `( rows|range [between] bound [and bound] )` after
// over, applying PostgreSQL's frame rules.
func (p *exprParser) parseFrame() (*winFrame, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	f := &winFrame{}
	switch {
	case p.atKeyword("rows"):
		f.rows = true
	case p.atKeyword("range"):
	case p.atKeyword("partition") || p.atKeyword("order"):
		t := p.peek()
		return nil, fmt.Errorf("over (…) at position %d takes only a frame (rows|range between …) — a window stage declares partitionBy and orderBy once, on the stage", t.pos)
	case p.atKeyword("groups"):
		t := p.peek()
		return nil, fmt.Errorf("groups frames at position %d are not supported — use rows or range", t.pos)
	default:
		t := p.peek()
		return nil, fmt.Errorf("expected rows or range in over (…) at position %d, got %q", t.pos, t.text)
	}
	p.next()
	var err error
	if p.atKeyword("between") {
		p.next()
		if f.start, err = p.parseFrameBound(f.rows); err != nil {
			return nil, err
		}
		if !p.atKeyword("and") {
			t := p.peek()
			return nil, fmt.Errorf("expected and in the frame at position %d, got %q", t.pos, t.text)
		}
		p.next()
		if f.end, err = p.parseFrameBound(f.rows); err != nil {
			return nil, err
		}
	} else {
		// The short form: `rows 3 preceding` = between 3 preceding and current row.
		if f.start, err = p.parseFrameBound(f.rows); err != nil {
			return nil, err
		}
		f.end = winBound{kind: boundCurrent}
	}
	if p.atKeyword("exclude") {
		t := p.peek()
		return nil, fmt.Errorf("exclude at position %d is not supported in a frame", t.pos)
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	// PostgreSQL's frame rules.
	switch {
	case f.start.kind == boundUnboundedFollowing:
		return nil, fmt.Errorf("frame start cannot be unbounded following")
	case f.end.kind == boundUnboundedPreceding:
		return nil, fmt.Errorf("frame end cannot be unbounded preceding")
	case f.start.kind == boundCurrent && f.end.kind == boundPreceding:
		return nil, fmt.Errorf("frame starting from current row cannot have preceding rows")
	case f.start.kind == boundFollowing && (f.end.kind == boundPreceding || f.end.kind == boundCurrent):
		return nil, fmt.Errorf("frame starting from following row cannot have preceding rows")
	}
	return f, nil
}

// parseFrameBound parses `unbounded preceding|following`, `current row`,
// or `<constant> preceding|following`.
func (p *exprParser) parseFrameBound(rows bool) (winBound, error) {
	switch {
	case p.atKeyword("unbounded"):
		p.next()
		switch {
		case p.atKeyword("preceding"):
			p.next()
			return winBound{kind: boundUnboundedPreceding}, nil
		case p.atKeyword("following"):
			p.next()
			return winBound{kind: boundUnboundedFollowing}, nil
		}
		t := p.peek()
		return winBound{}, fmt.Errorf("expected preceding or following after unbounded at position %d, got %q", t.pos, t.text)
	case p.atKeyword("current"):
		p.next()
		if !p.atKeyword("row") {
			t := p.peek()
			return winBound{}, fmt.Errorf("expected row after current at position %d, got %q", t.pos, t.text)
		}
		p.next()
		return winBound{kind: boundCurrent}, nil
	}
	start := p.peek().pos
	n, err := p.parseConcat()
	if err != nil {
		return winBound{}, err
	}
	b := winBound{}
	switch {
	case p.atKeyword("preceding"):
		b.kind = boundPreceding
	case p.atKeyword("following"):
		b.kind = boundFollowing
	default:
		t := p.peek()
		return winBound{}, fmt.Errorf("expected preceding or following after the frame offset at position %d, got %q", t.pos, t.text)
	}
	p.next()
	var hasPath bool
	walkExprPaths(n, func(string) { hasPath = true })
	if hasPath {
		return winBound{}, fmt.Errorf("a frame offset (position %d) is a constant — it cannot read the row", start)
	}
	v, err := Eval(n, nil, nil)
	if err != nil {
		return winBound{}, fmt.Errorf("frame offset at position %d: %w", start, err)
	}
	if rows {
		r, ok := v.(*big.Rat)
		if !ok || !r.IsInt() || r.Sign() < 0 || !r.Num().IsInt64() || r.Num().Int64() > windowMaxOffset {
			return winBound{}, fmt.Errorf("a rows frame offset (position %d) must be an integer from 0 to %d", start, windowMaxOffset)
		}
		b.rows = r.Num().Int64()
		return b, nil
	}
	switch t := v.(type) {
	case *big.Rat:
		if t.Sign() < 0 {
			return winBound{}, fmt.Errorf("a range frame offset (position %d) must not be negative", start)
		}
	case intervalVal:
		if intervalCmpValue(t).Sign() < 0 {
			return winBound{}, fmt.Errorf("a range frame offset (position %d) must not be negative", start)
		}
	default:
		return winBound{}, fmt.Errorf("a range frame offset (position %d) is a number or an interval, got %s", start, describeValue(v))
	}
	b.off = v
	return b, nil
}

// rangeBound moves an order value by a RANGE offset toward the preceding
// rows (back) or the following ones — in the order's direction: preceding
// is smaller ascending, larger descending.
func rangeBound(cur, off any, back, desc bool) (any, error) {
	sub := back != desc
	switch c := cur.(type) {
	case *big.Rat:
		o, ok := off.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("a range frame offset of %s does not apply to an orderBy number", describeValue(off))
		}
		if sub {
			return new(big.Rat).Sub(c, o), nil
		}
		return new(big.Rat).Add(c, o), nil
	case timestampVal, dateVal, intervalVal:
		if _, ok := off.(intervalVal); !ok {
			return nil, fmt.Errorf("a range frame offset over an orderBy %s must be an interval (interval '1 hour')", describeValue(cur))
		}
		op := "+"
		if sub {
			op = "-"
		}
		return timeArith(op, c, off)
	}
	return nil, fmt.Errorf("a range frame offset needs an orderBy number, date/time, or interval, got %s", describeValue(cur))
}
