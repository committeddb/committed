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
	arg    Node  // the value (lag/lead/first/last) or aggregate argument; nil for count(*) and ranking
	offset int64 // lag/lead
	def    Node  // lag/lead's default (nil = null)
	filter Node  // a running aggregate's FILTER
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
		t := p.peek()
		return nil, fmt.Errorf("over (…) at position %d: a window stage declares its window once — partitionBy, orderBy, and tieBy on the stage apply to every call", t.pos)
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
