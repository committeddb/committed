package stages

import (
	"fmt"
	"math/big"
)

// Array subscripts and slices — PostgreSQL's (arr)[i] and (arr)[lo:hi] on
// an array VALUE (array_agg's result): 1-based; a subscript outside the
// array is null; a slice clips to the array and is empty when nothing
// remains; a null array or a null bound gives null; a non-integer position
// rounds as PG's integer cast does (half away from zero). PG requires the
// parentheses, and so does the language. Payload arrays are not array
// values here — they are addressed by jsonpath, 0-based ($.items[0]).

// exprSubscript is (operand)[lo] or, with slice, (operand)[lo:hi] (a nil
// bound is omitted: the array's first or last element).
type exprSubscript struct {
	operand Node
	lo, hi  Node
	slice   bool
}

func (exprSubscript) node() {}

func (x exprSubscript) children() []Node {
	out := []Node{x.operand}
	for _, n := range []Node{x.lo, x.hi} {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

// parseSubscript parses `[i]`, `[lo:hi]`, `[:hi]`, `[lo:]`, or `[:]`.
func (p *exprParser) parseSubscript(operand Node) (Node, error) {
	p.next() // [
	p.subscripts++
	defer func() { p.subscripts-- }()
	x := exprSubscript{operand: operand}
	isColon := func() bool { t := p.peek(); return t.kind == "op" && t.text == ":" }
	isClose := func() bool { t := p.peek(); return t.kind == "op" && t.text == "]" }
	if isClose() {
		return nil, fmt.Errorf("an empty subscript [] — write [i] or [lo:hi]")
	}
	var err error
	if !isColon() {
		if x.lo, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	if isColon() {
		p.next()
		x.slice = true
		if !isClose() {
			if x.hi, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}
	}
	if !isClose() {
		t := p.peek()
		return nil, fmt.Errorf("expected ] to close the subscript at position %d, got %q", t.pos, t.text)
	}
	p.next()
	if !x.slice && x.lo == nil {
		return nil, fmt.Errorf("an empty subscript [] — write [i] or [lo:hi]")
	}
	return x, nil
}

func evalSubscript(x exprSubscript, payload, parent any) (any, error) {
	v, err := Eval(x.operand, payload, parent)
	if err != nil {
		return nil, err
	}
	bound := func(n Node) (int64, bool, error) {
		bv, err := Eval(n, payload, parent)
		if err != nil || bv == nil {
			return 0, false, err
		}
		r, ok := bv.(*big.Rat)
		if !ok {
			return 0, false, fmt.Errorf("an array subscript is an integer, got %s", describeValue(bv))
		}
		iv, err := castValue(r, "integer")
		if err != nil {
			return 0, false, err
		}
		return iv.(*big.Rat).Num().Int64(), true, nil
	}
	var lo, hi int64
	var loOK, hiOK bool
	if x.lo != nil {
		if lo, loOK, err = bound(x.lo); err != nil || !loOK {
			return nil, err
		}
	}
	if x.hi != nil {
		if hi, hiOK, err = bound(x.hi); err != nil || !hiOK {
			return nil, err
		}
	}
	if v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("a subscript needs an array (array_agg's result), got %s", describeValue(v))
	}
	n := int64(len(arr))
	if !x.slice {
		if lo < 1 || lo > n {
			return nil, nil
		}
		return arr[lo-1], nil
	}
	if x.lo == nil {
		lo = 1
	}
	if x.hi == nil {
		hi = n
	}
	lo, hi = max(lo, 1), min(hi, n)
	if lo > hi {
		return []any{}, nil
	}
	return append([]any(nil), arr[lo-1:hi]...), nil
}

// evalCardinality is PG's cardinality(array): the element count, 0 for an
// empty array, null for a null one.
func evalCardinality(x exprCall, payload, parent any) (any, error) {
	v, err := Eval(x.args[0], payload, parent)
	if err != nil || v == nil {
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("cardinality needs an array (array_agg's result), got %s", describeValue(v))
	}
	return new(big.Rat).SetInt64(int64(len(arr))), nil
}
