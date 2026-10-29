package stages

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Render prints a compiled expression as canonical source text: the plan
// carries expressions in this form, so two spellings of one expression
// (case, spacing, redundant parentheses, `x::int` against `cast(x as
// integer)`, `between` against its two comparisons) are one plan. Every
// operation is parenthesized and every keyword lower case, and the text
// compiles back to the same tree in the context it came from (a plain
// expression, an aggregate stage's emit or having, a window stage's emit).
// Paths print as written: two spellings of one path are not proven to be
// the same path, so they stay distinct.
func Render(n Node) string {
	var sb strings.Builder
	render(&sb, n)
	return sb.String()
}

func render(sb *strings.Builder, n Node) {
	switch x := n.(type) {
	case exprNum:
		sb.WriteString(ratText(x.val))
	case exprStr:
		sb.WriteString(quoteText(x.val))
	case exprPath:
		sb.WriteString(x.path)
	case exprBool:
		sb.WriteString(strconv.FormatBool(x.val))
	case exprNull:
		sb.WriteString("null")
	case exprSource:
		sb.WriteString("source()")
	case exprNeg:
		sb.WriteString("(-")
		render(sb, x.operand)
		sb.WriteByte(')')
	case exprNot:
		sb.WriteString("(not ")
		render(sb, x.operand)
		sb.WriteByte(')')
	case exprBin:
		if x, lo, hi, ok := betweenParts(x); ok {
			sb.WriteByte('(')
			render(sb, x)
			sb.WriteString(" between ")
			render(sb, lo)
			sb.WriteString(" and ")
			render(sb, hi)
			sb.WriteByte(')')
			return
		}
		sb.WriteByte('(')
		render(sb, x.l)
		sb.WriteString(" " + x.op + " ")
		render(sb, x.r)
		sb.WriteByte(')')
	case exprIsNull:
		sb.WriteByte('(')
		render(sb, x.operand)
		if x.negate {
			sb.WriteString(" is not null)")
		} else {
			sb.WriteString(" is null)")
		}
	case exprIn:
		sb.WriteByte('(')
		render(sb, x.operand)
		sb.WriteString(" in (")
		renderList(sb, x.list)
		sb.WriteString("))")
	case exprLike:
		sb.WriteByte('(')
		render(sb, x.operand)
		sb.WriteString(" like ")
		render(sb, x.pattern)
		sb.WriteByte(')')
	case exprCall:
		sb.WriteString(x.fn + "(")
		renderList(sb, x.args)
		sb.WriteByte(')')
	case exprCast:
		if x.try {
			sb.WriteString("try_cast(")
		} else {
			sb.WriteString("cast(")
		}
		render(sb, x.operand)
		sb.WriteString(" as " + x.typ + ")")
	case exprSubscript:
		sb.WriteByte('(')
		render(sb, x.operand)
		sb.WriteString(")[")
		if x.lo != nil {
			render(sb, x.lo)
		}
		if x.slice {
			sb.WriteByte(':')
			if x.hi != nil {
				render(sb, x.hi)
			}
		}
		sb.WriteByte(']')
	case exprCase:
		if l, r, negate, ok := distinctFromParts(x); ok {
			sb.WriteByte('(')
			render(sb, l)
			if negate {
				sb.WriteString(" is not distinct from ")
			} else {
				sb.WriteString(" is distinct from ")
			}
			render(sb, r)
			sb.WriteByte(')')
			return
		}
		sb.WriteString("case")
		if x.operand != nil {
			sb.WriteByte(' ')
			render(sb, x.operand)
		}
		for i := range x.whens {
			sb.WriteString(" when ")
			render(sb, x.whens[i])
			sb.WriteString(" then ")
			render(sb, x.thens[i])
		}
		if x.els != nil {
			sb.WriteString(" else ")
			render(sb, x.els)
		}
		sb.WriteString(" end")
	case exprAgg:
		renderAgg(sb, x)
	case exprWin:
		renderWin(sb, x)
	default:
		// Every node type is listed above; a new one must be added there,
		// and the render fidelity test fails until it is.
		fmt.Fprintf(sb, "<unrenderable %T>", n)
	}
}

func renderList(sb *strings.Builder, list []Node) {
	for i, n := range list {
		if i > 0 {
			sb.WriteString(", ")
		}
		render(sb, n)
	}
}

func renderAgg(sb *strings.Builder, x exprAgg) {
	sb.WriteString(x.fn + "(")
	if x.arg == nil {
		sb.WriteByte('*')
	} else {
		if x.distinct {
			sb.WriteString("distinct ")
		}
		render(sb, x.arg)
		if x.fn == "string_agg" {
			sb.WriteString(", " + quoteText(x.sep))
		}
		for i, o := range x.order {
			if i == 0 {
				sb.WriteString(" order by ")
			} else {
				sb.WriteString(", ")
			}
			renderSortKey(sb, o.expr, o.desc, o.nullsFirst)
		}
	}
	sb.WriteByte(')')
	renderFilter(sb, x.filter)
}

func renderWin(sb *strings.Builder, x exprWin) {
	sb.WriteString(x.fn + "(")
	switch {
	case x.fn == "lag" || x.fn == "lead":
		render(sb, x.arg)
		if x.offset != 1 || x.def != nil {
			sb.WriteString(", " + strconv.FormatInt(x.offset, 10))
		}
		if x.def != nil {
			sb.WriteString(", ")
			render(sb, x.def)
		}
	case x.arg != nil:
		render(sb, x.arg)
	case x.isAggregate():
		sb.WriteByte('*') // count(*)
	}
	sb.WriteByte(')')
	renderFilter(sb, x.filter)
	if x.frame != nil {
		sb.WriteString(" over (")
		if x.frame.rows {
			sb.WriteString("rows")
		} else {
			sb.WriteString("range")
		}
		sb.WriteString(" between ")
		renderBound(sb, x.frame.start, x.frame.rows)
		sb.WriteString(" and ")
		renderBound(sb, x.frame.end, x.frame.rows)
		sb.WriteByte(')')
	}
}

func renderFilter(sb *strings.Builder, filter Node) {
	if filter != nil {
		sb.WriteString(" filter (where ")
		render(sb, filter)
		sb.WriteByte(')')
	}
}

func renderBound(sb *strings.Builder, b winBound, rows bool) {
	switch b.kind {
	case boundUnboundedPreceding:
		sb.WriteString("unbounded preceding")
		return
	case boundUnboundedFollowing:
		sb.WriteString("unbounded following")
		return
	case boundCurrent:
		sb.WriteString("current row")
		return
	}
	switch {
	case rows:
		sb.WriteString(strconv.FormatInt(b.rows, 10))
	default:
		switch off := b.off.(type) {
		case *big.Rat:
			sb.WriteString(ratText(off))
		case intervalVal:
			sb.WriteString("interval " + quoteText(formatInterval(off)))
		default:
			fmt.Fprintf(sb, "<unrenderable offset %T>", b.off)
		}
	}
	if b.kind == boundPreceding {
		sb.WriteString(" preceding")
	} else {
		sb.WriteString(" following")
	}
}

// betweenParts recognizes `x between lo and hi` — the conjunction
// (lo <= x) and (x <= hi) over one x. It prints back as between: printing
// the conjunction would write x twice, and an aggregate or window call
// written twice compiles to two calls.
func betweenParts(b exprBin) (x, lo, hi Node, ok bool) {
	if b.op != "and" {
		return nil, nil, nil, false
	}
	l, lok := b.l.(exprBin)
	r, rok := b.r.(exprBin)
	if !lok || !rok || l.op != "<=" || r.op != "<=" || !sameTree(l.r, r.l) {
		return nil, nil, nil, false
	}
	return l.r, l.l, r.r, true
}

// distinctFromParts recognizes the case `l is [not] distinct from r`
// compiles to (parseDistinctFrom), for the same reason as betweenParts.
func distinctFromParts(c exprCase) (l, r Node, negate, ok bool) {
	if c.operand != nil || len(c.whens) != 2 || c.els == nil {
		return nil, nil, false, false
	}
	w0, ok0 := c.whens[0].(exprIsNull)
	w1, ok1 := c.whens[1].(exprIsNull)
	t0, ok2 := c.thens[0].(exprIsNull)
	t1, ok3 := c.thens[1].(exprBool)
	e, ok4 := c.els.(exprBin)
	if !ok0 || !ok1 || !ok2 || !ok3 || !ok4 || w0.negate || w1.negate {
		return nil, nil, false, false
	}
	l, r = w0.operand, w1.operand
	negate = !t1.val
	wantOp := "<>"
	if negate {
		wantOp = "="
	}
	if t0.negate == negate || !sameTree(t0.operand, r) || e.op != wantOp || !sameTree(e.l, l) || !sameTree(e.r, r) {
		return nil, nil, false, false
	}
	return l, r, negate, true
}

// renderSortKey prints one order key: the expression, then its direction
// and null placement (sortKeySuffix).
func renderSortKey(sb *strings.Builder, n Node, desc, nullsFirst bool) {
	render(sb, n)
	sb.WriteString(sortKeySuffix(desc, nullsFirst))
}

// sortKeySuffix prints an order key's direction and null placement where
// they differ from the defaults (ascending; nulls last ascending, first
// descending).
func sortKeySuffix(desc, nullsFirst bool) string {
	s := ""
	if desc {
		s = " desc"
	}
	switch {
	case nullsFirst == desc:
	case nullsFirst:
		s += " nulls first"
	default:
		s += " nulls last"
	}
	return s
}

// renderWinKey prints a window or latest order key.
func renderWinKey(k winKey) string {
	var sb strings.Builder
	renderSortKey(&sb, k.expr, k.desc, k.nullsFirst)
	return sb.String()
}

// quoteText prints a string literal: single quotes, a quote doubled.
func quoteText(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// ratText prints an exact number: its decimal digits when it has a finite
// decimal form (every literal does), otherwise the quotient that is it.
func ratText(r *big.Rat) string {
	if s, err := FormatRat(r); err == nil {
		return s
	}
	return "(" + r.Num().String() + " / " + r.Denom().String() + ")"
}

// sameTree compares two compiled trees exactly: numbers by value, and
// aggregate and window calls by slot too, so one call written once is not
// the same tree as the same call written twice.
func sameTree(a, b Node) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch x := a.(type) {
	case exprNum:
		y, ok := b.(exprNum)
		return ok && x.val.Cmp(y.val) == 0
	case exprStr:
		y, ok := b.(exprStr)
		return ok && x == y
	case exprPath:
		y, ok := b.(exprPath)
		return ok && x == y
	case exprBool:
		y, ok := b.(exprBool)
		return ok && x == y
	case exprNull:
		_, ok := b.(exprNull)
		return ok
	case exprSource:
		_, ok := b.(exprSource)
		return ok
	case exprNeg:
		y, ok := b.(exprNeg)
		return ok && sameTree(x.operand, y.operand)
	case exprNot:
		y, ok := b.(exprNot)
		return ok && sameTree(x.operand, y.operand)
	case exprBin:
		y, ok := b.(exprBin)
		return ok && x.op == y.op && sameTree(x.l, y.l) && sameTree(x.r, y.r)
	case exprIsNull:
		y, ok := b.(exprIsNull)
		return ok && x.negate == y.negate && sameTree(x.operand, y.operand)
	case exprIn:
		y, ok := b.(exprIn)
		return ok && sameTree(x.operand, y.operand) && sameTrees(x.list, y.list)
	case exprLike:
		y, ok := b.(exprLike)
		return ok && sameTree(x.operand, y.operand) && sameTree(x.pattern, y.pattern)
	case exprCall:
		y, ok := b.(exprCall)
		return ok && x.fn == y.fn && sameTrees(x.args, y.args)
	case exprCast:
		y, ok := b.(exprCast)
		return ok && x.typ == y.typ && x.try == y.try && sameTree(x.operand, y.operand)
	case exprSubscript:
		y, ok := b.(exprSubscript)
		return ok && x.slice == y.slice && sameTree(x.operand, y.operand) && sameTree(x.lo, y.lo) && sameTree(x.hi, y.hi)
	case exprCase:
		y, ok := b.(exprCase)
		return ok && sameTree(x.operand, y.operand) && sameTrees(x.whens, y.whens) && sameTrees(x.thens, y.thens) && sameTree(x.els, y.els)
	case exprAgg:
		y, ok := b.(exprAgg)
		if !ok || x.fn != y.fn || x.distinct != y.distinct || x.slot != y.slot || x.sep != y.sep || len(x.order) != len(y.order) ||
			!sameTree(x.arg, y.arg) || !sameTree(x.filter, y.filter) {
			return false
		}
		for i := range x.order {
			if x.order[i].desc != y.order[i].desc || x.order[i].nullsFirst != y.order[i].nullsFirst || !sameTree(x.order[i].expr, y.order[i].expr) {
				return false
			}
		}
		return true
	case exprWin:
		y, ok := b.(exprWin)
		if !ok || x.fn != y.fn || x.offset != y.offset || x.slot != y.slot || (x.frame == nil) != (y.frame == nil) ||
			!sameTree(x.arg, y.arg) || !sameTree(x.def, y.def) || !sameTree(x.filter, y.filter) {
			return false
		}
		return x.frame == nil || (x.frame.rows == y.frame.rows && sameBound(x.frame.start, y.frame.start) && sameBound(x.frame.end, y.frame.end))
	}
	return false
}

func sameTrees(a, b []Node) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameTree(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sameBound(a, b winBound) bool {
	if a.kind != b.kind || a.rows != b.rows {
		return false
	}
	switch x := a.off.(type) {
	case nil:
		return b.off == nil
	case intervalVal:
		y, ok := b.off.(intervalVal)
		return ok && x == y
	case *big.Rat:
		y, ok := b.off.(*big.Rat)
		return ok && x.Cmp(y) == 0
	}
	return false
}
