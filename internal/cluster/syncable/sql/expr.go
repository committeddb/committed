package sql

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/PaesslerAG/jsonpath"
)

// The projection expression language (`expr` set entries): a closed function
// set over exact decimal arithmetic, specified in the pipeline design's
// decimal-arithmetic section. The semantics in one paragraph: every numeric
// value is an exact rational built from its JSON source digits (json.Number
// end-to-end — float64 never participates); `+ - *` are exact and `/` is
// exact rational division; rounding happens ONLY at an explicit round
// (half away from zero) or trunc (toward zero); a missing payload field is
// null and null propagates through arithmetic and comparison, with coalesce
// and nullif the only null-aware functions. Because only division can
// produce a value with no finite decimal representation, every `/` must be
// dominated by a round/trunc (or comparison — booleans never materialize a
// decimal) ancestor in its tree, checked at admission: a bare quotient that
// could reach a column is a config error, never a runtime surprise.
//
// Grammar (recursive descent, no precedence surprises):
//
//	expr       := additive (compareOp additive)?
//	additive   := multiplicative (('+'|'-') multiplicative)*
//	multiplicative := unary (('*'|'/') unary)*
//	unary      := '-' unary | primary
//	primary    := NUMBER | PATH | IDENT '(' expr (',' expr)* ')' | '(' expr ')'
//	compareOp  := '=' | '<>' | '<' | '<=' | '>' | '>='
//
// NUMBER is a plain decimal literal (no exponent form); STRING is a
// SQL-style single-quoted literal with '' as the escaped quote (needed by
// real formulas like nullif($.GroupName, '')); PATH is a `$…` jsonpath
// into the event payload; IDENT names one of the closed function set:
// coalesce, nullif, round, trunc (matched case-insensitively).

// exprMaxBits caps the combined numerator+denominator size of any
// intermediate value — unreachable for admissible formulas (the domination
// rule bounds unrounded division depth); pure pathology insurance.
const exprMaxBits = 4096

type exprNode interface{ exprNode() }

type exprNum struct{ val *big.Rat }

type exprStr struct{ val string }

type exprPath struct{ path string }

type exprNeg struct{ operand exprNode }

type exprBin struct {
	op   string
	l, r exprNode
}

type exprCall struct {
	fn   string // canonical lower-case name from the closed set
	args []exprNode
}

func (exprNum) exprNode()  {}
func (exprStr) exprNode()  {}
func (exprPath) exprNode() {}
func (exprNeg) exprNode()  {}
func (exprBin) exprNode()  {}
func (exprCall) exprNode() {}

// ── lexing ──────────────────────────────────────────────────────────────

type exprToken struct {
	kind string // "num", "path", "ident", "op", "eof"
	text string
	pos  int
}

func lexExpr(src string) ([]exprToken, error) {
	var toks []exprToken
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c >= '0' && c <= '9':
			start := i
			seenDot := false
			for i < len(src) && (src[i] >= '0' && src[i] <= '9' || src[i] == '.' && !seenDot) {
				if src[i] == '.' {
					seenDot = true
				}
				i++
			}
			toks = append(toks, exprToken{"num", src[start:i], start})
		case c == '$':
			// A jsonpath runs to the next delimiter; bracket segments may
			// contain anything (quoted keys with spaces), so they are consumed
			// to their closing bracket.
			start := i
			i++
			for i < len(src) {
				if src[i] == '[' {
					depth := 1
					i++
					for i < len(src) && depth > 0 {
						if src[i] == '[' {
							depth++
						}
						if src[i] == ']' {
							depth--
						}
						i++
					}
					continue
				}
				if strings.ContainsRune(" \t\n\r+-*/(),=<>", rune(src[i])) {
					break
				}
				i++
			}
			toks = append(toks, exprToken{"path", src[start:i], start})
		case c == '\'':
			// SQL-style string literal; '' is the escaped quote.
			i++
			var sb strings.Builder
			closed := false
			for i < len(src) {
				if src[i] == '\'' {
					if i+1 < len(src) && src[i+1] == '\'' {
						sb.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				sb.WriteByte(src[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string literal starting at position %d", i)
			}
			toks = append(toks, exprToken{"str", sb.String(), i})
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
			start := i
			for i < len(src) && (src[i] >= 'a' && src[i] <= 'z' || src[i] >= 'A' && src[i] <= 'Z' || src[i] >= '0' && src[i] <= '9' || src[i] == '_') {
				i++
			}
			toks = append(toks, exprToken{"ident", src[start:i], start})
		case c == '<':
			if i+1 < len(src) && (src[i+1] == '=' || src[i+1] == '>') {
				toks = append(toks, exprToken{"op", src[i : i+2], i})
				i += 2
			} else {
				toks = append(toks, exprToken{"op", "<", i})
				i++
			}
		case c == '>':
			if i+1 < len(src) && src[i+1] == '=' {
				toks = append(toks, exprToken{"op", ">=", i})
				i += 2
			} else {
				toks = append(toks, exprToken{"op", ">", i})
				i++
			}
		case strings.ContainsRune("+-*/(),=", rune(c)):
			toks = append(toks, exprToken{"op", string(c), i})
			i++
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", string(c), i)
		}
	}
	toks = append(toks, exprToken{"eof", "", len(src)})
	return toks, nil
}

// ── parsing ─────────────────────────────────────────────────────────────

type exprParser struct {
	toks []exprToken
	i    int
}

func (p *exprParser) peek() exprToken { return p.toks[p.i] }

func (p *exprParser) next() exprToken { t := p.toks[p.i]; p.i++; return t }

func (p *exprParser) expectOp(op string) error {
	t := p.next()
	if t.kind != "op" || t.text != op {
		return fmt.Errorf("expected %q at position %d, got %q", op, t.pos, t.text)
	}
	return nil
}

var exprFunctions = map[string]struct{ minArgs, maxArgs int }{
	"coalesce": {2, 16},
	"nullif":   {2, 2},
	"round":    {2, 2},
	"trunc":    {1, 2},
}

func (p *exprParser) parseExpr() (exprNode, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == "op" && (t.text == "=" || t.text == "<>" || t.text == "<" || t.text == "<=" || t.text == ">" || t.text == ">=") {
		p.next()
		r, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return exprBin{t.text, l, r}, nil
	}
	return l, nil
}

func (p *exprParser) parseAdditive() (exprNode, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != "op" || (t.text != "+" && t.text != "-") {
			return l, nil
		}
		p.next()
		r, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		l = exprBin{t.text, l, r}
	}
}

func (p *exprParser) parseMultiplicative() (exprNode, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != "op" || (t.text != "*" && t.text != "/") {
			return l, nil
		}
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = exprBin{t.text, l, r}
	}
}

func (p *exprParser) parseUnary() (exprNode, error) {
	if t := p.peek(); t.kind == "op" && t.text == "-" {
		p.next()
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return exprNeg{operand}, nil
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (exprNode, error) {
	t := p.next()
	switch t.kind {
	case "num":
		r, ok := new(big.Rat).SetString(t.text)
		if !ok {
			return nil, fmt.Errorf("invalid number %q at position %d", t.text, t.pos)
		}
		return exprNum{r}, nil
	case "str":
		return exprStr{t.text}, nil
	case "path":
		if _, err := jsonpath.New(t.text); err != nil {
			return nil, fmt.Errorf("invalid jsonpath %q at position %d: %v", t.text, t.pos, err)
		}
		return exprPath{t.text}, nil
	case "ident":
		fn := strings.ToLower(t.text)
		spec, ok := exprFunctions[fn]
		if !ok {
			return nil, fmt.Errorf("unknown function %q at position %d (the closed set is coalesce, nullif, round, trunc)", t.text, t.pos)
		}
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var args []exprNode
		for {
			a, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			nt := p.next()
			if nt.kind == "op" && nt.text == "," {
				continue
			}
			if nt.kind == "op" && nt.text == ")" {
				break
			}
			return nil, fmt.Errorf("expected \",\" or \")\" at position %d, got %q", nt.pos, nt.text)
		}
		if len(args) < spec.minArgs || len(args) > spec.maxArgs {
			return nil, fmt.Errorf("%s takes %d–%d arguments, got %d", fn, spec.minArgs, spec.maxArgs, len(args))
		}
		if (fn == "round" || fn == "trunc") && len(args) == 2 {
			s, err := scaleLiteral(args[1], fn)
			if err != nil {
				return nil, err
			}
			// Normalize (e.g. a `-0` parses as neg(0)) so eval can rely on a
			// plain literal node.
			args[1] = exprNum{new(big.Rat).SetInt64(s)}
		}
		return exprCall{fn, args}, nil
	case "op":
		if t.text == "(" {
			inner, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return inner, nil
		}
	}
	return nil, fmt.Errorf("unexpected %q at position %d", t.text, t.pos)
}

// scaleLiteral enforces the spec's scale rule: round/trunc scale is a
// literal integer ≥ 0 (bounded to keep 10^s sane), never an expression. A
// negative literal parses as unary minus over a number, so unwrap it to
// give the range message rather than the shape message.
func scaleLiteral(n exprNode, fn string) (int64, error) {
	val := new(big.Rat)
	switch x := n.(type) {
	case exprNum:
		val.Set(x.val)
	case exprNeg:
		num, ok := x.operand.(exprNum)
		if !ok {
			return 0, fmt.Errorf("%s scale must be a literal integer (not an expression or path)", fn)
		}
		val.Neg(num.val)
	default:
		return 0, fmt.Errorf("%s scale must be a literal integer (not an expression or path)", fn)
	}
	if !val.IsInt() || val.Sign() < 0 || val.Num().Cmp(big.NewInt(12)) > 0 {
		return 0, fmt.Errorf("%s scale must be a literal integer between 0 and 12", fn)
	}
	return val.Num().Int64(), nil
}

// checkMaterializable enforces the finite-materialization rule: every `/`
// needs a dominating round/trunc (or comparison — booleans never carry a
// decimal) so no non-terminating value can reach a column. dominated
// propagates down whole subtrees: round(a/b + c) is fine because the
// rounding materializes everything beneath it.
func checkMaterializable(n exprNode, dominated bool) error {
	switch x := n.(type) {
	case exprNum, exprStr, exprPath:
		return nil
	case exprNeg:
		return checkMaterializable(x.operand, dominated)
	case exprBin:
		childDominated := dominated
		switch x.op {
		case "=", "<>", "<", "<=", ">", ">=":
			childDominated = true
		case "/":
			if !dominated {
				return fmt.Errorf("a division must be wrapped in round(...) or trunc(...) — an unrounded quotient (think 1/3) has no exact decimal form to store")
			}
		}
		if err := checkMaterializable(x.l, childDominated); err != nil {
			return err
		}
		return checkMaterializable(x.r, childDominated)
	case exprCall:
		childDominated := dominated || x.fn == "round" || x.fn == "trunc"
		for _, a := range x.args {
			if err := checkMaterializable(a, childDominated); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unhandled expression node %T", n)
}

// compileExpr lexes, parses, and admission-checks one expression. Every
// error it returns is a config error — expressions that compile can only
// fail at apply time on data (a non-numeric operand, the bit cap).
func compileExpr(src string) (exprNode, error) {
	toks, err := lexExpr(src)
	if err != nil {
		return nil, err
	}
	p := &exprParser{toks: toks}
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

// ── evaluation ──────────────────────────────────────────────────────────

// evalExpr evaluates a compiled expression against the decoded event
// payload. Results are nil (SQL NULL), *big.Rat (an exact decimal —
// formatRat renders it), bool (comparisons), or a passthrough scalar from
// coalesce/nullif (string, bool). Errors are data errors (a non-numeric
// operand where arithmetic needs one, the bit cap) — the caller dead-letters
// them as permanent.
func evalExpr(n exprNode, payload any) (any, error) {
	switch x := n.(type) {
	case exprNum:
		return x.val, nil
	case exprStr:
		return x.val, nil
	case exprPath:
		v, err := jsonpath.Get(x.path, payload)
		if err != nil {
			// A missing field is null (the spec's null semantics) — payload
			// shapes legitimately vary across a topic's event types.
			return nil, nil
		}
		return normalizeExprValue(v, x.path)
	case exprNeg:
		v, err := evalExpr(x.operand, payload)
		if err != nil || v == nil {
			return nil, err
		}
		r, err := ratOperand(v)
		if err != nil {
			return nil, err
		}
		return new(big.Rat).Neg(r), nil
	case exprBin:
		return evalBin(x, payload)
	case exprCall:
		return evalCall(x, payload)
	}
	return nil, fmt.Errorf("unhandled expression node %T", n)
}

func evalBin(x exprBin, payload any) (any, error) {
	l, err := evalExpr(x.l, payload)
	if err != nil {
		return nil, err
	}
	r, err := evalExpr(x.r, payload)
	if err != nil {
		return nil, err
	}
	if l == nil || r == nil {
		return nil, nil // null propagates through arithmetic AND comparison
	}
	switch x.op {
	case "=", "<>":
		eq, err := exprEqual(l, r)
		if err != nil {
			return nil, err
		}
		if x.op == "<>" {
			return !eq, nil
		}
		return eq, nil
	}
	lr, err := ratOperand(l)
	if err != nil {
		return nil, err
	}
	rr, err := ratOperand(r)
	if err != nil {
		return nil, err
	}
	switch x.op {
	case "+":
		return capBits(new(big.Rat).Add(lr, rr))
	case "-":
		return capBits(new(big.Rat).Sub(lr, rr))
	case "*":
		return capBits(new(big.Rat).Mul(lr, rr))
	case "/":
		if rr.Sign() == 0 {
			return nil, fmt.Errorf("division by zero (guard the divisor with nullif(x, 0))")
		}
		return capBits(new(big.Rat).Quo(lr, rr))
	case "<":
		return lr.Cmp(rr) < 0, nil
	case "<=":
		return lr.Cmp(rr) <= 0, nil
	case ">":
		return lr.Cmp(rr) > 0, nil
	case ">=":
		return lr.Cmp(rr) >= 0, nil
	}
	return nil, fmt.Errorf("unhandled operator %q", x.op)
}

func evalCall(x exprCall, payload any) (any, error) {
	switch x.fn {
	case "coalesce":
		for _, a := range x.args {
			v, err := evalExpr(a, payload)
			if err != nil {
				return nil, err
			}
			if v != nil {
				return v, nil
			}
		}
		return nil, nil
	case "nullif":
		a, err := evalExpr(x.args[0], payload)
		if err != nil {
			return nil, err
		}
		b, err := evalExpr(x.args[1], payload)
		if err != nil {
			return nil, err
		}
		// SQL semantics: nullif returns null only when the operands are
		// EQUAL; a null operand compares unknown, so a flows through.
		if a == nil || b == nil {
			return a, nil
		}
		eq, err := exprEqual(a, b)
		if err != nil {
			return nil, err
		}
		if eq {
			return nil, nil
		}
		return a, nil
	case "round", "trunc":
		v, err := evalExpr(x.args[0], payload)
		if err != nil || v == nil {
			return nil, err
		}
		r, err := ratOperand(v)
		if err != nil {
			return nil, err
		}
		scale := int64(0)
		if len(x.args) == 2 {
			scale = x.args[1].(exprNum).val.Num().Int64() // literal, checked at parse
		}
		return scaleRat(r, scale, x.fn == "round"), nil
	}
	return nil, fmt.Errorf("unhandled function %q", x.fn)
}

// scaleRat materializes r at the given decimal scale: half away from zero
// when rounding (PG round(numeric) / T-SQL ROUND), toward zero when
// truncating (PG trunc / T-SQL ROUND(x, s, 1)).
func scaleRat(r *big.Rat, scale int64, round bool) *big.Rat {
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(pow))
	q, rem := new(big.Int).QuoRem(scaled.Num(), scaled.Denom(), new(big.Int))
	if round && rem.Sign() != 0 {
		twice := new(big.Int).Abs(rem)
		twice.Lsh(twice, 1)
		if twice.Cmp(scaled.Denom()) >= 0 {
			if rem.Sign() > 0 {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
	}
	return new(big.Rat).SetFrac(q, pow)
}

// normalizeExprValue turns a payload leaf into an expression value: numbers
// become exact rationals from their source digits, strings and bools pass
// through (for coalesce/nullif), null stays null. Non-scalar leaves are data
// errors — expressions compute over scalars.
func normalizeExprValue(v any, path string) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case json.Number:
		r, ok := new(big.Rat).SetString(string(t))
		if !ok {
			return nil, fmt.Errorf("path [%s]: number %q does not parse", path, string(t))
		}
		return r, nil
	case string, bool:
		return t, nil
	case float64:
		// Payloads decode with UseNumber, so a float64 here means a caller
		// handed in non-UseNumber data — refuse rather than silently lose
		// precision.
		return nil, fmt.Errorf("path [%s]: payload decoded without UseNumber (float64 leaf)", path)
	default:
		return nil, fmt.Errorf("path [%s]: expressions compute over scalars, got %T", path, v)
	}
}

func ratOperand(v any) (*big.Rat, error) {
	switch t := v.(type) {
	case *big.Rat:
		return t, nil
	case string:
		return nil, fmt.Errorf("arithmetic needs a number, got the string %q", t)
	case bool:
		return nil, fmt.Errorf("arithmetic needs a number, got a boolean")
	default:
		return nil, fmt.Errorf("arithmetic needs a number, got %T", v)
	}
}

func exprEqual(a, b any) (bool, error) {
	ar, aNum := a.(*big.Rat)
	br, bNum := b.(*big.Rat)
	if aNum && bNum {
		return ar.Cmp(br) == 0, nil
	}
	if aNum != bNum {
		return false, nil // mixed types are simply not equal
	}
	return a == b, nil // strings/bools compare directly
}

func capBits(r *big.Rat) (*big.Rat, error) {
	if r.Num().BitLen()+r.Denom().BitLen() > exprMaxBits {
		return nil, fmt.Errorf("expression value exceeded %d bits (pathological input; check the source data)", exprMaxBits)
	}
	return r, nil
}

// formatRat renders an exact rational in minimal decimal form: no exponent,
// no trailing zeros, integral values without a point. The denominator must
// be of the form 2^a·5^b (a terminating decimal) — guaranteed for every
// materialized value by the admission-time domination rule; anything else
// is an internal invariant violation, reported loudly.
func formatRat(r *big.Rat) (string, error) {
	den := new(big.Int).Set(r.Denom())
	two, five, ten := big.NewInt(2), big.NewInt(5), big.NewInt(10)
	var a, b int64
	m := new(big.Int)
	for {
		q, rem := new(big.Int).QuoRem(den, two, m)
		if rem.Sign() != 0 {
			break
		}
		den, a = q, a+1
	}
	for {
		q, rem := new(big.Int).QuoRem(den, five, m)
		if rem.Sign() != 0 {
			break
		}
		den, b = q, b+1
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		return "", fmt.Errorf("internal: non-terminating decimal reached materialization (denominator retains factor %s)", den)
	}
	scale := a
	if b > scale {
		scale = b
	}
	pow := new(big.Int).Exp(ten, big.NewInt(scale), nil)
	digits := new(big.Int).Mul(r.Num(), pow)
	digits.Quo(digits, r.Denom())
	neg := digits.Sign() < 0
	s := new(big.Int).Abs(digits).String()
	if scale > 0 {
		for int64(len(s)) <= scale {
			s = "0" + s
		}
		intPart, fracPart := s[:int64(len(s))-scale], s[int64(len(s))-scale:]
		fracPart = strings.TrimRight(fracPart, "0")
		if fracPart == "" {
			s = intPart
		} else {
			s = intPart + "." + fracPart
		}
	}
	if neg {
		s = "-" + s
	}
	return s, nil
}
