package stages

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"

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
// and nullif the only null-aware functions. The boolean connectives are the
// deliberate EXCEPTION to blanket propagation: or/and/not follow SQL's
// three-valued logic (null is UNKNOWN, so null or true = true and null and
// false = false), `x in (...)` is its expansion into equalities, and
// `x is [not] null` is the one predicate that never returns null. `case`
// is SQL's conditional: the first WHEN that is TRUE selects its THEN (null
// and false both fall through), no match and no ELSE is null, and only the
// selected branch evaluates — so a data error in an unselected branch never
// surfaces (the guard idiom `case when $.n = 0 then null else … end`).
// The string functions follow PostgreSQL over code points: `||` propagates
// null and `concat` skips it (both render a number in canonical digits and
// a boolean as true/false); `lower`/`upper` use Unicode SIMPLE case mapping
// (locale-free, so `ß` stays `ß` where PG's `upper` gives `ẞ`; a Go
// toolchain's Unicode-table bump can, rarely, change a mapping — Unicode 11
// gave Georgian new capitals); `trim`/`ltrim`/`rtrim` strip spaces (or the
// characters of a second argument); `like` is case-sensitive and anchored,
// with % (any run), _ (one character), and \ as the escape.
// Keywords match case-insensitively, like the function names. Because only division can
// produce a value with no finite decimal representation, every `/` must be
// dominated by a round/trunc (or comparison — booleans never materialize a
// decimal) ancestor in its tree, checked at admission: a bare quotient that
// could reach a column is a config error, never a runtime surprise.
//
// Grammar (recursive descent, no precedence surprises):
//
//	expr       := orExpr
//	orExpr     := andExpr ('or' andExpr)*
//	andExpr    := notExpr ('and' notExpr)*
//	notExpr    := 'not' notExpr | comparison
//	comparison := concat (compareOp concat
//	              | 'not'? 'in' '(' concat (',' concat)* ')'
//	              | 'not'? 'like' concat
//	              | 'not'? 'between' concat 'and' concat
//	              | 'is' 'not'? 'null')?
//	concat     := additive ('||' additive)*
//	additive   := multiplicative (('+'|'-') multiplicative)*
//	multiplicative := unary (('*'|'/') unary)*
//	unary      := '-' unary | postfix
//	postfix    := primary ('::' TYPE)*
//	primary    := NUMBER | STRING | TRUE | FALSE | NULL | PATH | caseExpr
//	              | ('cast' | 'try_cast') '(' expr 'as' TYPE ')'
//	              | IDENT '(' expr (',' expr)* ')' | '(' expr ')'
//	caseExpr   := 'case' expr? ('when' expr 'then' expr)+ ('else' expr)? 'end'
//	compareOp  := '=' | '<>' | '<' | '<=' | '>' | '>='
//
// NUMBER is a plain decimal literal (no exponent form); STRING is a
// SQL-style single-quoted literal with '' as the escaped quote (needed by
// real formulas like nullif($.GroupName, '')); PATH is a `$…` jsonpath
// into the event payload; IDENT names one of the closed function set
// (exprFunctionList, matched case-insensitively). `||` binds looser than
// `+`/`-` and tighter than comparison, as in PostgreSQL. TYPE is a cast
// target (castTypes); casts and the math functions live in lang_cast.go.
// `x between a and b` is `a <= x and x <= b`. A `case`
// with an operand is the simple form (`case x when a then …`): each WHEN
// is `x = a`, so a null operand matches no branch.

// exprMaxBits caps the combined numerator+denominator size of any
// intermediate value — unreachable for admissible formulas (the domination
// rule bounds unrounded division depth); pure pathology insurance.
const exprMaxBits = 4096

type Node interface{ node() }

type exprNum struct{ val *big.Rat }

type exprStr struct{ val string }

type exprPath struct{ path string }

type exprNeg struct{ operand Node }

type exprBin struct {
	op   string
	l, r Node
}

type exprCall struct {
	fn   string // canonical lower-case name from the closed set
	args []Node
}

type exprNot struct{ operand Node }

type exprIn struct {
	operand Node
	list    []Node
}

type exprIsNull struct {
	operand Node
	negate  bool
}

type exprBool struct{ val bool }

type exprNull struct{}

// exprLike is `operand like pattern` (`not like` wraps it in exprNot).
type exprLike struct{ operand, pattern Node }

// exprCase is both CASE forms: operand is nil for the searched form
// (whens are predicates) and set for the simple form (whens are values
// compared to the operand).
type exprCase struct {
	operand Node
	whens   []Node
	thens   []Node
	els     Node // nil: no ELSE (an unmatched case is null)
}

func (exprNum) node()    {}
func (exprStr) node()    {}
func (exprPath) node()   {}
func (exprNeg) node()    {}
func (exprBin) node()    {}
func (exprCall) node()   {}
func (exprNot) node()    {}
func (exprIn) node()     {}
func (exprIsNull) node() {}
func (exprBool) node()   {}
func (exprNull) node()   {}
func (exprCase) node()   {}
func (exprLike) node()   {}

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
				if strings.ContainsRune(" \t\n\r+-*/(),=<>|:", rune(src[i])) {
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
		case c == ':':
			if i+1 >= len(src) || src[i+1] != ':' {
				return nil, fmt.Errorf("unexpected character %q at position %d (a cast is ::type)", ":", i)
			}
			toks = append(toks, exprToken{"op", "::", i})
			i += 2
		case c == '|':
			if i+1 >= len(src) || src[i+1] != '|' {
				return nil, fmt.Errorf("unexpected character %q at position %d (concatenation is ||)", "|", i)
			}
			toks = append(toks, exprToken{"op", "||", i})
			i += 2
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
	// aggs collects aggregate calls as fold slots; nil means aggregates
	// are not allowed here (everywhere but an aggregate stage's emit expr
	// and having). inAgg rejects a nested aggregate.
	aggs  *[]exprAgg
	inAgg bool
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
	"coalesce":     {2, 16},
	"nullif":       {2, 2},
	"round":        {2, 2},
	"trunc":        {1, 2},
	"concat":       {1, 16},
	"lower":        {1, 1},
	"upper":        {1, 1},
	"trim":         {1, 2},
	"ltrim":        {1, 2},
	"rtrim":        {1, 2},
	"substring":    {2, 3},
	"length":       {1, 1},
	"char_length":  {1, 1},
	"replace":      {3, 3},
	"starts_with":  {2, 2},
	"abs":          {1, 1},
	"ceil":         {1, 1},
	"ceiling":      {1, 1},
	"floor":        {1, 1},
	"mod":          {2, 2},
	"greatest":     {1, 16},
	"least":        {1, 16},
	"date_trunc":   {2, 2},
	"date_part":    {2, 2},
	"to_timestamp": {1, 1},
	"make_date":    {3, 3},
}

// exprFunctionList names the closed set for error messages.
const exprFunctionList = "coalesce, nullif, round, trunc, concat, lower, upper, trim, ltrim, rtrim, substring, length, char_length, replace, starts_with, abs, ceil, ceiling, floor, mod, greatest, least, date_trunc, date_part, to_timestamp, make_date; extract(field from x)"

// atKeyword reports whether the next token is the given SQL keyword
// (identifiers double as keywords, matched case-insensitively; a `$`
// path can never collide since paths are their own token kind).
func (p *exprParser) atKeyword(kw string) bool {
	t := p.peek()
	return t.kind == "ident" && strings.EqualFold(t.text, kw)
}

func (p *exprParser) parseExpr() (Node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.atKeyword("or") {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = exprBin{"or", l, r}
	}
	return l, nil
}

func (p *exprParser) parseAnd() (Node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.atKeyword("and") {
		p.next()
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = exprBin{"and", l, r}
	}
	return l, nil
}

func (p *exprParser) parseNot() (Node, error) {
	if p.atKeyword("not") {
		p.next()
		operand, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return exprNot{operand}, nil
	}
	return p.parseComparison()
}

func (p *exprParser) parseComparison() (Node, error) {
	l, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == "op" && (t.text == "=" || t.text == "<>" || t.text == "<" || t.text == "<=" || t.text == ">" || t.text == ">=") {
		p.next()
		r, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return exprBin{t.text, l, r}, nil
	}
	// `x not in (…)` / `x not like p` are SQL's spellings of not(x in …) /
	// not(x like p) — three-valued, so a null stays null.
	negate := false
	if p.atKeyword("not") {
		if nt := p.toks[p.i+1]; nt.kind == "ident" && (strings.EqualFold(nt.text, "in") || strings.EqualFold(nt.text, "like") || strings.EqualFold(nt.text, "between")) {
			p.next()
			negate = true
		}
	}
	if p.atKeyword("between") {
		p.next()
		lo, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		if !p.atKeyword("and") {
			t := p.peek()
			return nil, fmt.Errorf("expected and in between at position %d, got %q", t.pos, t.text)
		}
		p.next()
		hi, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		// SQL's definition, three-valued logic included.
		return negateIf(exprBin{"and", exprBin{"<=", lo, l}, exprBin{"<=", l, hi}}, negate), nil
	}
	if p.atKeyword("like") {
		p.next()
		pat, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return negateIf(exprLike{l, pat}, negate), nil
	}
	if p.atKeyword("in") {
		p.next()
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var list []Node
		for {
			m, err := p.parseConcat()
			if err != nil {
				return nil, err
			}
			list = append(list, m)
			nt := p.next()
			if nt.kind == "op" && nt.text == "," {
				continue
			}
			if nt.kind == "op" && nt.text == ")" {
				break
			}
			return nil, fmt.Errorf("expected \",\" or \")\" at position %d, got %q", nt.pos, nt.text)
		}
		return negateIf(exprIn{l, list}, negate), nil
	}
	if p.atKeyword("is") {
		p.next()
		negate := false
		if p.atKeyword("not") {
			p.next()
			negate = true
		}
		if !p.atKeyword("null") {
			t := p.peek()
			return nil, fmt.Errorf("expected null after is at position %d, got %q", t.pos, t.text)
		}
		p.next()
		return exprIsNull{l, negate}, nil
	}
	return l, nil
}

func negateIf(n Node, negate bool) Node {
	if negate {
		return exprNot{n}
	}
	return n
}

func (p *exprParser) parseConcat() (Node, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != "op" || t.text != "||" {
			return l, nil
		}
		p.next()
		r, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		l = exprBin{"||", l, r}
	}
}

func (p *exprParser) parseAdditive() (Node, error) {
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

func (p *exprParser) parseMultiplicative() (Node, error) {
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

func (p *exprParser) parseUnary() (Node, error) {
	if t := p.peek(); t.kind == "op" && t.text == "-" {
		p.next()
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return exprNeg{operand}, nil
	}
	return p.parsePostfix()
}

func (p *exprParser) parsePrimary() (Node, error) {
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
		// A `$parent.` prefix is the forEach element scope's way of reaching
		// the enclosing event payload; validate the remainder as a rooted
		// path (the apply layer resolves the scope).
		checkPath := t.text
		if rest, ok := strings.CutPrefix(checkPath, "$parent"); ok {
			checkPath = "$" + rest
		}
		if _, err := jsonpath.New(checkPath); err != nil {
			return nil, fmt.Errorf("invalid jsonpath %q at position %d: %v", t.text, t.pos, err)
		}
		if MultiValuedPath(checkPath) {
			return nil, fmt.Errorf("jsonpath %q at position %d is multi-valued (wildcard/recursive/filter) — expressions compute over scalars", t.text, t.pos)
		}
		return exprPath{t.text}, nil
	case "ident":
		fn := strings.ToLower(t.text)
		// SQL's boolean literals, case-insensitive like every keyword.
		if fn == "true" || fn == "false" {
			return exprBool{fn == "true"}, nil
		}
		if fn == "null" {
			return exprNull{}, nil
		}
		if fn == "case" {
			return p.parseCase()
		}
		if fn == "cast" || fn == "try_cast" {
			return p.parseCastCall(fn == "try_cast")
		}
		if fn == "extract" {
			return p.parseExtract()
		}
		if aggregateFns[fn] {
			if nt := p.peek(); nt.kind == "op" && nt.text == "(" {
				return p.parseAggCall(fn, t.pos)
			}
		}
		if typ, ok := typedLiteralTypes[fn]; ok && p.peek().kind == "str" {
			// PG's typed literal: date '2024-01-15', interval '30 days'.
			return exprCast{exprStr{p.next().text}, typ, false}, nil
		}
		spec, ok := exprFunctions[fn]
		if !ok {
			return nil, fmt.Errorf("unknown function %q at position %d (the closed set is %s; case … end is the conditional)", t.text, t.pos, exprFunctionList)
		}
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var args []Node
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
		if fn == "date_trunc" || fn == "date_part" {
			allowed := func(string) bool { return true }
			if fn == "date_trunc" {
				allowed = func(u string) bool { return truncUnits[u] }
			}
			u, err := timeUnitLiteral(args[0], fn, allowed)
			if err != nil {
				return nil, err
			}
			args[0] = exprStr{u}
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

// parseCase parses the rest of a CASE after its `case` keyword: an
// optional operand (the simple form), one or more WHEN … THEN arms, an
// optional ELSE, and END.
func (p *exprParser) parseCase() (Node, error) {
	var c exprCase
	if p.atKeyword("end") || p.atKeyword("else") || p.atKeyword("then") {
		t := p.peek()
		return nil, fmt.Errorf("case needs at least one when … then at position %d, got %q", t.pos, t.text)
	}
	if !p.atKeyword("when") {
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.operand = operand
	}
	for p.atKeyword("when") {
		p.next()
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if !p.atKeyword("then") {
			t := p.peek()
			return nil, fmt.Errorf("expected then at position %d, got %q", t.pos, t.text)
		}
		p.next()
		th, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.whens = append(c.whens, w)
		c.thens = append(c.thens, th)
	}
	if len(c.whens) == 0 {
		t := p.peek()
		return nil, fmt.Errorf("case needs at least one when … then at position %d, got %q", t.pos, t.text)
	}
	if p.atKeyword("else") {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.els = e
	}
	if !p.atKeyword("end") {
		t := p.peek()
		return nil, fmt.Errorf("expected when, else, or end in case at position %d, got %q", t.pos, t.text)
	}
	p.next()
	if err := checkCaseResultFamilies(c); err != nil {
		return nil, err
	}
	return c, nil
}

// checkCaseResultFamilies rejects a CASE whose result branches are LITERALS
// of different families (`then 1 else 'x'`) — a certain bug, caught at
// admission. Non-literal branches are not typed (payload paths are
// dynamic), so a runtime mix is allowed and the column's coercion decides.
func checkCaseResultFamilies(c exprCase) error {
	branches := append([]Node{}, c.thens...)
	if c.els != nil {
		branches = append(branches, c.els)
	}
	first := ""
	for _, b := range branches {
		fam := literalFamily(b)
		if fam == "" {
			continue
		}
		if first == "" {
			first = fam
			continue
		}
		if fam != first {
			return fmt.Errorf("case result branches mix literal types (%s and %s) — every branch should produce the same kind of value", first, fam)
		}
	}
	return nil
}

// literalFamily names a literal node's value family ("" for non-literals
// and for null, which belongs to every family).
func literalFamily(n Node) string {
	switch x := n.(type) {
	case exprNum:
		return "number"
	case exprNeg:
		if _, ok := x.operand.(exprNum); ok {
			return "number"
		}
	case exprStr:
		return "string"
	case exprBool:
		return "boolean"
	}
	return ""
}

// scaleLiteral enforces the spec's scale rule: round/trunc scale is a
// literal integer ≥ 0 (bounded to keep 10^s sane), never an expression. A
// negative literal parses as unary minus over a number, so unwrap it to
// give the range message rather than the shape message.
func scaleLiteral(n Node, fn string) (int64, error) {
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
func checkMaterializable(n Node, dominated bool) error {
	switch x := n.(type) {
	case exprNum, exprStr, exprPath, exprBool, exprNull:
		return nil
	case exprNeg:
		return checkMaterializable(x.operand, dominated)
	case exprBin:
		childDominated := dominated
		switch x.op {
		case "=", "<>", "<", "<=", ">", ">=", "or", "and":
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
		// round/trunc/ceil/floor materialize what lies beneath them.
		childDominated := dominated
		switch x.fn {
		case "round", "trunc", "ceil", "ceiling", "floor", "to_timestamp":
			childDominated = true
		}
		for _, a := range x.args {
			if err := checkMaterializable(a, childDominated); err != nil {
				return err
			}
		}
		return nil
	case exprNot:
		return checkMaterializable(x.operand, true)
	case exprIn:
		if err := checkMaterializable(x.operand, true); err != nil {
			return err
		}
		for _, m := range x.list {
			if err := checkMaterializable(m, true); err != nil {
				return err
			}
		}
		return nil
	case exprIsNull:
		return checkMaterializable(x.operand, true)
	case exprAgg:
		// avg is an exact quotient (like /); count's argument is only
		// tested for null; sum/min/max store values derived from theirs.
		if x.fn == "avg" && !dominated {
			return fmt.Errorf("avg(...) is an exact quotient with no finite decimal form to store — wrap it in round(..., scale) or trunc(...)")
		}
		if x.arg != nil {
			if err := checkMaterializable(x.arg, dominated || x.fn == "count"); err != nil {
				return err
			}
		}
		if x.filter != nil {
			return checkMaterializable(x.filter, true)
		}
		return nil
	case exprCast:
		// An integer cast rounds, and a boolean cast never stores a decimal;
		// numeric and text casts carry the value through as it is.
		return checkMaterializable(x.operand, dominated || x.typ == "integer" || x.typ == "bigint" || x.typ == "boolean")
	case exprLike:
		if err := checkMaterializable(x.operand, true); err != nil {
			return err
		}
		return checkMaterializable(x.pattern, true)
	case exprCase:
		// The operand and the WHENs are compared/tested, never stored; the
		// results inherit the enclosing context, so round(case … end, 2)
		// dominates every branch.
		if x.operand != nil {
			if err := checkMaterializable(x.operand, true); err != nil {
				return err
			}
		}
		for i := range x.whens {
			if err := checkMaterializable(x.whens[i], true); err != nil {
				return err
			}
			if err := checkMaterializable(x.thens[i], dominated); err != nil {
				return err
			}
		}
		if x.els != nil {
			return checkMaterializable(x.els, dominated)
		}
		return nil
	}
	return fmt.Errorf("unhandled expression node %T", n)
}

// Compile lexes, parses, and admission-checks one expression. Every
// error it returns is a config error — expressions that compile can only
// fail at apply time on data (a non-numeric operand, the bit cap).
func Compile(src string) (Node, error) {
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

// Eval evaluates a compiled expression against the decoded event
// payload. Results are nil (SQL NULL), *big.Rat (an exact decimal —
// FormatRat renders it), bool (comparisons), or a passthrough scalar from
// coalesce/nullif (string, bool). Errors are data errors (a non-numeric
// operand where arithmetic needs one, the bit cap) — the caller dead-letters
// them as permanent.
func Eval(n Node, payload, parent any) (any, error) {
	switch x := n.(type) {
	case exprNum:
		return x.val, nil
	case exprStr:
		return x.val, nil
	case exprBool:
		return x.val, nil
	case exprNull:
		return nil, nil
	case exprPath:
		v, err := ResolvePath(x.path, payload, parent)
		if err != nil {
			// A missing field is null (the spec's null semantics) — payload
			// shapes legitimately vary across a topic's event types.
			return nil, nil
		}
		return normalizeExprValue(v, x.path)
	case exprNeg:
		v, err := Eval(x.operand, payload, parent)
		if err != nil || v == nil {
			return nil, err
		}
		if iv, ok := v.(intervalVal); ok {
			return negInterval(iv), nil
		}
		r, err := ratOperand(v)
		if err != nil {
			return nil, err
		}
		return new(big.Rat).Neg(r), nil
	case exprBin:
		return evalBin(x, payload, parent)
	case exprCall:
		return evalCall(x, payload, parent)
	case exprNot:
		v, err := Eval(x.operand, payload, parent)
		if err != nil || v == nil {
			return nil, err // not null = null (three-valued)
		}
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("not needs a boolean operand (a comparison/predicate), got %T", v)
		}
		return !b, nil
	case exprIn:
		return evalIn(x, payload, parent)
	case exprCase:
		return evalCase(x, payload, parent)
	case exprLike:
		return evalLike(x, payload, parent)
	case exprCast:
		return evalCast(x, payload, parent)
	case exprAgg:
		res, ok := payload.(aggResults)
		if !ok {
			return nil, fmt.Errorf("internal: aggregate %s evaluated outside a fold", x.fn)
		}
		return res[x.slot], nil
	case exprIsNull:
		// A PATH operand tests presence directly, skipping scalar
		// normalization: `$.cust is not null` legitimately asks about a
		// looked-up row or a merge side — SQL's row-valued IS NULL — and
		// an object in a value position stays an error everywhere else.
		if p, ok := x.operand.(exprPath); ok {
			v, err := ResolvePath(p.path, payload, parent)
			if err != nil {
				v = nil // missing = null, per the language
			}
			return (v == nil) != x.negate, nil
		}
		v, err := Eval(x.operand, payload, parent)
		if err != nil {
			return nil, err
		}
		return (v == nil) != x.negate, nil
	}
	return nil, fmt.Errorf("unhandled expression node %T", n)
}

func evalBin(x exprBin, payload, parent any) (any, error) {
	if x.op == "or" || x.op == "and" {
		return evalConnective(x, payload, parent)
	}
	l, err := Eval(x.l, payload, parent)
	if err != nil {
		return nil, err
	}
	r, err := Eval(x.r, payload, parent)
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
	case "<", "<=", ">", ">=":
		return orderedCompare(x.op, l, r)
	case "||":
		ls, err := renderText(l, "||")
		if err != nil {
			return nil, err
		}
		rs, err := renderText(r, "||")
		if err != nil {
			return nil, err
		}
		return ls + rs, nil
	}
	if v, ok, err := evalTimeArith(x.op, l, r); ok {
		return v, err
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
	}
	return nil, fmt.Errorf("unhandled operator %q", x.op)
}

func evalCall(x exprCall, payload, parent any) (any, error) {
	switch x.fn {
	case "coalesce":
		for _, a := range x.args {
			v, err := Eval(a, payload, parent)
			if err != nil {
				return nil, err
			}
			if v != nil {
				return v, nil
			}
		}
		return nil, nil
	case "nullif":
		a, err := Eval(x.args[0], payload, parent)
		if err != nil {
			return nil, err
		}
		b, err := Eval(x.args[1], payload, parent)
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
		v, err := Eval(x.args[0], payload, parent)
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
	case "concat":
		// PG's concat: null arguments are skipped, never propagated.
		var sb strings.Builder
		for _, a := range x.args {
			v, err := Eval(a, payload, parent)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue
			}
			s, err := renderText(v, "concat")
			if err != nil {
				return nil, err
			}
			sb.WriteString(s)
		}
		return sb.String(), nil
	case "abs", "ceil", "ceiling", "floor", "mod", "greatest", "least":
		return evalMathCall(x, payload, parent)
	case "date_trunc", "date_part", "to_timestamp", "make_date":
		return evalTimeCall(x, payload, parent)
	case "lower", "upper", "trim", "ltrim", "rtrim", "substring", "length", "char_length", "replace", "starts_with":
		// Strict, as in PG: any null argument makes the result null. Every
		// argument evaluates first, so a data error surfaces regardless of
		// which argument is null.
		args := make([]any, len(x.args))
		for i, a := range x.args {
			v, err := Eval(a, payload, parent)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
		for _, v := range args {
			if v == nil {
				return nil, nil
			}
		}
		return evalStringFn(x.fn, args)
	}
	return nil, fmt.Errorf("unhandled function %q", x.fn)
}

// orderedCompare is SQL's < <= > >= within one family: numbers exactly,
// strings byte-wise (code-point order — PostgreSQL's COLLATE "C", never a
// locale collation, which library upgrades can reorder), booleans
// false < true. It orders by collectLess, the order min/max and collect
// fold by, so an expression and a fold can never disagree. Mixed families
// are a data error, as in PG (there is no text < numeric).
func orderedCompare(op string, l, r any) (any, error) {
	l, r, err := coerceTimePair(l, r)
	if err != nil {
		return nil, err
	}
	lf, rf := orderFamily(l), orderFamily(r)
	if lf == "" || rf == "" || lf != rf {
		return nil, fmt.Errorf("%s compares two values of the same kind, got %s and %s", op, describeValue(l), describeValue(r))
	}
	less, greater := collectLess(l, r), collectLess(r, l)
	switch op {
	case "<":
		return less, nil
	case "<=":
		return !greater, nil
	case ">":
		return greater, nil
	}
	return !less, nil
}

func orderFamily(v any) string {
	switch v.(type) {
	case *big.Rat:
		return "number"
	case string:
		return "text"
	case bool:
		return "boolean"
	case timestampVal, dateVal:
		return "time" // a date, timestamp, and timestamptz compare on one timeline
	case intervalVal:
		return "interval"
	}
	return ""
}

func evalStringFn(fn string, args []any) (any, error) {
	s, err := stringArg(args[0], fn)
	if err != nil {
		return nil, err
	}
	switch fn {
	case "lower":
		return strings.ToLower(s), nil
	case "upper":
		return strings.ToUpper(s), nil
	case "length", "char_length":
		return new(big.Rat).SetInt64(int64(utf8.RuneCountInString(s))), nil
	case "trim", "ltrim", "rtrim":
		cut := " " // PG trims spaces only, never tabs or newlines
		if len(args) == 2 {
			if cut, err = stringArg(args[1], fn); err != nil {
				return nil, err
			}
		}
		switch fn {
		case "ltrim":
			return strings.TrimLeft(s, cut), nil
		case "rtrim":
			return strings.TrimRight(s, cut), nil
		}
		return strings.Trim(s, cut), nil
	case "replace":
		from, err := stringArg(args[1], fn)
		if err != nil {
			return nil, err
		}
		to, err := stringArg(args[2], fn)
		if err != nil {
			return nil, err
		}
		if from == "" {
			return s, nil
		}
		return strings.ReplaceAll(s, from, to), nil
	case "starts_with":
		prefix, err := stringArg(args[1], fn)
		if err != nil {
			return nil, err
		}
		return strings.HasPrefix(s, prefix), nil
	case "substring":
		return substringRunes(s, args[1:])
	}
	return nil, fmt.Errorf("unhandled function %q", fn)
}

// substringRunes is PG's substring(s, start[, count]) over code points:
// positions are 1-based, the window [start, start+count) is clipped to
// the string (so a start below 1 eats into the count), and a negative
// count is an error.
func substringRunes(s string, args []any) (any, error) {
	start, err := intArg(args[0], "substring")
	if err != nil {
		return nil, err
	}
	runes := []rune(s)
	n := int64(len(runes))
	end := n + 1
	if len(args) == 2 {
		count, err := intArg(args[1], "substring")
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, fmt.Errorf("substring length must not be negative, got %d", count)
		}
		end = start + count
	}
	from, to := max(start, 1), min(end, n+1)
	if to <= from {
		return "", nil
	}
	return string(runes[from-1 : to-1]), nil
}

// evalLike is SQL's LIKE: null on either side is null; both sides must be
// strings.
func evalLike(x exprLike, payload, parent any) (any, error) {
	v, err := Eval(x.operand, payload, parent)
	if err != nil {
		return nil, err
	}
	pv, err := Eval(x.pattern, payload, parent)
	if err != nil {
		return nil, err
	}
	if v == nil || pv == nil {
		return nil, nil
	}
	s, err := stringArg(v, "like")
	if err != nil {
		return nil, err
	}
	pat, err := stringArg(pv, "like")
	if err != nil {
		return nil, err
	}
	return likeMatch(s, pat)
}

// likeMatch is PG's LIKE over code points: % matches any run (including
// none), _ exactly one character, \ makes the next pattern character
// literal; case-sensitive and anchored at both ends. The match is the
// greedy two-pointer algorithm with single-% backtracking — O(len(s) ·
// len(pattern)) worst case, never exponential.
func likeMatch(s, pattern string) (bool, error) {
	type elem struct {
		kind byte // 'l' literal, '_' one character, '%' any run
		r    rune
	}
	var pat []elem
	pr := []rune(pattern)
	for i := 0; i < len(pr); i++ {
		switch pr[i] {
		case '\\':
			if i+1 == len(pr) {
				return false, fmt.Errorf("like pattern must not end with the escape character \\")
			}
			i++
			pat = append(pat, elem{'l', pr[i]})
		case '%', '_':
			pat = append(pat, elem{byte(pr[i]), 0})
		default:
			pat = append(pat, elem{'l', pr[i]})
		}
	}
	str := []rune(s)
	si, pi := 0, 0
	starP, starS := -1, 0
	for si < len(str) {
		switch {
		case pi < len(pat) && (pat[pi].kind == '_' || pat[pi].kind == 'l' && pat[pi].r == str[si]):
			si++
			pi++
		case pi < len(pat) && pat[pi].kind == '%':
			starP, starS = pi, si
			pi++
		case starP >= 0:
			starS++
			si, pi = starS, starP+1
		default:
			return false, nil
		}
	}
	for pi < len(pat) && pat[pi].kind == '%' {
		pi++
	}
	return pi == len(pat), nil
}

// renderText is the implicit text rendering `||` and concat apply: strings
// pass through, numbers render in canonical digits (FormatRat — 1.50 is
// "1.5"), booleans as true/false.
func renderText(v any, fn string) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case *big.Rat:
		return FormatRat(t)
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	case timestampVal, dateVal, intervalVal:
		m, err := Materialize(t)
		if err != nil {
			return "", err
		}
		return m.(string), nil
	}
	return "", fmt.Errorf("%s needs text, a number, or a boolean, got %s", fn, describeValue(v))
}

func stringArg(v any, fn string) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("%s needs a string, got %s", fn, describeValue(v))
}

// intArg reads an integral number within PG's integer range — the
// position/length arguments of substring.
func intArg(v any, fn string) (int64, error) {
	r, ok := v.(*big.Rat)
	if !ok || !r.IsInt() {
		return 0, fmt.Errorf("%s needs an integer position/length, got %s", fn, describeValue(v))
	}
	if !r.Num().IsInt64() || r.Num().Int64() < -1<<31 || r.Num().Int64() > 1<<31-1 {
		return 0, fmt.Errorf("%s position/length %s is out of the integer range", fn, r.Num())
	}
	return r.Num().Int64(), nil
}

func describeValue(v any) string {
	switch t := v.(type) {
	case string:
		return fmt.Sprintf("the string %q", t)
	case *big.Rat:
		if s, err := FormatRat(t); err == nil {
			return "the number " + s
		}
		return "a number"
	case bool:
		return "a boolean"
	case timestampVal, dateVal, intervalVal:
		m, _ := Materialize(t)
		return fmt.Sprintf("the %s %s", describeTimeType(t), m)
	}
	return fmt.Sprintf("%T", v)
}

// evalConnective applies SQL's three-valued logic — null is UNKNOWN, so
// (null or true) is true and (null and false) is false; the blanket
// null-propagation rule deliberately does NOT apply here. Both sides
// always evaluate (no short-circuit), so a data error surfaces
// regardless of the other side's value — error behavior never depends
// on operand order.
func evalConnective(x exprBin, payload, parent any) (any, error) {
	l, err := Eval(x.l, payload, parent)
	if err != nil {
		return nil, err
	}
	r, err := Eval(x.r, payload, parent)
	if err != nil {
		return nil, err
	}
	lb, lNull, err := boolOperand(l, x.op)
	if err != nil {
		return nil, err
	}
	rb, rNull, err := boolOperand(r, x.op)
	if err != nil {
		return nil, err
	}
	if x.op == "or" {
		switch {
		case (!lNull && lb) || (!rNull && rb):
			return true, nil
		case lNull || rNull:
			return nil, nil
		default:
			return false, nil
		}
	}
	switch {
	case (!lNull && !lb) || (!rNull && !rb):
		return false, nil
	case lNull || rNull:
		return nil, nil
	default:
		return true, nil
	}
}

func boolOperand(v any, op string) (val, isNull bool, err error) {
	switch t := v.(type) {
	case nil:
		return false, true, nil
	case bool:
		return t, false, nil
	default:
		return false, false, fmt.Errorf("%s needs boolean operands (comparisons/predicates), got %T", op, v)
	}
}

// evalCase selects the first branch whose WHEN is TRUE (searched form) or
// equals the operand (simple form); null and false fall through, as in
// SQL. Evaluation is lazy: WHENs after the match and every unselected
// result never run, so their data errors cannot surface — unlike the
// connectives, whose operands always evaluate.
func evalCase(x exprCase, payload, parent any) (any, error) {
	var operand any
	if x.operand != nil {
		v, err := Eval(x.operand, payload, parent)
		if err != nil {
			return nil, err
		}
		operand = v
	}
	for i, w := range x.whens {
		wv, err := Eval(w, payload, parent)
		if err != nil {
			return nil, err
		}
		matched := false
		if x.operand != nil {
			// x = a: a null on either side is UNKNOWN, never a match.
			if operand != nil && wv != nil {
				if matched, err = exprEqual(operand, wv); err != nil {
					return nil, err
				}
			}
		} else {
			b, isNull, err := boolOperand(wv, "case when")
			if err != nil {
				return nil, err
			}
			matched = !isNull && b
		}
		if matched {
			return Eval(x.thens[i], payload, parent)
		}
	}
	if x.els != nil {
		return Eval(x.els, payload, parent)
	}
	return nil, nil
}

// evalIn is x = m1 or x = m2 ... with SQL's semantics folded in: a null
// operand is null; a match is true; a null MEMBER with no match makes
// the answer UNKNOWN (null), never false.
func evalIn(x exprIn, payload, parent any) (any, error) {
	v, err := Eval(x.operand, payload, parent)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	sawNull := false
	for _, m := range x.list {
		mv, err := Eval(m, payload, parent)
		if err != nil {
			return nil, err
		}
		if mv == nil {
			sawNull = true
			continue
		}
		eq, err := exprEqual(v, mv)
		if err != nil {
			return nil, err
		}
		if eq {
			return true, nil
		}
	}
	if sawNull {
		return nil, nil
	}
	return false, nil
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
	a, b, err := coerceTimePair(a, b)
	if err != nil {
		return false, err
	}
	if fa := orderFamily(a); (fa == "time" || fa == "interval") && fa == orderFamily(b) {
		return !collectLess(a, b) && !collectLess(b, a), nil
	}
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

// FormatRat renders an exact rational in minimal decimal form: no exponent,
// no trailing zeros, integral values without a point. The denominator must
// be of the form 2^a·5^b (a terminating decimal) — guaranteed for every
// materialized value by the admission-time domination rule; anything else
// is an internal invariant violation, reported loudly.
func FormatRat(r *big.Rat) (string, error) {
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
