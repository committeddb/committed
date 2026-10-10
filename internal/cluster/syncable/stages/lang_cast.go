package stages

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Casts and the math functions of the expression language, PostgreSQL 16
// semantics. `cast(x as t)` and `x::t` convert between the language's
// value families; `try_cast(x as t)` is the same conversion yielding null
// where cast would fail — committed's one addition, so a producer's bad
// string can be skipped deliberately instead of failing the event. The
// listed divergences from PG: NaN/Infinity (numeric accepts them; exact
// arithmetic cannot represent them) are a conversion failure, and a
// number casts to text in canonical digits (1.50 → "1.5").

// exprCast is cast(operand as typ) / operand::typ / try_cast(…).
type exprCast struct {
	operand Node
	typ     string // canonical: numeric, integer, bigint, text, boolean
	try     bool
}

func (exprCast) node() {}

// castTypes maps the accepted PG type names to their canonical form.
var castTypes = map[string]string{
	"numeric": "numeric", "decimal": "numeric",
	"integer": "integer", "int": "integer", "int4": "integer",
	"bigint": "bigint", "int8": "bigint",
	"text": "text", "varchar": "text",
	"boolean": "boolean", "bool": "boolean",
	"timestamptz": "timestamptz", "timestamp": "timestamp", "date": "date", "interval": "interval",
}

const castTypeList = "numeric, decimal, integer, int, int4, bigint, int8, text, varchar, boolean, bool, timestamptz, timestamp, date, interval"

// parseCastType reads the type name after `as` or `::`.
func (p *exprParser) parseCastType() (string, error) {
	t := p.next()
	if t.kind == "ident" {
		if typ, ok := castTypes[strings.ToLower(t.text)]; ok {
			return typ, nil
		}
	}
	return "", fmt.Errorf("unknown cast type %q at position %d (the types are %s)", t.text, t.pos, castTypeList)
}

// parseCastCall parses the rest of `cast(x as t)` / `try_cast(x as t)`
// after the keyword.
func (p *exprParser) parseCastCall(try bool) (Node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	operand, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if !p.atKeyword("as") {
		t := p.peek()
		return nil, fmt.Errorf("expected as in cast at position %d, got %q", t.pos, t.text)
	}
	p.next()
	typ, err := p.parseCastType()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return exprCast{operand, typ, try}, nil
}

// parsePostfix is a primary followed by any number of `::type` casts,
// which bind tighter than unary minus, as in PG (-'5'::int is -(5)).
func (p *exprParser) parsePostfix() (Node, error) {
	n, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != "op" || t.text != "::" {
			return n, nil
		}
		p.next()
		typ, err := p.parseCastType()
		if err != nil {
			return nil, err
		}
		n = exprCast{n, typ, false}
	}
}

func evalCast(x exprCast, payload, parent any) (any, error) {
	v, err := Eval(x.operand, payload, parent)
	if err != nil || v == nil {
		return nil, err
	}
	out, err := castValue(v, x.typ)
	if err != nil && x.try {
		return nil, nil // try_cast: a failed conversion is null
	}
	return out, err
}

// castValue converts one non-null value; every error it returns is a
// conversion failure (what try_cast turns into null).
func castValue(v any, typ string) (any, error) {
	switch typ {
	case "numeric":
		switch t := v.(type) {
		case *big.Rat:
			return t, nil
		case string:
			return parsePGNumber(t, "numeric")
		}
	case "integer", "bigint":
		var r *big.Rat
		switch t := v.(type) {
		case *big.Rat:
			r = scaleRat(t, 0, true) // PG rounds half away from zero
		case string:
			var err error
			if r, err = parsePGNumber(t, typ); err != nil {
				return nil, err
			}
		case bool:
			if t {
				return big.NewRat(1, 1), nil
			}
			return new(big.Rat), nil
		}
		if r != nil {
			if !inIntRange(r.Num(), typ) {
				return nil, fmt.Errorf("%s is out of range for %s", r.Num(), typ)
			}
			return r, nil
		}
	case "text":
		return renderText(v, "cast")
	case "timestamptz", "timestamp":
		zoned := typ == "timestamptz"
		switch t := v.(type) {
		case string:
			return parseTimestamp(t, zoned)
		case timestampVal:
			return timestampVal{t.us, zoned}, nil
		case dateVal:
			return timestampVal{t.days * usPerDay, zoned}, nil
		}
	case "date":
		switch t := v.(type) {
		case string:
			return parseDate(t)
		case dateVal:
			return t, nil
		case timestampVal:
			return dateVal{floorDiv(t.us, usPerDay)}, nil
		}
	case "interval":
		switch t := v.(type) {
		case string:
			return parseInterval(t)
		case intervalVal:
			return t, nil
		}
	case "boolean":
		switch t := v.(type) {
		case bool:
			return t, nil
		case string:
			if b, ok := parsePGBool(t); ok {
				return b, nil
			}
			return nil, fmt.Errorf("invalid input for boolean: %q", t)
		case *big.Rat:
			// PG casts an integer (nonzero is true) but not a numeric.
			if !t.IsInt() {
				return nil, fmt.Errorf("cannot cast the non-integer number %s to boolean", t.FloatString(6))
			}
			return t.Sign() != 0, nil
		}
	}
	return nil, fmt.Errorf("cannot cast %s to %s", describeValue(v), typ)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func inIntRange(n *big.Int, typ string) bool {
	if !n.IsInt64() {
		return false
	}
	if typ == "integer" {
		return n.Int64() >= -1<<31 && n.Int64() <= 1<<31-1
	}
	return true
}

// maxCastExponent bounds a parsed exponent before 10^e is built: beyond
// it the value exceeds the expression bit cap anyway, and building it
// would be the pathology the cap exists to prevent.
const maxCastExponent = 1300

// parsePGNumber parses PostgreSQL 16's input syntax for typ — numeric,
// or integer/bigint (no fraction, no exponent): surrounding ASCII
// whitespace, an optional sign, then either a 0x/0o/0b-prefixed whole
// number or decimal digits with an optional fraction and exponent; a
// single underscore may separate digits (and follow a base prefix).
func parsePGNumber(s, typ string) (*big.Rat, error) {
	invalid := fmt.Errorf("invalid input for %s: %q", typ, s)
	body := strings.Trim(s, " \t\n\r\v\f")
	neg := false
	if body != "" && (body[0] == '+' || body[0] == '-') {
		neg = body[0] == '-'
		body = body[1:]
	}
	if typ == "numeric" {
		switch strings.ToLower(body) {
		case "nan", "infinity", "inf":
			return nil, fmt.Errorf("%q has no exact decimal value (PostgreSQL's numeric accepts NaN and Infinity; exact arithmetic cannot)", s)
		}
	}
	var r *big.Rat
	if len(body) >= 2 && body[0] == '0' && strings.ContainsRune("xXoObB", rune(body[1])) {
		base := map[byte]int{'x': 16, 'o': 8, 'b': 2}[body[1]|0x20]
		digits, i := scanDigits(body, 2, base, true)
		if digits == "" || i != len(body) {
			return nil, invalid
		}
		n, _ := new(big.Int).SetString(digits, base)
		r = new(big.Rat).SetInt(n)
	} else {
		intPart, i := scanDigits(body, 0, 10, false)
		frac := ""
		hasPoint := i < len(body) && body[i] == '.'
		if hasPoint {
			frac, i = scanDigits(body, i+1, 10, false)
		}
		if intPart == "" && frac == "" {
			return nil, invalid
		}
		exp := 0
		if i < len(body) && (body[i] == 'e' || body[i] == 'E') {
			if typ != "numeric" {
				return nil, invalid
			}
			i++
			expNeg := false
			if i < len(body) && (body[i] == '+' || body[i] == '-') {
				expNeg = body[i] == '-'
				i++
			}
			var expDigits string
			expDigits, i = scanDigits(body, i, 10, false)
			if expDigits == "" {
				return nil, invalid
			}
			e, err := strconv.Atoi(expDigits)
			if err != nil || e > maxCastExponent {
				return nil, fmt.Errorf("%q is out of range for %s", s, typ)
			}
			if expNeg {
				e = -e
			}
			exp = e
		}
		if i != len(body) || (hasPoint && typ != "numeric") {
			return nil, invalid
		}
		n, _ := new(big.Int).SetString(intPart+frac, 10)
		r = new(big.Rat).SetInt(n)
		if scale := len(frac) - exp; scale != 0 {
			pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(max(scale, -scale))), nil)
			if scale > 0 {
				r.Quo(r, new(big.Rat).SetInt(pow))
			} else {
				r.Mul(r, new(big.Rat).SetInt(pow))
			}
		}
	}
	if neg {
		r.Neg(r)
	}
	if _, err := capBits(r); err != nil {
		return nil, fmt.Errorf("%q is out of range for %s", s, typ)
	}
	return r, nil
}

// scanDigits consumes base-`base` digits from s[i:], skipping single
// underscores that sit between digits (or, with leadingUnderscore, right
// after a base prefix). It stops at the first byte that is not part of
// the group, leaving a stray underscore unconsumed for the caller to
// reject.
func scanDigits(s string, i, base int, leadingUnderscore bool) (string, int) {
	var sb strings.Builder
	for i < len(s) {
		c := s[i]
		if c == '_' {
			if (sb.Len() == 0 && !leadingUnderscore) || i+1 >= len(s) || !isBaseDigit(s[i+1], base) {
				break
			}
			i++
			continue
		}
		if !isBaseDigit(c, base) {
			break
		}
		sb.WriteByte(c)
		i++
	}
	return sb.String(), i
}

func isBaseDigit(c byte, base int) bool {
	switch {
	case c >= '0' && c <= '9':
		return int(c-'0') < base
	case base == 16:
		lc := c | 0x20
		return lc >= 'a' && lc <= 'f'
	}
	return false
}

// parsePGBool is PostgreSQL's boolean input: surrounding whitespace
// trimmed, case-insensitive, any unambiguous prefix of true/false/yes/no,
// on/off (at least two letters), or exactly 1/0.
func parsePGBool(s string) (bool, bool) {
	v := strings.ToLower(strings.Trim(s, " \t\n\r\v\f"))
	if v == "" {
		return false, false
	}
	switch v[0] {
	case 't':
		return true, strings.HasPrefix("true", v)
	case 'f':
		return false, strings.HasPrefix("false", v)
	case 'y':
		return true, strings.HasPrefix("yes", v)
	case 'n':
		return false, strings.HasPrefix("no", v)
	case 'o':
		switch v {
		case "on":
			return true, true
		case "of", "off":
			return false, true
		}
	case '1':
		return true, v == "1"
	case '0':
		return false, v == "0"
	}
	return false, false
}

// evalMathCall evaluates abs, ceil/ceiling, floor, mod, greatest, and
// least. All arguments evaluate first, so a data error surfaces
// regardless of which argument is null.
func evalMathCall(x exprCall, payload, parent any) (any, error) {
	args := make([]any, len(x.args))
	for i, a := range x.args {
		v, err := Eval(a, payload, parent)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	if x.fn == "greatest" || x.fn == "least" {
		return greatestLeast(x.fn, args)
	}
	for _, v := range args {
		if v == nil {
			return nil, nil // strict, as in PG
		}
	}
	r, err := ratOperand(args[0])
	if err != nil {
		return nil, err
	}
	switch x.fn {
	case "abs":
		return new(big.Rat).Abs(r), nil
	case "floor":
		return floorRat(r), nil
	case "ceil", "ceiling":
		f := floorRat(new(big.Rat).Neg(r))
		return f.Neg(f), nil
	case "mod":
		d, err := ratOperand(args[1])
		if err != nil {
			return nil, err
		}
		if d.Sign() == 0 {
			return nil, fmt.Errorf("division by zero in mod (guard the divisor with nullif(x, 0))")
		}
		// PG's remainder takes the dividend's sign: a - d·trunc(a/d).
		q := scaleRat(new(big.Rat).Quo(r, d), 0, false)
		return capBits(new(big.Rat).Sub(r, new(big.Rat).Mul(d, q)))
	}
	return nil, fmt.Errorf("unhandled function %q", x.fn)
}

func floorRat(r *big.Rat) *big.Rat {
	// big.Int.Div is Euclidean; with the always-positive Rat denominator
	// that is floor division.
	q := new(big.Int).Div(r.Num(), r.Denom())
	return new(big.Rat).SetInt(q)
}

// greatestLeast is PG's greatest/least: nulls are ignored (null only when
// every argument is), and the values must share a family — ordered by
// collectLess, the order of comparisons and the min/max folds.
func greatestLeast(fn string, args []any) (any, error) {
	for _, v := range args {
		if isTimeValue(v) {
			for i, a := range args {
				var err error
				if args[i], err = coerceTimeOperand(v, a); err != nil {
					return nil, err
				}
			}
			break
		}
	}
	var best any
	for _, v := range args {
		if v == nil {
			continue
		}
		if orderFamily(v) == "" {
			return nil, fmt.Errorf("%s orders scalars, got %s", fn, describeValue(v))
		}
		if best == nil {
			best = v
			continue
		}
		if orderFamily(v) != orderFamily(best) {
			return nil, fmt.Errorf("%s compares values of the same kind, got %s and %s", fn, describeValue(best), describeValue(v))
		}
		if (fn == "greatest" && collectLess(best, v)) || (fn == "least" && collectLess(v, best)) {
			best = v
		}
	}
	return best, nil
}
