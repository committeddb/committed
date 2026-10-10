package stages

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Date/time in the expression language, PostgreSQL 16 semantics under the
// UTC session committed's ingest pins. The values exist only inside
// evaluation and materialize as canonical text (Materialize):
//
//	timestamptz  2024-01-15 07:00:45.123456+00:00
//	timestamp    2024-01-15 07:00:45.123456
//	date         2024-01-15
//	interval     1 year 2 mons 3 days 04:05:06.5   (PG's own text form)
//
// The listed divergences from PG: the timestamptz offset is written
// +00:00, not PG's +00, because a MySQL DATETIME/TIMESTAMP column rejects
// +00 (and Z); input is the ISO 8601 family only — `now`/`today` (never
// deterministic), `epoch`, month names, `infinity`, and BC years are
// conversion failures — and years are 0001–9999, the range every engine
// shares; there are no named time zones (tzdata changes across releases
// would change replayed results), so a value without an offset is UTC.
// Precision is PG's microseconds. Where PG computes in float64 — fractional
// input seconds, fractional interval fields, interval × number — this code
// performs the same IEEE operations in the same order (explicit float64
// conversions prevent fused multiply-adds), so results are bit-identical.

// timestampVal is a timestamptz (zoned) or timestamp: microseconds since
// 1970-01-01 00:00:00 UTC. The two differ only in rendering (and in which
// type an operation returns), as they do in PG under a UTC session.
type timestampVal struct {
	us    int64
	zoned bool
}

// dateVal is days since 1970-01-01.
type dateVal struct{ days int64 }

// intervalVal is PG's three-part interval: months, days, and microseconds
// are kept apart because a month and a day have no fixed length.
type intervalVal struct {
	months, days, us int64
}

const (
	usPerSecond = int64(1_000_000)
	usPerMinute = 60 * usPerSecond
	usPerHour   = 60 * usPerMinute
	usPerDay    = 24 * usPerHour
)

var (
	minTimestampUs = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxTimestampUs = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() - 1
)

func timestampInRange(us int64) bool { return us >= minTimestampUs && us <= maxTimestampUs }

func dateInRange(days int64) bool {
	return timestampInRange(days * usPerDay)
}

// ── parsing ─────────────────────────────────────────────────────────────

// parseTimestamp reads the ISO 8601 family: YYYY-MM-DD, optionally
// followed by T or a space and HH:MM[:SS[.frac]], optionally followed by
// an offset Z | ±HH | ±HH:MM | ±HHMM (a space before it is allowed). A
// value without an offset is UTC. For a timestamp (zoned=false) a given
// offset is ignored, as PG's timestamp input does.
func parseTimestamp(s string, zoned bool) (timestampVal, error) {
	typ := "timestamp"
	if zoned {
		typ = "timestamptz"
	}
	invalid := fmt.Errorf("invalid input for %s: %q", typ, s)
	body := strings.Trim(s, " \t\n\r\v\f")
	y, mo, d, rest, ok := parseISODate(body)
	if !ok {
		return timestampVal{}, invalid
	}
	var tod, offset int64
	if rest != "" {
		if rest[0] != 'T' && rest[0] != 't' && rest[0] != ' ' {
			return timestampVal{}, invalid
		}
		rest = strings.TrimLeft(rest[1:], " ")
		tod, rest, ok = parseTimeOfDay(rest)
		if !ok {
			return timestampVal{}, invalid
		}
		rest = strings.TrimPrefix(rest, " ")
		if rest != "" {
			if offset, ok = parseOffset(rest); !ok {
				return timestampVal{}, invalid
			}
		}
	}
	us := time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC).UnixMicro() + tod
	if zoned {
		us -= offset
	}
	if !timestampInRange(us) {
		return timestampVal{}, fmt.Errorf("%q is out of range for %s (years 0001–9999)", s, typ)
	}
	return timestampVal{us, zoned}, nil
}

// parseDate reads a date; a time of day after it is accepted and
// dropped, as PG's date input does (so SQL Server's midnight-instant
// rendering of a date, 2024-01-15T00:00:00Z, reads as 2024-01-15).
func parseDate(s string) (dateVal, error) {
	invalid := fmt.Errorf("invalid input for date: %q", s)
	body := strings.Trim(s, " \t\n\r\v\f")
	y, mo, d, rest, ok := parseISODate(body)
	if !ok {
		return dateVal{}, invalid
	}
	if rest != "" {
		if _, err := parseTimestamp(body, false); err != nil {
			return dateVal{}, invalid
		}
	}
	return dateVal{time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC).Unix() / 86400}, nil
}

// parseISODate reads YYYY-M[M]-D[D] (a four-digit year 0001–9999; PG
// also takes one-digit months and days) and returns what follows.
func parseISODate(s string) (y, mo, d int, rest string, ok bool) {
	if len(s) < 8 || s[4] != '-' {
		return 0, 0, 0, "", false
	}
	y, ok = atoiDigits(s[0:4])
	if !ok {
		return 0, 0, 0, "", false
	}
	i := 5
	mo, i, ok = digitRun(s, i, 2)
	if !ok || i >= len(s) || s[i] != '-' {
		return 0, 0, 0, "", false
	}
	d, i, ok = digitRun(s, i+1, 2)
	if !ok || y < 1 || mo < 1 || mo > 12 || d < 1 || d > daysIn(y, mo) {
		return 0, 0, 0, "", false
	}
	return y, mo, d, s[i:], true
}

// digitRun reads 1..maxLen digits at s[i:].
func digitRun(s string, i, maxLen int) (int, int, bool) {
	j := i
	for j < len(s) && j-i < maxLen && isDigit(s[j]) {
		j++
	}
	n, ok := atoiDigits(s[i:j])
	return n, j, ok
}

// parseTimeOfDay reads H[H]:MM[:SS[.frac]] into microseconds. PG's edges
// hold: 24:00:00 is the next midnight, and second 60 rolls over.
func parseTimeOfDay(s string) (int64, string, bool) {
	i := 0
	for i < len(s) && i < 2 && isDigit(s[i]) {
		i++
	}
	if i == 0 || i >= len(s) || s[i] != ':' {
		return 0, "", false
	}
	h, _ := atoiDigits(s[:i])
	if len(s) < i+3 {
		return 0, "", false
	}
	m, ok := atoiDigits(s[i+1 : i+3])
	if !ok || m > 59 {
		return 0, "", false
	}
	i += 3
	var sec int
	var frac int64
	if i < len(s) && s[i] == ':' {
		if len(s) < i+3 {
			return 0, "", false
		}
		if sec, ok = atoiDigits(s[i+1 : i+3]); !ok {
			return 0, "", false
		}
		i += 3
		if i < len(s) && s[i] == '.' {
			j := i + 1
			for j < len(s) && isDigit(s[j]) {
				j++
			}
			if j == i+1 {
				return 0, "", false
			}
			frac = pgFractionMicros(s[i+1 : j])
			i = j
		}
	}
	if h > 24 || sec > 60 || (h == 24 && (m != 0 || sec != 0 || frac != 0)) || (sec == 60 && frac != 0) {
		return 0, "", false
	}
	return int64(h)*usPerHour + int64(m)*usPerMinute + int64(sec)*usPerSecond + frac, s[i:], true
}

// pgFractionMicros is PG's ParseFractionalSecond: strtod of the fraction,
// times 1e6, rounded half to even. Go's float64 is the same IEEE
// arithmetic, so the result is bit-identical (a 7th digit — SQL Server's
// 100ns — rounds exactly as PG rounds it). The result may be 1e6 (a
// fraction that rounds up to a whole second), which the caller adds.
func pgFractionMicros(digits string) int64 {
	f, _ := strconv.ParseFloat("0."+digits, 64)
	return int64(math.RoundToEven(f * 1e6))
}

// parseOffset reads Z | ±HH | ±HH:MM | ±HHMM into microseconds east of UTC.
func parseOffset(s string) (int64, bool) {
	if s == "Z" || s == "z" {
		return 0, true
	}
	if s[0] != '+' && s[0] != '-' {
		return 0, false
	}
	sign := int64(1)
	if s[0] == '-' {
		sign = -1
	}
	body := s[1:]
	var h, m int
	var ok bool
	switch {
	case len(body) == 2:
		h, ok = atoiDigits(body)
	case len(body) == 5 && body[2] == ':':
		var ok2 bool
		h, ok = atoiDigits(body[:2])
		m, ok2 = atoiDigits(body[3:])
		ok = ok && ok2
	case len(body) == 4:
		var ok2 bool
		h, ok = atoiDigits(body[:2])
		m, ok2 = atoiDigits(body[2:])
		ok = ok && ok2
	}
	if !ok || h > 15 || m > 59 {
		return 0, false
	}
	return sign * (int64(h)*usPerHour + int64(m)*usPerMinute), true
}

func atoiDigits(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func daysIn(y, mo int) int {
	return time.Date(y, time.Month(mo)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// intervalUnits maps PG's interval unit spellings to a canonical unit.
var intervalUnits = map[string]string{
	"microsecond": "us", "microseconds": "us", "usec": "us", "usecs": "us", "us": "us",
	"millisecond": "ms", "milliseconds": "ms", "msec": "ms", "msecs": "ms", "ms": "ms",
	"second": "s", "seconds": "s", "sec": "s", "secs": "s", "s": "s",
	"minute": "min", "minutes": "min", "min": "min", "mins": "min", "m": "min",
	"hour": "h", "hours": "h", "hr": "h", "hrs": "h", "h": "h",
	"day": "d", "days": "d", "d": "d",
	"week": "w", "weeks": "w", "w": "w",
	"month": "mon", "months": "mon", "mon": "mon", "mons": "mon",
	"year": "y", "years": "y", "yr": "y", "yrs": "y", "y": "y",
	"decade": "dec", "decades": "dec",
	"century": "c", "centuries": "c",
	"millennium": "mil", "millennia": "mil", "millenniums": "mil",
}

// parseInterval reads PG's verbose interval input — `[±]N unit` pairs (a
// bare number is seconds), an optional [±]H:MM[:SS[.frac]] time, an
// optional leading @ and trailing `ago` — or ISO 8601 (P1Y2M3DT4H5M6.5S,
// P2W). Fractional fields spill downward exactly as PG's DecodeInterval
// spills them.
func parseInterval(s string) (intervalVal, error) {
	invalid := fmt.Errorf("invalid input for interval: %q", s)
	body := strings.ToLower(strings.Trim(s, " \t\n\r\v\f"))
	if body == "" {
		return intervalVal{}, invalid
	}
	var acc intervalAcc
	if body[0] == 'p' {
		if !acc.parseISO(body[1:]) {
			return intervalVal{}, invalid
		}
		return acc.result(s)
	}
	fields := strings.Fields(body)
	ago := false
	if fields[len(fields)-1] == "ago" {
		ago = true
		fields = fields[:len(fields)-1]
	}
	if len(fields) > 0 && fields[0] == "@" {
		fields = fields[1:]
	}
	if len(fields) == 0 {
		return intervalVal{}, invalid
	}
	sawTime := false
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if strings.Contains(f, ":") {
			if sawTime {
				return intervalVal{}, invalid
			}
			neg := false
			t := f
			if t[0] == '+' || t[0] == '-' {
				neg, t = t[0] == '-', t[1:]
			}
			us, ok := parseIntervalTime(t)
			if !ok {
				return intervalVal{}, invalid
			}
			if neg {
				us = -us
			}
			acc.us += us
			sawTime = true
			continue
		}
		num, unit := splitNumberUnit(f)
		val, fval, ok := pgIntervalNumber(num)
		if !ok {
			return intervalVal{}, invalid
		}
		if unit == "" {
			if i+1 < len(fields) {
				i++
				unit = fields[i]
			} else {
				unit = "s" // a bare number is seconds, as in PG ('5' is 00:00:05)
			}
		}
		canon, ok := intervalUnits[unit]
		if !ok {
			return intervalVal{}, invalid
		}
		acc.add(val, fval, canon)
	}
	iv, err := acc.result(s)
	if err != nil {
		return intervalVal{}, err
	}
	if ago {
		iv = negInterval(iv)
	}
	return iv, nil
}

// pgIntervalNumber splits a [±]digits[.digits] field into PG's integer
// part and float fraction (the fraction carries the field's sign).
func pgIntervalNumber(s string) (int64, float64, bool) {
	neg := false
	body := s
	if body != "" && (body[0] == '+' || body[0] == '-') {
		neg, body = body[0] == '-', body[1:]
	}
	intPart, fracPart, hasPoint := strings.Cut(body, ".")
	if intPart == "" && (!hasPoint || fracPart == "") {
		return 0, 0, false
	}
	val := int64(0)
	for i := 0; i < len(intPart); i++ {
		if !isDigit(intPart[i]) || val > 1<<40 {
			return 0, 0, false
		}
		val = val*10 + int64(intPart[i]-'0')
	}
	fval := 0.0
	if hasPoint && fracPart != "" {
		for i := 0; i < len(fracPart); i++ {
			if !isDigit(fracPart[i]) {
				return 0, 0, false
			}
		}
		fval, _ = strconv.ParseFloat("0."+fracPart, 64)
	}
	if neg {
		val, fval = -val, -fval
	}
	return val, fval, true
}

// parseISO reads the part after P: [nY][nM][nW][nD][T[nH][nM][nS]].
func (a *intervalAcc) parseISO(s string) bool {
	inTime, sawAny := false, false
	for s != "" {
		if s[0] == 't' {
			if inTime {
				return false
			}
			inTime = true
			s = s[1:]
			continue
		}
		j := 0
		for j < len(s) && (isDigit(s[j]) || s[j] == '.' || s[j] == '-' || s[j] == '+') {
			j++
		}
		if j == 0 || j >= len(s) {
			return false
		}
		val, fval, ok := pgIntervalNumber(s[:j])
		if !ok {
			return false
		}
		var unit string
		switch {
		case s[j] == 'y' && !inTime:
			unit = "y"
		case s[j] == 'm' && !inTime:
			unit = "mon"
		case s[j] == 'w' && !inTime:
			unit = "w"
		case s[j] == 'd' && !inTime:
			unit = "d"
		case s[j] == 'h' && inTime:
			unit = "h"
		case s[j] == 'm' && inTime:
			unit = "min"
		case s[j] == 's' && inTime:
			unit = "s"
		default:
			return false
		}
		a.add(val, fval, unit)
		sawAny = true
		s = s[j+1:]
	}
	return sawAny
}

// parseIntervalTime reads H[H...]:MM[:SS[.frac]] (hours unbounded).
func parseIntervalTime(s string) (int64, bool) {
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}
	h, ok := atoiDigits(parts[0])
	if !ok || h > 1_000_000 {
		return 0, false
	}
	m, ok := atoiDigits(parts[1])
	if !ok || m > 59 {
		return 0, false
	}
	us := int64(h)*usPerHour + int64(m)*usPerMinute
	if len(parts) == 3 {
		secPart, fracPart, hasFrac := strings.Cut(parts[2], ".")
		sec, ok := atoiDigits(secPart)
		if !ok || sec > 59 || (hasFrac && fracPart == "") {
			return 0, false
		}
		us += int64(sec) * usPerSecond
		if hasFrac {
			for i := 0; i < len(fracPart); i++ {
				if !isDigit(fracPart[i]) {
					return 0, false
				}
			}
			us += pgFractionMicros(fracPart)
		}
	}
	return us, true
}

func splitNumberUnit(f string) (string, string) {
	j := 0
	for j < len(f) && (isDigit(f[j]) || f[j] == '.' || f[j] == '-' || f[j] == '+') {
		j++
	}
	return f[:j], f[j:]
}

// intervalAcc accumulates interval fields the way PG's DecodeInterval
// does: the integer part of each field adds exactly, and its float
// fraction spills downward — a year fraction rounds to whole months, a
// month or week fraction becomes days and then microseconds, a day
// fraction becomes microseconds.
type intervalAcc struct {
	months, days, us int64
}

func (a *intervalAcc) add(val int64, fval float64, unit string) {
	switch unit {
	case "us":
		a.addMicros(val, fval, 1)
	case "ms":
		a.addMicros(val, fval, 1000)
	case "s":
		a.addMicros(val, fval, usPerSecond)
	case "min":
		a.addMicros(val, fval, usPerMinute)
	case "h":
		a.addMicros(val, fval, usPerHour)
	case "d":
		a.days += val
		a.fractMicros(fval, usPerDay)
	case "w":
		a.days += val * 7
		a.fractDays(fval, 7)
	case "mon":
		a.months += val
		a.fractDays(fval, 30)
	case "y":
		a.addYears(val, fval, 1)
	case "dec":
		a.addYears(val, fval, 10)
	case "c":
		a.addYears(val, fval, 100)
	case "mil":
		a.addYears(val, fval, 1000)
	}
}

func (a *intervalAcc) addMicros(val int64, fval float64, scale int64) {
	a.us += val * scale
	a.fractMicros(fval, scale)
}

// fractMicros is PG's AdjustFractMicroseconds.
func (a *intervalAcc) fractMicros(frac float64, scale int64) {
	if frac == 0 {
		return
	}
	frac = float64(frac * float64(scale))
	usec := int64(frac)
	frac = float64(frac - float64(usec))
	a.us += usec + int64(math.RoundToEven(frac))
}

// fractDays is PG's AdjustFractDays.
func (a *intervalAcc) fractDays(frac float64, scale int64) {
	if frac == 0 {
		return
	}
	frac = float64(frac * float64(scale))
	extra := int64(frac)
	a.days += extra
	a.fractMicros(float64(frac-float64(extra)), usPerDay)
}

// addYears is the year-family fields: whole years are 12 months, and a
// fraction rounds to whole months (PG's AdjustFractYears).
func (a *intervalAcc) addYears(val int64, fval float64, scale int64) {
	a.months += val * scale * 12
	a.months += int64(math.RoundToEven(float64(fval * float64(scale) * 12)))
}

func (a *intervalAcc) result(src string) (intervalVal, error) {
	iv := intervalVal{a.months, a.days, a.us}
	if _, err := checkInterval(iv); err != nil {
		return intervalVal{}, fmt.Errorf("%q is out of range for interval", src)
	}
	return iv, nil
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// ── rendering ───────────────────────────────────────────────────────────

func formatTimestamp(t timestampVal) string {
	tt := time.UnixMicro(t.us).UTC()
	s := tt.Format("2006-01-02 15:04:05")
	if frac := t.us % usPerSecond; frac != 0 {
		if frac < 0 {
			frac += usPerSecond
		}
		s += "." + strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
	}
	if t.zoned {
		s += "+00:00"
	}
	return s
}

func formatDate(d dateVal) string {
	return time.Unix(d.days*86400, 0).UTC().Format("2006-01-02")
}

// formatInterval is PG's IntervalStyle 'postgres' output.
func formatInterval(iv intervalVal) string {
	var sb strings.Builder
	isZero, isBefore := true, false
	field := func(v int64, unit string) {
		if v == 0 {
			return
		}
		if !isZero {
			sb.WriteByte(' ')
		}
		if isBefore && v > 0 {
			sb.WriteByte('+')
		}
		sb.WriteString(strconv.FormatInt(v, 10))
		sb.WriteByte(' ')
		sb.WriteString(unit)
		if v != 1 {
			sb.WriteByte('s')
		}
		isBefore, isZero = v < 0, false
	}
	field(iv.months/12, "year")
	field(iv.months%12, "mon")
	field(iv.days, "day")
	if isZero || iv.us != 0 {
		if !isZero {
			sb.WriteByte(' ')
		}
		us := iv.us
		switch {
		case us < 0:
			sb.WriteByte('-')
			us = -us
		case isBefore:
			sb.WriteByte('+')
		}
		h, rem := us/usPerHour, us%usPerHour
		m, rem := rem/usPerMinute, rem%usPerMinute
		sec, frac := rem/usPerSecond, rem%usPerSecond
		fmt.Fprintf(&sb, "%02d:%02d:%02d", h, m, sec)
		if frac != 0 {
			sb.WriteString("." + strings.TrimRight(fmt.Sprintf("%06d", frac), "0"))
		}
	}
	return sb.String()
}

// Materialize turns an evaluator value into what a column or a stage
// output stores: an exact rational becomes its minimal-decimal
// json.Number, a date/time value its canonical text, and everything else
// passes through.
func Materialize(v any) (any, error) {
	switch t := v.(type) {
	case *big.Rat:
		text, err := FormatRat(t)
		if err != nil {
			return nil, err
		}
		return json.Number(text), nil
	case timestampVal:
		return formatTimestamp(t), nil
	case dateVal:
		return formatDate(t), nil
	case intervalVal:
		return formatInterval(t), nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			m, err := Materialize(e)
			if err != nil {
				return nil, err
			}
			out[i] = m
		}
		return out, nil
	}
	return v, nil
}

// ── comparison ──────────────────────────────────────────────────────────

// timeInstantUs places a date or timestamp on one timeline (a date is its
// midnight), so the three compare with each other as PG compares them.
func timeInstantUs(v any) (int64, bool) {
	switch t := v.(type) {
	case timestampVal:
		return t.us, true
	case dateVal:
		return t.days * usPerDay, true
	}
	return 0, false
}

// intervalCmpValue is PG's interval_cmp_value: months count 30 days and
// days 24 hours, so '1 mon' = '30 days' and '1 day' = '24 hours'.
func intervalCmpValue(iv intervalVal) *big.Int {
	days := new(big.Int).Add(big.NewInt(iv.days), new(big.Int).Mul(big.NewInt(iv.months), big.NewInt(30)))
	total := new(big.Int).Mul(days, big.NewInt(usPerDay))
	return total.Add(total, big.NewInt(iv.us))
}

// coerceTimeOperand reads a string compared with a date/time value as that
// value's type — PG's coercion of an untyped literal, so
// `cast($.paid as date) >= '2026-01-01'` compares dates.
func coerceTimeOperand(other, s any) (any, error) {
	str, ok := s.(string)
	if !ok {
		return s, nil
	}
	switch t := other.(type) {
	case timestampVal:
		return parseTimestamp(str, t.zoned)
	case dateVal:
		return parseDate(str)
	case intervalVal:
		return parseInterval(str)
	}
	return s, nil
}

func isTimeValue(v any) bool {
	switch v.(type) {
	case timestampVal, dateVal, intervalVal:
		return true
	}
	return false
}

// coerceTimePair applies coerceTimeOperand in whichever direction applies.
func coerceTimePair(l, r any) (any, any, error) {
	var err error
	if isTimeValue(l) {
		r, err = coerceTimeOperand(l, r)
	} else if isTimeValue(r) {
		l, err = coerceTimeOperand(r, l)
	}
	return l, r, err
}

// ── arithmetic ──────────────────────────────────────────────────────────

// evalTimeArith is PG's date/time operator table for + and - (and * on an
// interval). ok=false means neither operand is a date/time value, so the
// caller's numeric path applies.
func evalTimeArith(op string, l, r any) (any, bool, error) {
	if !isTimeValue(l) && !isTimeValue(r) {
		return nil, false, nil
	}
	l, r, err := coerceTimeArithPair(op, l, r)
	if err != nil {
		return nil, true, err
	}
	v, err := timeArith(op, l, r)
	return v, true, err
}

// coerceTimeArithPair reads an untyped string operand the way PG resolves
// an unknown literal in these operators: beside a timestamp, `+` reads it
// as an interval (ts + '1 day') and `-` as the timestamp's own type
// (ts - '2024-01-01' is an interval); beside a date, `-` reads a date
// (date - '2024-01-01' is a day count) while `+` is ambiguous, as in PG;
// beside an interval it is an interval.
func coerceTimeArithPair(op string, l, r any) (any, any, error) {
	ls, lStr := l.(string)
	rs, rStr := r.(string)
	if !lStr && !rStr {
		return l, r, nil
	}
	other, str := r, ls
	if rStr {
		other, str = l, rs
	}
	var v any
	var err error
	switch o := other.(type) {
	case intervalVal:
		v, err = parseInterval(str)
	case timestampVal:
		if op == "-" {
			v, err = parseTimestamp(str, o.zoned)
		} else {
			v, err = parseInterval(str)
		}
	case dateVal:
		if op != "-" {
			return nil, nil, fmt.Errorf("operator is not unique: date %s text — cast the text to an interval or a number of days", op)
		}
		v, err = parseDate(str)
	default:
		return l, r, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if rStr {
		return l, v, nil
	}
	return v, r, nil
}

func timeArith(op string, l, r any) (any, error) {
	undefined := fmt.Errorf("operator does not exist: %s %s %s", describeTimeType(l), op, describeTimeType(r))
	switch a := l.(type) {
	case timestampVal:
		switch b := r.(type) {
		case intervalVal:
			if op == "*" {
				return nil, undefined
			}
			if op == "-" {
				b = negInterval(b)
			}
			return addInterval(a, b)
		case timestampVal:
			if op != "-" {
				return nil, undefined
			}
			return justifyHours(a.us - b.us), nil
		case dateVal:
			if op != "-" {
				return nil, undefined
			}
			return justifyHours(a.us - b.days*usPerDay), nil
		}
	case dateVal:
		switch b := r.(type) {
		case *big.Rat:
			if op == "*" {
				return nil, undefined
			}
			n, err := wholeNumber(b, "a date moves by a whole number of days")
			if err != nil {
				return nil, err
			}
			if op == "-" {
				n = -n
			}
			d := dateVal{a.days + n}
			if !dateInRange(d.days) {
				return nil, fmt.Errorf("date out of range")
			}
			return d, nil
		case dateVal:
			if op != "-" {
				return nil, undefined
			}
			return new(big.Rat).SetInt64(a.days - b.days), nil
		case intervalVal:
			// PG: date ± interval is a timestamp (without time zone).
			if op == "*" {
				return nil, undefined
			}
			if op == "-" {
				b = negInterval(b)
			}
			return addInterval(timestampVal{a.days * usPerDay, false}, b)
		case timestampVal:
			if op != "-" {
				return nil, undefined
			}
			return justifyHours(a.days*usPerDay - b.us), nil
		}
	case intervalVal:
		switch b := r.(type) {
		case intervalVal:
			switch op {
			case "+":
				return checkInterval(intervalVal{a.months + b.months, a.days + b.days, a.us + b.us})
			case "-":
				return checkInterval(intervalVal{a.months - b.months, a.days - b.days, a.us - b.us})
			}
		case timestampVal:
			if op == "+" {
				return addInterval(b, a)
			}
		case dateVal:
			if op == "+" {
				return addInterval(timestampVal{b.days * usPerDay, false}, a)
			}
		case *big.Rat:
			if op == "*" {
				return scaleInterval(a, b)
			}
		}
	case *big.Rat:
		switch b := r.(type) {
		case dateVal:
			if op == "+" {
				return timeArith("+", b, a)
			}
		case intervalVal:
			if op == "*" {
				return scaleInterval(b, a)
			}
		}
	}
	return nil, undefined
}

func describeTimeType(v any) string {
	switch t := v.(type) {
	case timestampVal:
		if t.zoned {
			return "timestamptz"
		}
		return "timestamp"
	case dateVal:
		return "date"
	case intervalVal:
		return "interval"
	case *big.Rat:
		return "number"
	case string:
		return "text"
	case bool:
		return "boolean"
	}
	return fmt.Sprintf("%T", v)
}

func wholeNumber(r *big.Rat, why string) (int64, error) {
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("%s, got %s", why, describeValue(r))
	}
	return r.Num().Int64(), nil
}

func negInterval(iv intervalVal) intervalVal { return intervalVal{-iv.months, -iv.days, -iv.us} }

// scaleInterval is PG's interval_mul: the number becomes a float64 factor
// (PG's numeric → float8), whole months and days scale by truncation, and
// their fractional parts cascade down — months into days at 30, days into
// seconds — in PG's own float64 operations, so the result is bit-identical.
func scaleInterval(iv intervalVal, r *big.Rat) (any, error) {
	factor, _ := r.Float64()
	outOfRange := fmt.Errorf("interval out of range")
	monthD := float64(float64(iv.months) * factor)
	dayD := float64(float64(iv.days) * factor)
	if math.IsNaN(monthD) || math.Abs(monthD) >= 1<<31 || math.IsNaN(dayD) || math.Abs(dayD) >= 1<<31 {
		return nil, outOfRange
	}
	month, day := int64(monthD), int64(dayD)
	tsround := func(j float64) float64 { return math.RoundToEven(float64(j*1e6)) / 1e6 }
	monthRemainderDays := tsround(float64(float64(float64(iv.months)*factor)-float64(month)) * 30)
	secRemainder := tsround(float64(float64(float64(float64(iv.days)*factor)-float64(day))+monthRemainderDays-float64(int64(monthRemainderDays))) * 86400)
	if math.Abs(secRemainder) >= 86400 {
		whole := int64(secRemainder / 86400)
		day += whole
		secRemainder -= float64(whole * 86400)
	}
	day += int64(monthRemainderDays)
	timeD := math.RoundToEven(float64(float64(iv.us)*factor) + float64(secRemainder*1e6))
	if math.IsNaN(timeD) || math.Abs(timeD) >= 1<<63 {
		return nil, outOfRange
	}
	return checkInterval(intervalVal{month, day, int64(timeD)})
}

func checkInterval(iv intervalVal) (any, error) {
	if abs64(iv.months) > 12*178_000_000 || abs64(iv.days) > 1<<31 || abs64(iv.us) > 1<<62 {
		return nil, fmt.Errorf("interval out of range")
	}
	return iv, nil
}

// addInterval is PG's timestamp_pl_interval: months first (calendar
// arithmetic, the day clamped to the month's end — Jan 31 + 1 mon is
// Feb 29 in 2024), then days, then the time part.
func addInterval(t timestampVal, iv intervalVal) (any, error) {
	us := t.us
	if iv.months != 0 || iv.days != 0 {
		tt := time.UnixMicro(us).UTC()
		y, mo, d := tt.Date()
		tod := us - time.Date(y, mo, d, 0, 0, 0, 0, time.UTC).UnixMicro()
		total := int64(y)*12 + int64(mo) - 1 + iv.months
		ny, nmo := total/12, total%12+1
		if ny < 1 || ny > 9999 {
			return nil, fmt.Errorf("timestamp out of range")
		}
		nd := min(d, daysIn(int(ny), int(nmo)))
		us = time.Date(int(ny), time.Month(nmo), nd, 0, 0, 0, 0, time.UTC).UnixMicro() + iv.days*usPerDay + tod
	}
	us += iv.us
	if !timestampInRange(us) {
		return nil, fmt.Errorf("timestamp out of range")
	}
	return timestampVal{us, t.zoned}, nil
}

// justifyHours is the interval a timestamp difference yields in PG: whole
// days split out of the microseconds, both parts carrying the sign.
func justifyHours(us int64) intervalVal {
	return intervalVal{0, us / usPerDay, us % usPerDay}
}

// ── functions ───────────────────────────────────────────────────────────

// timeUnits names the date_trunc units and extract fields (singular and
// plural, as PG accepts them) by their canonical spelling.
var timeUnits = map[string]string{
	"microsecond": "microseconds", "microseconds": "microseconds",
	"millisecond": "milliseconds", "milliseconds": "milliseconds",
	"second": "second", "seconds": "second",
	"minute": "minute", "minutes": "minute",
	"hour": "hour", "hours": "hour",
	"day": "day", "days": "day",
	"week": "week", "weeks": "week",
	"month": "month", "months": "month",
	"quarter": "quarter", "quarters": "quarter",
	"year": "year", "years": "year",
	"decade": "decade", "decades": "decade",
	"century": "century", "centuries": "century",
	"millennium": "millennium", "millennia": "millennium", "millenniums": "millennium",
	"dow": "dow", "isodow": "isodow", "doy": "doy", "isoyear": "isoyear",
	"epoch": "epoch", "timezone": "timezone", "timezone_hour": "timezone_hour", "timezone_minute": "timezone_minute",
}

// truncUnits are the units date_trunc accepts.
var truncUnits = map[string]bool{
	"microseconds": true, "milliseconds": true, "second": true, "minute": true, "hour": true,
	"day": true, "week": true, "month": true, "quarter": true, "year": true,
	"decade": true, "century": true, "millennium": true,
}

// timeUnitLiteral validates a date_trunc unit / extract field written as
// a string literal (the language requires a literal, so the check is
// admission-time).
func timeUnitLiteral(n Node, fn string, allowed func(string) bool) (string, error) {
	s, ok := n.(exprStr)
	if !ok {
		return "", fmt.Errorf("%s's unit must be a string literal like 'month'", fn)
	}
	u, ok := timeUnits[strings.ToLower(s.val)]
	if !ok || !allowed(u) {
		return "", fmt.Errorf("%s: unit %q is not recognized", fn, s.val)
	}
	return u, nil
}

// parseExtract parses the rest of `extract(field from x)` after the
// keyword, as date_part('field', x).
func (p *exprParser) parseExtract() (Node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	t := p.next()
	if t.kind != "ident" && t.kind != "str" {
		return nil, fmt.Errorf("expected a field name in extract at position %d, got %q", t.pos, t.text)
	}
	field := exprStr{t.text}
	if _, err := timeUnitLiteral(field, "extract", func(string) bool { return true }); err != nil {
		return nil, err
	}
	if !p.atKeyword("from") {
		nt := p.peek()
		return nil, fmt.Errorf("expected from in extract at position %d, got %q", nt.pos, nt.text)
	}
	p.next()
	x, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return exprCall{"date_part", []Node{field, x}}, nil
}

// typedLiteralTypes are the types PG's `type 'string'` literal form
// covers here (date '2024-01-15', interval '30 days').
var typedLiteralTypes = map[string]string{
	"date": "date", "timestamp": "timestamp", "timestamptz": "timestamptz", "interval": "interval",
}

func evalTimeCall(x exprCall, payload, parent any) (any, error) {
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
	switch x.fn {
	case "date_trunc":
		unit := timeUnits[strings.ToLower(args[0].(string))]
		return dateTrunc(unit, args[1])
	case "date_part":
		field := timeUnits[strings.ToLower(args[0].(string))]
		return datePart(field, args[1])
	case "to_timestamp":
		r, err := ratOperand(args[0])
		if err != nil {
			return nil, err
		}
		us := scaleRat(new(big.Rat).Mul(r, new(big.Rat).SetInt64(usPerSecond)), 0, true)
		if !us.Num().IsInt64() || !timestampInRange(us.Num().Int64()) {
			return nil, fmt.Errorf("to_timestamp(%s) is out of range", r.FloatString(6))
		}
		return timestampVal{us.Num().Int64(), true}, nil
	case "make_date":
		var parts [3]int64
		for i := range parts {
			r, err := ratOperand(args[i])
			if err != nil {
				return nil, err
			}
			if parts[i], err = wholeNumber(r, "make_date takes whole numbers"); err != nil {
				return nil, err
			}
		}
		y, mo, d := parts[0], parts[1], parts[2]
		if y < 1 || y > 9999 || mo < 1 || mo > 12 || d < 1 || d > int64(daysIn(int(y), int(mo))) {
			return nil, fmt.Errorf("make_date(%d, %d, %d): date field value out of range", y, mo, d)
		}
		return dateVal{time.Date(int(y), time.Month(mo), int(d), 0, 0, 0, 0, time.UTC).Unix() / 86400}, nil
	}
	return nil, fmt.Errorf("unhandled function %q", x.fn)
}

// dateTrunc is PG's date_trunc. A date truncates as a timestamptz (PG
// resolves date_trunc(text, date) to the timestamptz form).
func dateTrunc(unit string, v any) (any, error) {
	var t timestampVal
	switch x := v.(type) {
	case timestampVal:
		t = x
	case dateVal:
		t = timestampVal{x.days * usPerDay, true}
	default:
		return nil, fmt.Errorf("date_trunc needs a timestamp or date, got %s", describeValue(v))
	}
	tt := time.UnixMicro(t.us).UTC()
	y, mo, d := tt.Date()
	day := func(y, mo, d int) int64 { return time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC).UnixMicro() }
	var us int64
	switch unit {
	case "microseconds":
		us = t.us
	case "milliseconds":
		us = t.us - floorMod(t.us, 1000)
	case "second":
		us = t.us - floorMod(t.us, usPerSecond)
	case "minute":
		us = t.us - floorMod(t.us, usPerMinute)
	case "hour":
		us = t.us - floorMod(t.us, usPerHour)
	case "day":
		us = day(y, int(mo), d)
	case "week":
		wd := (int(tt.Weekday()) + 6) % 7 // ISO: Monday is 0
		us = day(y, int(mo), d) - int64(wd)*usPerDay
	case "month":
		us = day(y, int(mo), 1)
	case "quarter":
		us = day(y, (int(mo)-1)/3*3+1, 1)
	case "year":
		us = day(y, 1, 1)
	case "decade":
		us = day(y/10*10, 1, 1)
	case "century":
		us = day((y+99)/100*100-99, 1, 1)
	case "millennium":
		us = day((y+999)/1000*1000-999, 1, 1)
	}
	if !timestampInRange(us) {
		return nil, fmt.Errorf("date_trunc: timestamp out of range")
	}
	return timestampVal{us, t.zoned}, nil
}

func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// datePart is PG's extract / date_part, returning an exact number.
func datePart(field string, v any) (any, error) {
	ratI := func(n int64) *big.Rat { return new(big.Rat).SetInt64(n) }
	ratUs := func(us, per int64) *big.Rat { return new(big.Rat).SetFrac(big.NewInt(us), big.NewInt(per)) }
	switch x := v.(type) {
	case intervalVal:
		switch field {
		case "epoch":
			// PG: a year is 365.25 days, a remaining month 30 days.
			secs := new(big.Rat).Mul(ratI(x.months/12), big.NewRat(36525*86400, 100))
			secs.Add(secs, ratI((x.months%12)*30*86400))
			secs.Add(secs, ratI(x.days*86400))
			return secs.Add(secs, ratUs(x.us, usPerSecond)), nil
		case "year":
			return ratI(x.months / 12), nil
		case "month":
			return ratI(x.months % 12), nil
		case "day":
			return ratI(x.days), nil
		case "hour":
			return ratI(x.us / usPerHour), nil
		case "minute":
			return ratI(x.us / usPerMinute % 60), nil
		case "second":
			return ratUs(x.us%usPerMinute, usPerSecond), nil
		case "milliseconds":
			return ratUs(x.us%usPerMinute, 1000), nil
		case "microseconds":
			return ratI(x.us % usPerMinute), nil
		}
		return nil, fmt.Errorf("unit %q not supported for type interval", field)
	case dateVal:
		switch field {
		case "hour", "minute", "second", "milliseconds", "microseconds", "timezone", "timezone_hour", "timezone_minute":
			return nil, fmt.Errorf("unit %q not supported for type date", field)
		case "epoch":
			return ratI(x.days * 86400), nil
		}
		return timestampPart(field, timestampVal{x.days * usPerDay, false})
	case timestampVal:
		return timestampPart(field, x)
	}
	return nil, fmt.Errorf("extract needs a date, timestamp, or interval, got %s", describeValue(v))
}

func timestampPart(field string, t timestampVal) (any, error) {
	ratI := func(n int64) *big.Rat { return new(big.Rat).SetInt64(n) }
	tt := time.UnixMicro(t.us).UTC()
	y, mo, d := tt.Date()
	secUs := floorMod(t.us, usPerMinute)
	switch field {
	case "microseconds":
		return ratI(secUs), nil
	case "milliseconds":
		return new(big.Rat).SetFrac64(secUs, 1000), nil
	case "second":
		return new(big.Rat).SetFrac64(secUs, usPerSecond), nil
	case "minute":
		return ratI(int64(tt.Minute())), nil
	case "hour":
		return ratI(int64(tt.Hour())), nil
	case "day":
		return ratI(int64(d)), nil
	case "month":
		return ratI(int64(mo)), nil
	case "quarter":
		return ratI(int64((mo-1)/3 + 1)), nil
	case "year":
		return ratI(int64(y)), nil
	case "decade":
		return ratI(int64(y / 10)), nil
	case "century":
		return ratI(int64((y + 99) / 100)), nil
	case "millennium":
		return ratI(int64((y + 999) / 1000)), nil
	case "week":
		_, w := tt.ISOWeek()
		return ratI(int64(w)), nil
	case "isoyear":
		iy, _ := tt.ISOWeek()
		return ratI(int64(iy)), nil
	case "dow":
		return ratI(int64(tt.Weekday())), nil
	case "isodow":
		return ratI(int64((int(tt.Weekday())+6)%7 + 1)), nil
	case "doy":
		return ratI(int64(tt.YearDay())), nil
	case "epoch":
		return new(big.Rat).SetFrac64(t.us, usPerSecond), nil
	case "timezone", "timezone_hour", "timezone_minute":
		if !t.zoned {
			return nil, fmt.Errorf("unit %q not supported for type timestamp", field)
		}
		return new(big.Rat), nil // always UTC
	}
	return nil, fmt.Errorf("unit %q not supported", field)
}
