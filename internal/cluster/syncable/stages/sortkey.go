package stages

import (
	"encoding/binary"
	"fmt"
	"math/big"
)

// Order-preserving value encoding — the sort keys of ordered stage
// indexes (a window stage's partition order; later the as-of join's
// dimension versions). An encoded value's BYTES compare exactly as the
// value compares in the shared value order (collectLess: numbers, then
// strings, then bools, then date/time values on one timeline, then
// intervals), so a bbolt cursor walks rows in business order and a seek
// finds a position without decoding anything.
//
// Every encoding is self-delimiting and prefix-free (no encoding is a
// proper prefix of another), which is what lets values concatenate into
// a tuple that still compares value by value, and lets a descending key
// invert its bytes: for prefix-free codes, inverting every byte reverses
// the order exactly. Nulls are single marker bytes at the extremes (0x00
// first, 0xFF last), outside every ascending or inverted value tag, so
// NULLS FIRST/LAST hold in either direction.
//
// Numbers encode exactly (a terminating decimal: sign class, a signed
// exponent, the digits); a quotient with no finite decimal form has no
// encoding, which is why sort keys must be materializable (division under
// round/trunc), like stored values.

const (
	sortNullFirst   byte = 0x00
	sortNullLast    byte = 0xFF
	sortTagNumber   byte = 0x10
	sortTagString   byte = 0x20
	sortTagBool     byte = 0x30
	sortTagTime     byte = 0x40
	sortTagInterval byte = 0x50

	sortSignNeg  byte = 0x01
	sortSignZero byte = 0x02
	sortSignPos  byte = 0x03
)

// appendSortKey appends one value's sort encoding: ascending, or
// descending (every byte inverted); a null sorts first or last as asked,
// in either direction.
func appendSortKey(dst []byte, v any, desc, nullsFirst bool) ([]byte, error) {
	if v == nil {
		if nullsFirst {
			return append(dst, sortNullFirst), nil
		}
		return append(dst, sortNullLast), nil
	}
	start := len(dst)
	dst, err := appendSortValue(dst, v)
	if err != nil {
		return nil, err
	}
	if desc {
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
	}
	return dst, nil
}

// appendSortValue appends a non-null value's ascending encoding.
func appendSortValue(dst []byte, v any) ([]byte, error) {
	switch t := v.(type) {
	case *big.Rat:
		dst = append(dst, sortTagNumber)
		switch t.Sign() {
		case 0:
			return append(dst, sortSignZero), nil
		case 1:
			dst = append(dst, sortSignPos)
			return appendDecimalMagnitude(dst, t)
		default:
			dst = append(dst, sortSignNeg)
			start := len(dst)
			dst, err := appendDecimalMagnitude(dst, new(big.Rat).Neg(t))
			if err != nil {
				return nil, err
			}
			// A larger magnitude is a smaller negative number: invert.
			for i := start; i < len(dst); i++ {
				dst[i] = ^dst[i]
			}
			return dst, nil
		}
	case string:
		dst = append(dst, sortTagString)
		for i := 0; i < len(t); i++ {
			if t[i] == 0x00 {
				dst = append(dst, 0x00, 0xFF) // escaped NUL
				continue
			}
			dst = append(dst, t[i])
		}
		return append(dst, 0x00, 0x01), nil // terminator, below any escape
	case bool:
		if t {
			return append(dst, sortTagBool, 0x02), nil
		}
		return append(dst, sortTagBool, 0x01), nil
	case timestampVal, dateVal:
		us, _ := timeInstantUs(t)
		dst = append(dst, sortTagTime)
		return binary.BigEndian.AppendUint64(dst, uint64(us)^(1<<63)), nil //nolint:gosec // G115: the sign flip IS the order-preserving int64 → uint64 mapping
	case intervalVal:
		dst = append(dst, sortTagInterval)
		return appendBigIntSortKey(dst, intervalCmpValue(t)), nil
	}
	return nil, fmt.Errorf("%s has no order (only scalars sort)", describeValue(v))
}

// appendDecimalMagnitude encodes a positive terminating decimal as
// 0.d1d2…dk × 10^E: the exponent as an order-preserving int64, then the
// digits (each +1, so 0x00 can terminate), shortest form (no trailing
// zeros) — a larger exponent is a larger value, and at equal exponents the
// digit strings compare lexically with a shorter prefix first.
func appendDecimalMagnitude(dst []byte, r *big.Rat) ([]byte, error) {
	num, den := r.Num(), r.Denom()
	d := new(big.Int).Set(den)
	twos, fives := 0, 0
	two, five := big.NewInt(2), big.NewInt(5)
	mod := new(big.Int)
	for {
		if q, m := new(big.Int).QuoRem(d, two, mod); m.Sign() == 0 {
			d, twos = q, twos+1
			continue
		}
		break
	}
	for {
		if q, m := new(big.Int).QuoRem(d, five, mod); m.Sign() == 0 {
			d, fives = q, fives+1
			continue
		}
		break
	}
	if d.Cmp(big.NewInt(1)) != 0 {
		return nil, fmt.Errorf("the number %s has no finite decimal form to order by — round it", r.RatString())
	}
	k := max(twos, fives)
	scaled := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k)), nil))
	scaled.Quo(scaled, den)
	digits := scaled.String()
	end := len(digits)
	for end > 1 && digits[end-1] == '0' {
		end--
	}
	// value = scaled × 10^-k = 0.digits × 10^(len(digits)-k); trailing
	// zeros of the fraction change nothing, so they drop.
	exp := int64(len(digits)) - int64(k)
	dst = binary.BigEndian.AppendUint64(dst, uint64(exp)^(1<<63)) //nolint:gosec // G115: order-preserving sign flip
	for i := 0; i < end; i++ {
		dst = append(dst, digits[i]-'0'+1)
	}
	return append(dst, 0x00), nil
}

// appendBigIntSortKey encodes a signed integer: a sign class, then for a
// nonzero value its magnitude length and big-endian bytes (a longer
// magnitude is larger), inverted for negatives.
func appendBigIntSortKey(dst []byte, x *big.Int) []byte {
	switch x.Sign() {
	case 0:
		return append(dst, sortSignZero)
	case 1:
		mag := x.Bytes()
		dst = append(dst, sortSignPos, byte(len(mag))) //nolint:gosec // G115: an interval's comparison value is far below 2^2040
		return append(dst, mag...)
	default:
		mag := new(big.Int).Neg(x).Bytes()
		dst = append(dst, sortSignNeg)
		start := len(dst)
		dst = append(dst, byte(len(mag))) //nolint:gosec // G115: as above
		dst = append(dst, mag...)
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
		return dst
	}
}
