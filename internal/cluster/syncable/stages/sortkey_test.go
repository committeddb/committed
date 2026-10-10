package stages

import (
	"bytes"
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// sortKeyCorpus is a hand-picked set of values covering every family's
// edges: signs, zero, exponents, digit-prefix pairs, NUL bytes in text,
// the time families sharing one timeline, and interval normalization.
func sortKeyCorpus() []any {
	rat := func(s string) any {
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			panic(s)
		}
		return r
	}
	return []any{
		rat("0"), rat("1"), rat("-1"), rat("10"), rat("9"), rat("100"), rat("0.1"),
		rat("0.12"), rat("0.123"), rat("0.13"), rat("-0.12"), rat("-0.123"), rat("-0.13"),
		rat("123456789012345678901234567890"), rat("-123456789012345678901234567890"),
		rat("0.000001"), rat("-0.000001"), rat("1e30"), rat("1e-30"), rat("5"), rat("5.5"), rat("-5.5"),
		"", "a", "a\x00", "a\x00b", "a\x01", "ab", "b", "\x00", "\xff", "zz",
		false, true,
		timestampVal{us: 0},
		timestampVal{us: -1},
		timestampVal{us: 86_400_000_000},
		dateVal{days: 1},
		dateVal{days: -1},
		timestampVal{us: 1, zoned: true},
		intervalVal{},
		intervalVal{months: 1},
		intervalVal{days: 30},
		intervalVal{days: 31},
		intervalVal{us: -1},
		intervalVal{months: -2, days: 3},
		intervalVal{us: 1 << 40},
		nil,
	}
}

func encodeSortKeyForTest(t *testing.T, v any, desc, nullsFirst bool) []byte {
	t.Helper()
	b, err := appendSortKey(nil, v, desc, nullsFirst)
	require.NoError(t, err, "%v", v)
	return b
}

// wantOrder is the value order a sort key must reproduce: compareNullable
// (the shared order, nulls placed as asked), reversed for desc — except
// that null placement never reverses.
func wantOrder(a, b any, desc, nullsFirst bool) int {
	if a == nil || b == nil {
		return compareNullable(a, b, nullsFirst)
	}
	c := compareNullable(a, b, nullsFirst)
	if desc {
		return -c
	}
	return c
}

func TestSortKeyOrderMatchesValueOrder(t *testing.T) {
	vals := sortKeyCorpus()
	for _, desc := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			for _, a := range vals {
				for _, b := range vals {
					ka := encodeSortKeyForTest(t, a, desc, nullsFirst)
					kb := encodeSortKeyForTest(t, b, desc, nullsFirst)
					require.Equal(t, wantOrder(a, b, desc, nullsFirst), bytes.Compare(ka, kb),
						"desc=%v nullsFirst=%v: %#v vs %#v", desc, nullsFirst, a, b)
				}
			}
		}
	}
}

// Prefix-freeness is what makes tuples compare value by value and what
// makes descending inversion exact.
func TestSortKeyIsPrefixFree(t *testing.T) {
	vals := sortKeyCorpus()
	for _, desc := range []bool{false, true} {
		for _, a := range vals {
			for _, b := range vals {
				ka := encodeSortKeyForTest(t, a, desc, false)
				kb := encodeSortKeyForTest(t, b, desc, false)
				if bytes.Equal(ka, kb) {
					continue
				}
				require.False(t, bytes.HasPrefix(kb, ka), "desc=%v: %#v's key is a prefix of %#v's", desc, a, b)
			}
		}
	}
}

// Tuples: (a1, a2) vs (b1, b2) compare lexicographically by value, with
// mixed directions — the window stage's orderBy list.
func TestSortKeyTuplesCompareValueByValue(t *testing.T) {
	vals := sortKeyCorpus()
	r := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		a1, a2 := vals[r.IntN(len(vals))], vals[r.IntN(len(vals))]
		b1, b2 := vals[r.IntN(len(vals))], vals[r.IntN(len(vals))]
		d1, d2 := r.IntN(2) == 0, r.IntN(2) == 0
		n1, n2 := r.IntN(2) == 0, r.IntN(2) == 0
		ka := encodeSortKeyForTest(t, a1, d1, n1)
		ka = append(ka, encodeSortKeyForTest(t, a2, d2, n2)...)
		kb := encodeSortKeyForTest(t, b1, d1, n1)
		kb = append(kb, encodeSortKeyForTest(t, b2, d2, n2)...)
		want := wantOrder(a1, b1, d1, n1)
		if want == 0 {
			want = wantOrder(a2, b2, d2, n2)
		}
		require.Equal(t, want, bytes.Compare(ka, kb), "(%#v,%#v) vs (%#v,%#v)", a1, a2, b1, b2)
	}
}

// Random rationals with terminating decimals, against big.Rat's own order.
func TestSortKeyNumbersRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	gen := func() *big.Rat {
		n := new(big.Rat).SetInt64(r.Int64N(2_000_001) - 1_000_000)
		scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(r.IntN(12))), nil))
		if r.IntN(2) == 0 {
			return n.Quo(n, scale)
		}
		return n.Mul(n, scale)
	}
	for range 20000 {
		a, b := gen(), gen()
		ka := encodeSortKeyForTest(t, a, false, false)
		kb := encodeSortKeyForTest(t, b, false, false)
		require.Equal(t, a.Cmp(b), bytes.Compare(ka, kb), "%s vs %s", a.RatString(), b.RatString())
	}
}

func TestSortKeyRejectsUnorderable(t *testing.T) {
	_, err := appendSortKey(nil, new(big.Rat).SetFrac64(1, 3), false, false)
	require.ErrorContains(t, err, "no finite decimal form")
	_, err = appendSortKey(nil, []any{"a"}, false, false)
	require.ErrorContains(t, err, "only scalars sort")
	// Equal values with different spellings share one key: 5 and 5.0, a
	// date and its midnight timestamp, '1 mon' and '30 days'.
	k1 := encodeSortKeyForTest(t, new(big.Rat).SetInt64(5), false, false)
	k2 := encodeSortKeyForTest(t, new(big.Rat).SetFrac64(50, 10), false, false)
	require.Equal(t, k1, k2)
	require.Equal(t, encodeSortKeyForTest(t, dateVal{days: 2}, false, false), encodeSortKeyForTest(t, timestampVal{us: 2 * usPerDay}, false, false))
	require.Equal(t, encodeSortKeyForTest(t, intervalVal{months: 1}, true, false), encodeSortKeyForTest(t, intervalVal{days: 30}, true, false))
}
