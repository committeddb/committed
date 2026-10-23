package stages

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// codecStage has two expression slots, one with two order keys, and two
// legacy arms — every shape a state carries.
func codecStage() *Stage {
	return &Stage{
		aggSlots: []exprAgg{{}, {order: make([]aggOrder, 2)}},
		Emit:     []Emit{{Count: true}, {Collect: "$.s"}},
	}
}

func randomStateValue(rng *rand.Rand) any {
	switch rng.Intn(9) {
	case 0:
		return nil
	case 1:
		return big.NewRat(rng.Int63n(2000)-1000, rng.Int63n(97)+1)
	case 2:
		r, _ := new(big.Rat).SetString("-123456789012345678901234567890.0625")
		return r
	case 3:
		return fmt.Sprintf("s%d\x00\xff", rng.Intn(5)) // NUL and invalid UTF-8 kept exactly
	case 4:
		return rng.Intn(2) == 0
	case 5:
		return timestampVal{us: rng.Int63() - rng.Int63(), zoned: rng.Intn(2) == 0}
	case 6:
		return dateVal{days: rng.Int63n(1e6) - 5e5}
	case 7:
		return intervalVal{months: rng.Int63n(100) - 50, days: rng.Int63n(100), us: -rng.Int63()}
	}
	return ""
}

func randomAggState(rng *rand.Rand, st *Stage) *aggState {
	slot := func(nOrd int) slotState {
		ss := slotState{n: rng.Int63n(1e9) - 5e8, best: randomStateValue(rng)}
		if rng.Intn(2) == 0 {
			ss.sum = big.NewRat(rng.Int63n(1e6)-5e5, rng.Int63n(1000)+1)
		}
		for i := range rng.Intn(4) {
			it := stateItem{inKey: fmt.Appendf(nil, "in%d", i), val: randomStateValue(rng)}
			for range nOrd {
				it.ord = append(it.ord, randomStateValue(rng))
			}
			ss.items = append(ss.items, it)
		}
		return ss
	}
	as := &aggState{count: rng.Int63n(1e6), slots: make([]slotState, len(st.aggSlots)), emits: make([]slotState, len(st.Emit))}
	for i := range as.slots {
		as.slots[i] = slot(len(st.aggSlots[i].order))
	}
	for i := range as.emits {
		as.emits[i] = slot(0)
	}
	return as
}

// The binary state decodes to exactly the state encoded, and re-encodes to
// the same bytes.
func TestAggStateBinaryRoundTrip(t *testing.T) {
	st := codecStage()
	rng := rand.New(rand.NewSource(3))
	for i := range 2000 {
		as := randomAggState(rng, st)
		raw, err := encodeAggState(as)
		require.NoError(t, err)
		got, err := decodeAggState(st, raw)
		require.NoError(t, err, "state %d", i)
		require.Equal(t, as, got, "state %d", i)
		again, err := encodeAggState(got)
		require.NoError(t, err)
		require.Equal(t, raw, again, "state %d", i)
	}
}

// A store written by an earlier release holds JSON states; they still read.
func TestAggStateReadsLegacyJSON(t *testing.T) {
	st := codecStage()
	rng := rand.New(rand.NewSource(5))
	for i := range 500 {
		as := randomAggState(rng, st)
		for _, slots := range [][]slotState{as.slots, as.emits} {
			for k := range slots { // JSON repairs invalid UTF-8, so compare on valid strings
				slots[k].best = validUTF8(slots[k].best)
				for j := range slots[k].items {
					slots[k].items[j].val = validUTF8(slots[k].items[j].val)
					for o := range slots[k].items[j].ord {
						slots[k].items[j].ord[o] = validUTF8(slots[k].items[j].ord[o])
					}
				}
			}
		}
		raw, err := encodeAggStateJSON(as)
		require.NoError(t, err)
		require.Equal(t, byte('{'), raw[0])
		got, err := decodeAggState(st, raw)
		require.NoError(t, err, "state %d", i)
		require.Equal(t, as, nilEmptyOrders(got), "state %d", i)
	}
}

// validUTF8 quotes a string, which makes it valid UTF-8.
func validUTF8(v any) any {
	if s, ok := v.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return v
}

// A cut or corrupted state is refused (the key then refolds in full), never
// a panic or a different state.
func TestAggStateRejectsDamage(t *testing.T) {
	st := codecStage()
	rng := rand.New(rand.NewSource(9))
	for range 300 {
		raw, err := encodeAggState(randomAggState(rng, st))
		require.NoError(t, err)
		for cut := range len(raw) {
			_, err := decodeAggState(st, raw[:cut])
			require.Error(t, err, "a state cut to %d of %d bytes decoded", cut, len(raw))
		}
		_, err = decodeAggState(st, append(append([]byte(nil), raw...), 0))
		require.Error(t, err, "trailing bytes")
		damaged := append([]byte(nil), raw...)
		damaged[rng.Intn(len(damaged))] ^= byte(1 + rng.Intn(255))
		require.NotPanics(t, func() { _, _ = decodeAggState(st, damaged) })
	}
	_, err := decodeAggState(&Stage{aggSlots: []exprAgg{{}}}, mustEncode(t, randomAggState(rng, st)))
	require.Error(t, err, "a state of another shape")
}

func mustEncode(t *testing.T, as *aggState) []byte {
	t.Helper()
	raw, err := encodeAggState(as)
	require.NoError(t, err)
	return raw
}

// TestAggStateBinaryLayout pins the binary layout: stores hold these bytes
// across upgrades, so a layout change needs a new version byte (and a read
// path for this one), never a silent reinterpretation.
func TestAggStateBinaryLayout(t *testing.T) {
	as := &aggState{
		count: 3,
		slots: []slotState{
			{n: 2, sum: big.NewRat(-7, 4), best: "b"},
			{n: 1, items: []stateItem{{inKey: []byte("k1"), val: timestampVal{us: 5, zoned: true}, ord: []any{dateVal{days: -1}, nil}}}},
		},
		emits: []slotState{
			{n: 3, best: true},
			{items: []stateItem{{inKey: []byte("k2"), val: intervalVal{months: 1, days: 2, us: 3}}, {inKey: []byte("k3"), val: false}}},
		},
	}
	raw, err := encodeAggState(as)
	require.NoError(t, err)
	require.Equal(t, "010602040101010701040201620002000001026b31060a02070100020600040000000002026b320802040600026b330300", hex.EncodeToString(raw))
	got, err := decodeAggState(&Stage{aggSlots: []exprAgg{{}, {order: make([]aggOrder, 2)}}, Emit: []Emit{{}, {}}}, raw)
	require.NoError(t, err)
	require.Equal(t, as, got)
}

// nilEmptyOrders makes the JSON reader's empty order lists nil, as the fold
// builds them; nothing tells the two apart.
func nilEmptyOrders(as *aggState) *aggState {
	for _, slots := range [][]slotState{as.slots, as.emits} {
		for k := range slots {
			for j := range slots[k].items {
				if len(slots[k].items[j].ord) == 0 {
					slots[k].items[j].ord = nil
				}
			}
		}
	}
	return as
}
