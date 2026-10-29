package stages

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// codecStage has a slot of every layout — one item list with two order
// keys, a first-only list with one — and a legacy arm of every layout,
// with an expr emit (which keeps nothing) between them.
func codecStage() *Stage {
	return &Stage{
		aggSlots: []exprAgg{
			{fn: "count"},
			{fn: "sum"},
			{fn: "min"},
			{fn: "array_agg", order: make([]aggOrder, 2)},
			{fn: "array_agg", order: make([]aggOrder, 1), firstOnly: true},
		},
		Emit: []Emit{{Count: true}, {Collect: "$.s"}, {Expr: "count(*)"}, {Sum: "$.n"}, {Max: "$.n"}},
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

// randomAggState fills each slot as its fold would: only the fields its
// layout keeps, with a sum present exactly when n is not 0 and a first-only
// slot holding one item exactly when n is above 0.
func randomAggState(rng *rand.Rand, st *Stage) *aggState {
	item := func(i, nOrd int) stateItem {
		it := stateItem{inKey: fmt.Appendf(nil, "in%d", i), val: randomStateValue(rng)}
		for range nOrd {
			it.ord = append(it.ord, randomStateValue(rng))
		}
		return it
	}
	slot := func(l slotLayout, nOrd int) slotState {
		var ss slotState
		switch l {
		case layoutCount:
			ss.n = rng.Int63n(1e9) - 5e8
		case layoutSum:
			if rng.Intn(3) > 0 {
				ss.n = rng.Int63n(1e9) + 1
				ss.sum = big.NewRat(rng.Int63n(1e6)-5e5, rng.Int63n(1000)+1)
			}
		case layoutBest:
			ss.n, ss.best = rng.Int63n(1e9), randomStateValue(rng)
		case layoutItems:
			for i := range rng.Intn(4) {
				ss.items = append(ss.items, item(i, nOrd))
			}
		case layoutFirst:
			if ss.n = rng.Int63n(3); ss.n > 0 {
				ss.items = []stateItem{item(0, nOrd)}
			}
		}
		return ss
	}
	as := newAggState(st)
	as.count = rng.Int63n(1e6)
	for i := range as.slots {
		as.slots[i] = slot(st.aggSlots[i].layout(), len(st.aggSlots[i].order))
	}
	for i := range as.emits {
		as.emits[i] = slot(emitLayout(&st.Emit[i]), 0)
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
		raw := encodeAggState(st, as)
		got, err := decodeAggState(st, raw)
		require.NoError(t, err, "state %d", i)
		require.Equal(t, as, got, "state %d", i)
		require.Equal(t, raw, encodeAggState(st, got), "state %d", i)
	}
}

// A cut or corrupted state is refused (the key then refolds in full), never
// a panic or a different state.
func TestAggStateRejectsDamage(t *testing.T) {
	st := codecStage()
	rng := rand.New(rand.NewSource(9))
	for range 300 {
		raw := encodeAggState(st, randomAggState(rng, st))
		for cut := range len(raw) {
			_, err := decodeAggState(st, raw[:cut])
			require.Error(t, err, "a state cut to %d of %d bytes decoded", cut, len(raw))
		}
		_, err := decodeAggState(st, append(append([]byte(nil), raw...), 0))
		require.Error(t, err, "trailing bytes")
		damaged := append([]byte(nil), raw...)
		damaged[rng.Intn(len(damaged))] ^= byte(1 + rng.Intn(255))
		require.NotPanics(t, func() { _, _ = decodeAggState(st, damaged) })
	}
	_, err := decodeAggState(&Stage{aggSlots: []exprAgg{{fn: "count"}}}, encodeAggState(st, randomAggState(rng, st)))
	require.Error(t, err, "a state of another shape")
}

// TestAggStateBinaryLayout pins the binary layout: a layout change is a
// store format change (stagestore.formatVersion), never a silent
// reinterpretation of stored bytes.
func TestAggStateBinaryLayout(t *testing.T) {
	st := codecStage()
	as := &aggState{
		count: 3,
		slots: []slotState{
			{n: 2},
			{n: 2, sum: big.NewRat(-7, 4)},
			{n: 1, best: "b"},
			{items: []stateItem{{inKey: []byte("k1"), val: timestampVal{us: 5, zoned: true}, ord: []any{dateVal{days: -1}, nil}}}},
			{n: 2, items: []stateItem{{inKey: []byte("k2"), val: "x", ord: []any{true}}}},
		},
		emits: []slotState{
			{n: 3},
			{items: []stateItem{{inKey: []byte("k3"), val: intervalVal{months: 1, days: 2, us: 3}}, {inKey: []byte("k4"), val: false}}},
			{},
			{},
			{n: 1, best: big.NewRat(9, 1)},
		},
	}
	raw := encodeAggState(st, as)
	require.Equal(t, "0206040401010701040202016201026b31060a07010004026b32020178040602026b3308020406026b34030002010001090101", hex.EncodeToString(raw))
	got, err := decodeAggState(st, raw)
	require.NoError(t, err)
	require.Equal(t, as, got)
}
