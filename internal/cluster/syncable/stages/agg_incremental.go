package stages

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Incremental aggregate folds.
//
// An aggregate key refolds from its whole retained input set (the keyed
// refold, see refoldAggregate). For a key with many inputs that costs a
// decode and an evaluation per input on every change. This file keeps, next
// to the retained set, the key's FOLD STATE: the row count and, per arm, an
// exact running value — counts and sums, the current extreme, the ordered
// item list. A changed input then removes its old contribution and adds its
// new one: one decode and evaluation per CHANGED input.
//
// It is an evaluation strategy under the refold contract, never a different
// result. The state is exactly what a full refold over the same retained set
// accumulates, rendered by the same rules, so the output bytes are equal.
// Wherever that cannot be shown cheaply the key refolds in full instead,
// which also rebuilds its state:
//
//   - no state yet (a new key, a reset store), or a key dirtied by anything
//     other than its own input changes;
//   - a min/max whose extreme is removed, or tied by an equal value that
//     renders differently (the full refold keeps the first in input-key
//     order, which the state does not know);
//   - an interval sum (its range check runs on intermediate sums);
//   - any evaluation error, so the error surfaces exactly as the full
//     refold raises it.
//
// A join makes an input's membership and contribution depend on dimension
// rows, which change without the input: a dimension change marks its
// dependent keys full (Dirty.MarkFull), so each refolds in full and its
// state is rebuilt under the new rows. Merge stages keep no state.

// errAggFallback asks for a full refold of the key.
var errAggFallback = errors.New("aggregate key needs a full refold")

// incrementalAggregate reports whether a stage keeps a fold state.
func (st *Stage) incrementalAggregate() bool {
	return st.Reduce == "aggregate" && len(st.Merge) == 0
}

// usesItems reports whether a slot keeps its contributions as an item list:
// the ordered aggregates, and any distinct aggregate (distinct needs the
// values, not a running total).
func (s *exprAgg) usesItems() bool {
	return s.fn == "array_agg" || s.fn == "string_agg" || s.distinct
}

// aggDelta is one key's input changes within a drain: per input key, its
// retained bytes before the batch and its retained bytes now (nil = absent).
type aggDelta struct {
	inputs map[string]*inputChange
}

type inputChange struct {
	old, cur []byte
	curIn    *decodedInput // cur decoded, when the fold had it (nil: decode cur)
}

// decodedInput is a retained input as unpackStageInput returns it.
type decodedInput struct {
	data, parent any
}

// slotContrib is one input's contribution to one expression-aggregate slot.
type slotContrib struct {
	in  bool // counts toward the slot
	val any
	ord []any
}

// emitContrib is one input's contribution to one legacy emit arm.
type emitContrib struct {
	in  bool
	val any
}

// aggContrib is one retained input's contribution to its key.
type aggContrib struct {
	slots []slotContrib
	emits []emitContrib
}

// contribution evaluates one retained input exactly as refoldAggregate folds
// it; nil means the input is not a member (a join does not admit it).
// errAggFallback (or an evaluation error, which the caller also turns into a
// full refold) means the input cannot be accounted incrementally.
func (g *Graph) contribution(tx *stagestore.Tx, st *Stage, inKey, retained []byte) (*aggContrib, error) {
	_, payload := decodeRetained(retained)
	obj, parent, err := unpackStageInput(st, payload)
	if err != nil {
		return nil, err
	}
	return g.contributionOf(tx, st, inKey, obj, parent)
}

// contributionOf is contribution over an input already decoded.
func (g *Graph) contributionOf(tx *stagestore.Tx, st *Stage, inKey []byte, obj, parent any) (*aggContrib, error) {
	if len(st.Joins) > 0 {
		if ok, err := g.inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return nil, err
		}
		var err error
		if obj, err = g.joinLookupScope(tx, st, obj, parent); err != nil {
			return nil, err
		}
	}
	obj = withSource(obj, st.inputSource(inKey))
	c := &aggContrib{slots: make([]slotContrib, len(st.aggSlots)), emits: make([]emitContrib, len(st.Emit))}
	for k := range st.aggSlots {
		s := &st.aggSlots[k]
		sc := &c.slots[k]
		if s.filter != nil {
			if v, err := Eval(s.filter, obj, parent); err != nil || v != true {
				continue
			}
		}
		if s.arg == nil { // count(*)
			sc.in = true
			continue
		}
		v, err := Eval(s.arg, obj, parent)
		if err != nil {
			return nil, err
		}
		if s.fn == "array_agg" || s.fn == "string_agg" {
			if s.fn == "string_agg" {
				if v == nil {
					continue
				}
				if _, ok := v.(string); !ok {
					return nil, errAggFallback
				}
			}
			ord := make([]any, len(s.order))
			for i, o := range s.order {
				if ord[i], err = Eval(o.expr, obj, parent); err != nil {
					return nil, err
				}
			}
			sc.in, sc.val, sc.ord = true, v, ord
			continue
		}
		if v == nil {
			continue
		}
		switch s.fn {
		case "count", "min", "max":
			sc.in, sc.val = true, v
		case "sum", "avg":
			switch v.(type) {
			case *big.Rat:
				sc.in, sc.val = true, v
			case intervalVal:
				return nil, errAggFallback
			}
		}
	}
	for i := range st.Emit {
		e := &st.Emit[i]
		ec := &c.emits[i]
		if e.Expr != "" {
			continue
		}
		if len(e.Where) > 0 && !MatchScoped(e.Where, obj, parent) {
			continue
		}
		if e.Count {
			ec.in = true
			continue
		}
		v, err := Eval(e.compiled, obj, parent)
		if err != nil {
			return nil, err
		}
		switch {
		case e.Collect != "", e.Min != "", e.Max != "":
			if v != nil {
				ec.in, ec.val = true, v
			}
		case e.Sum != "":
			if _, ok := v.(*big.Rat); ok {
				ec.in, ec.val = true, v
			}
		}
	}
	return c, nil
}

// stateItem is one input's value in an item list, with the input key that
// breaks ties as the full refold's stable insertion order does.
type stateItem struct {
	inKey []byte
	val   any
	ord   []any
}

// slotState is one slot's (or legacy arm's) running fold.
type slotState struct {
	n     int64
	sum   *big.Rat
	best  any
	items []stateItem
}

// aggState is one key's fold state.
type aggState struct {
	count int64
	slots []slotState
	emits []slotState
}

func newAggState(st *Stage) *aggState {
	return &aggState{slots: make([]slotState, len(st.aggSlots)), emits: make([]slotState, len(st.Emit))}
}

// itemLess orders a slot's items as orderedItems does — order keys, then the
// value — with the input key last, which reproduces the stable sort over
// items inserted in input-key order.
func (s *exprAgg) itemLess(a, b *stateItem) bool {
	for i, o := range s.order {
		if c := compareNullable(a.ord[i], b.ord[i], o.nullsFirst); c != 0 {
			if o.desc && a.ord[i] != nil && b.ord[i] != nil {
				c = -c
			}
			return c < 0
		}
	}
	if c := compareNullable(a.val, b.val, false); c != 0 {
		return c < 0
	}
	return bytes.Compare(a.inKey, b.inKey) < 0
}

// collectItemLess orders a legacy collect's items as sortCollectValues does
// over values gathered in input-key order.
func collectItemLess(a, b *stateItem) bool {
	switch {
	case collectLess(a.val, b.val):
		return true
	case collectLess(b.val, a.val):
		return false
	}
	return bytes.Compare(a.inKey, b.inKey) < 0
}

func insertItem(items []stateItem, it stateItem, less func(a, b *stateItem) bool) []stateItem {
	i := sort.Search(len(items), func(i int) bool { return less(&it, &items[i]) })
	items = append(items, stateItem{})
	copy(items[i+1:], items[i:])
	items[i] = it
	return items
}

func removeItem(items []stateItem, inKey []byte) ([]stateItem, bool) {
	for i := range items {
		if bytes.Equal(items[i].inKey, inKey) {
			return append(items[:i], items[i+1:]...), true
		}
	}
	return items, false
}

// sameRendering reports whether two values that compare equal render to the
// same bytes, so which of them an extreme keeps cannot show in the output.
func sameRendering(a, b any) bool {
	ra, err1 := stageValue(a)
	rb, err2 := stageValue(b)
	if err1 != nil || err2 != nil {
		return false
	}
	ja, err1 := json.Marshal(ra)
	jb, err2 := json.Marshal(rb)
	return err1 == nil && err2 == nil && bytes.Equal(ja, jb)
}

// apply adds (sign +1) or removes (sign -1) one input's contribution.
func (as *aggState) apply(st *Stage, inKey []byte, c *aggContrib, sign int64) error {
	as.count += sign
	for k := range st.aggSlots {
		s := &st.aggSlots[k]
		if err := applySlot(&as.slots[k], s.fn, s.usesItems(), inKey, c.slots[k], sign, s.itemLess); err != nil {
			return err
		}
	}
	for i := range st.Emit {
		e := &st.Emit[i]
		if e.Expr != "" {
			continue
		}
		fn := legacyFn(e)
		if err := applySlot(&as.emits[i], fn, fn == "collect", inKey, slotContrib{in: c.emits[i].in, val: c.emits[i].val}, sign, collectItemLess); err != nil {
			return err
		}
	}
	return nil
}

// replace swaps one input's contribution for its new one. Per arm it is the
// removal then the addition, except an extreme the input held: it moves to
// the new value when that value is strictly better, or equal and rendered
// the same — the full refold would pick it too — and refolds otherwise.
func (as *aggState) replace(st *Stage, inKey []byte, oc, nc *aggContrib) error {
	for k := range st.aggSlots {
		s := &st.aggSlots[k]
		if err := replaceSlot(&as.slots[k], s.fn, s.usesItems(), inKey, oc.slots[k], nc.slots[k], s.itemLess); err != nil {
			return err
		}
	}
	for i := range st.Emit {
		e := &st.Emit[i]
		if e.Expr != "" {
			continue
		}
		fn := legacyFn(e)
		o := slotContrib{in: oc.emits[i].in, val: oc.emits[i].val}
		n := slotContrib{in: nc.emits[i].in, val: nc.emits[i].val}
		if err := replaceSlot(&as.emits[i], fn, fn == "collect", inKey, o, n, collectItemLess); err != nil {
			return err
		}
	}
	return nil
}

func replaceSlot(ss *slotState, fn string, items bool, inKey []byte, o, n slotContrib, less func(a, b *stateItem) bool) error {
	if !items && (fn == "min" || fn == "max") && o.in && ss.best != nil &&
		!collectLess(o.val, ss.best) && !collectLess(ss.best, o.val) {
		// The input held the extreme (or tied it).
		if !n.in {
			return errAggFallback
		}
		better := collectLess(n.val, ss.best)
		if fn == "max" {
			better = collectLess(ss.best, n.val)
		}
		equalSame := !collectLess(n.val, ss.best) && !collectLess(ss.best, n.val) &&
			sameRendering(n.val, ss.best) && sameRendering(o.val, ss.best)
		if !better && !equalSame {
			return errAggFallback
		}
		ss.best = n.val
		return nil
	}
	if err := applySlot(ss, fn, items, inKey, o, -1, less); err != nil {
		return err
	}
	return applySlot(ss, fn, items, inKey, n, +1, less)
}

// legacyFn names a legacy emit arm's fold.
func legacyFn(e *Emit) string {
	switch {
	case e.Count:
		return "count"
	case e.Collect != "":
		return "collect"
	case e.Min != "":
		return "min"
	case e.Max != "":
		return "max"
	case e.Sum != "":
		return "sum"
	}
	return ""
}

func applySlot(ss *slotState, fn string, items bool, inKey []byte, c slotContrib, sign int64, less func(a, b *stateItem) bool) error {
	if !c.in {
		return nil
	}
	if items {
		if sign > 0 {
			ss.items = insertItem(ss.items, stateItem{inKey: inKey, val: c.val, ord: c.ord}, less)
			return nil
		}
		var ok bool
		if ss.items, ok = removeItem(ss.items, inKey); !ok {
			return errAggFallback
		}
		return nil
	}
	switch fn {
	case "count":
		ss.n += sign
	case "sum", "avg":
		r := c.val.(*big.Rat)
		if ss.sum == nil {
			ss.sum = new(big.Rat)
		}
		if sign > 0 {
			ss.sum.Add(ss.sum, r)
		} else {
			ss.sum.Sub(ss.sum, r)
		}
		ss.n += sign
		if ss.n == 0 {
			ss.sum = nil // no summed values: null, as the full refold leaves it
		}
	case "min", "max":
		v := c.val
		better := func(a, b any) bool { // a strictly beats b
			if fn == "min" {
				return collectLess(a, b)
			}
			return collectLess(b, a)
		}
		equal := ss.best != nil && !collectLess(v, ss.best) && !collectLess(ss.best, v)
		if sign < 0 {
			if ss.best == nil || equal {
				return errAggFallback // the extreme (or a tie with it) left
			}
			ss.n--
			return nil
		}
		ss.n++
		switch {
		case ss.best == nil || better(v, ss.best):
			ss.best = v
		case equal && !sameRendering(v, ss.best):
			return errAggFallback
		}
	}
	return nil
}

// distinctItems keeps, per distinct value, the item first in input-key
// order — the one the full refold's first-appearance rule keeps — in the
// list's order.
func distinctItems(items []stateItem, key func(any) string) []stateItem {
	first := map[string][]byte{}
	for i := range items {
		k := key(items[i].val)
		if cur, ok := first[k]; !ok || bytes.Compare(items[i].inKey, cur) < 0 {
			first[k] = items[i].inKey
		}
	}
	out := make([]stateItem, 0, len(first))
	for i := range items {
		if bytes.Equal(first[key(items[i].val)], items[i].inKey) {
			out = append(out, items[i])
		}
	}
	return out
}

func nullableDistinctKey(v any) string {
	if v == nil {
		return "null|"
	}
	return distinctKey(v)
}

// slotResult is the slot's value from its state, as exprAgg.result computes
// it from a full fold.
func slotResult(s *exprAgg, ss *slotState) any {
	if s.usesItems() {
		items := ss.items
		if s.distinct {
			items = distinctItems(items, nullableDistinctKey)
		}
		switch s.fn {
		case "count":
			return new(big.Rat).SetInt64(int64(len(items)))
		case "sum", "avg":
			if len(items) == 0 {
				return nil
			}
			sum := new(big.Rat)
			for i := range items {
				sum.Add(sum, items[i].val.(*big.Rat))
			}
			if s.fn == "avg" {
				return sum.Quo(sum, new(big.Rat).SetInt64(int64(len(items))))
			}
			return sum
		}
		if len(items) == 0 {
			return nil
		}
		if s.fn == "string_agg" {
			parts := make([]string, len(items))
			for i := range items {
				parts[i] = items[i].val.(string)
			}
			return strings.Join(parts, s.sep)
		}
		vals := make([]any, len(items))
		for i := range items {
			vals[i] = items[i].val
		}
		return vals
	}
	switch s.fn {
	case "count":
		return new(big.Rat).SetInt64(ss.n)
	case "min", "max":
		return ss.best
	case "sum":
		if ss.sum == nil {
			return nil
		}
		return new(big.Rat).Set(ss.sum)
	case "avg":
		if ss.sum == nil || ss.n == 0 {
			return nil
		}
		return new(big.Rat).Quo(ss.sum, new(big.Rat).SetInt64(ss.n))
	}
	return nil
}

// renderState is the key's output from its state: the refold's results,
// having, and emit rendering (aggregateOutput), fed from the state.
func (g *Graph) renderState(st *Stage, as *aggState) ([]byte, any, bool, error) {
	if as.count == 0 {
		return nil, nil, false, nil
	}
	results := make(aggResults, len(st.aggSlots))
	for k := range st.aggSlots {
		results[k] = slotResult(&st.aggSlots[k], &as.slots[k])
	}
	legacy := legacyFolds{
		sums:     make([]*big.Rat, len(st.Emit)),
		mins:     make([]any, len(st.Emit)),
		maxs:     make([]any, len(st.Emit)),
		counts:   make([]int, len(st.Emit)),
		collects: make([][]any, len(st.Emit)),
	}
	for i := range st.Emit {
		e := &st.Emit[i]
		es := &as.emits[i]
		switch {
		case e.Expr != "":
		case e.Count:
			legacy.counts[i] = int(es.n)
		case e.Collect != "":
			items := es.items
			if e.Distinct {
				items = distinctItems(items, collectDistinctKey)
			}
			vals := make([]any, len(items))
			for k := range items {
				vals[k] = items[k].val
			}
			legacy.collects[i] = vals
		case e.Sum != "":
			if es.sum != nil {
				legacy.sums[i] = new(big.Rat).Set(es.sum)
			}
		case e.Min != "":
			legacy.mins[i] = es.best
		case e.Max != "":
			legacy.maxs[i] = es.best
		}
	}
	return g.aggregateOutputObject(st, results, legacy)
}

// refoldAggregateIncremental refolds an aggregate key from its fold state
// and this drain's input changes, or in full when the state cannot show the
// result (see the file comment). Either way the key's state is left in step
// with its retained set.
func (g *Graph) refoldAggregateIncremental(tx *stagestore.Tx, st *Stage, outKey []byte, delta *aggDelta) ([]byte, any, bool, error) {
	n := g.byName[st.Name]
	if delta != nil {
		if out, obj, live, err := g.tryIncremental(tx, st, outKey, delta); err == nil {
			n.foldStateRefolds.Add(1)
			if checkFoldState != nil {
				checkFoldState(g, tx, st, outKey, out, live)
			}
			return out, obj, live, nil
		} else if !errors.Is(err, errAggFallback) {
			return nil, nil, false, err
		}
	}
	n.fullRefolds.Add(1)
	return g.refoldAggregateFull(tx, st, outKey)
}

func (g *Graph) tryIncremental(tx *stagestore.Tx, st *Stage, outKey []byte, delta *aggDelta) ([]byte, any, bool, error) {
	raw, err := tx.GetAcc(st.Name, outKey)
	if err != nil {
		return nil, nil, false, err
	}
	if raw == nil {
		return nil, nil, false, errAggFallback
	}
	as, err := decodeAggState(st, raw)
	if err != nil {
		return nil, nil, false, errAggFallback
	}
	keys := make([]string, 0, len(delta.inputs))
	for k := range delta.inputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ch := delta.inputs[k]
		inKey := []byte(k)
		if bytes.Equal(ch.old, ch.cur) {
			continue // re-retained unchanged (a redelivery): nothing moves
		}
		var oc, nc *aggContrib
		if ch.old != nil {
			if oc, err = g.contribution(tx, st, inKey, ch.old); err != nil {
				return nil, nil, false, errAggFallback
			}
		}
		if ch.cur != nil {
			if ch.curIn != nil {
				nc, err = g.contributionOf(tx, st, inKey, ch.curIn.data, ch.curIn.parent)
			} else {
				nc, err = g.contribution(tx, st, inKey, ch.cur)
			}
			if err != nil {
				return nil, nil, false, errAggFallback
			}
		}
		switch {
		case oc == nil && nc == nil:
			continue // a non-member before and after
		case oc != nil && nc != nil:
			err = as.replace(st, inKey, oc, nc)
		case oc != nil:
			err = as.apply(st, inKey, oc, -1)
		default:
			err = as.apply(st, inKey, nc, +1)
		}
		if err != nil {
			return nil, nil, false, errAggFallback
		}
	}
	if as.count < 0 {
		return nil, nil, false, errAggFallback
	}
	out, obj, live, err := g.renderState(st, as)
	if err != nil {
		return nil, nil, false, errAggFallback // the full refold raises it
	}
	return out, obj, live, g.storeAggState(tx, st, outKey, as)
}

// refoldAggregateFull refolds the key from every retained input and rebuilds
// its state. The output is refoldAggregate's: on any input the state cannot
// account for, the state is dropped and the plain refold decides.
func (g *Graph) refoldAggregateFull(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, any, bool, error) {
	as := newAggState(st)
	ok := true
	err := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		c, err := g.contribution(tx, st, inKey, val)
		if err != nil {
			ok = false
			return errAggFallback
		}
		if c == nil {
			return nil // not a member
		}
		if err := as.apply(st, append([]byte(nil), inKey...), c, +1); err != nil {
			ok = false
			return errAggFallback
		}
		return nil
	})
	if err != nil && !errors.Is(err, errAggFallback) {
		return nil, nil, false, err
	}
	if !ok {
		if err := tx.DeleteAcc(st.Name, outKey); err != nil {
			return nil, nil, false, err
		}
		return g.refoldAggregateNoObject(tx, st, outKey)
	}
	out, obj, live, err := g.renderState(st, as)
	if err != nil {
		if derr := tx.DeleteAcc(st.Name, outKey); derr != nil {
			return nil, nil, false, derr
		}
		return g.refoldAggregateNoObject(tx, st, outKey)
	}
	return out, obj, live, g.storeAggState(tx, st, outKey, as)
}

func (g *Graph) storeAggState(tx *stagestore.Tx, st *Stage, outKey []byte, as *aggState) error {
	if as.count == 0 {
		return tx.DeleteAcc(st.Name, outKey)
	}
	raw, err := encodeAggState(as)
	if err != nil {
		return err
	}
	return tx.PutAcc(st.Name, outKey, raw)
}

// The stored state. A state is written in a compact binary form, read in
// it or in the JSON form earlier releases wrote (a JSON state begins with
// '{', a binary one with stateBinaryV1), so a store carried across an
// upgrade keeps its states. A binary state an earlier release cannot
// read fails its decode there, which refolds that key in full and rewrites
// its state — never a wrong result.

const stateBinaryV1 = 0x01

func encodeAggState(as *aggState) ([]byte, error) {
	b := make([]byte, 0, 64)
	b = append(b, stateBinaryV1)
	b = binary.AppendVarint(b, as.count)
	b = binary.AppendUvarint(b, uint64(len(as.slots)))
	for i := range as.slots {
		b = appendSlot(b, &as.slots[i])
	}
	b = binary.AppendUvarint(b, uint64(len(as.emits)))
	for i := range as.emits {
		b = appendSlot(b, &as.emits[i])
	}
	return b, nil
}

func appendSlot(b []byte, ss *slotState) []byte {
	b = binary.AppendVarint(b, ss.n)
	if ss.sum == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		b = appendRat(b, ss.sum)
	}
	b = appendStateValue(b, ss.best)
	b = binary.AppendUvarint(b, uint64(len(ss.items)))
	for i := range ss.items {
		it := &ss.items[i]
		b = appendBytes(b, it.inKey)
		b = appendStateValue(b, it.val)
		b = binary.AppendUvarint(b, uint64(len(it.ord)))
		for _, o := range it.ord {
			b = appendStateValue(b, o)
		}
	}
	return b
}

// State value tags: the window codec's value set. A value of any other type
// is stored as nil, as the JSON form stores it.
const (
	tagNil byte = iota
	tagRat
	tagString
	tagFalse
	tagTrue
	tagTimestamp
	tagZonedTimestamp
	tagDate
	tagInterval
)

func appendStateValue(b []byte, v any) []byte {
	switch t := v.(type) {
	case *big.Rat:
		return appendRat(append(b, tagRat), t)
	case string:
		return appendBytes(append(b, tagString), []byte(t))
	case bool:
		if t {
			return append(b, tagTrue)
		}
		return append(b, tagFalse)
	case timestampVal:
		tag := tagTimestamp
		if t.zoned {
			tag = tagZonedTimestamp
		}
		return binary.AppendVarint(append(b, tag), t.us)
	case dateVal:
		return binary.AppendVarint(append(b, tagDate), t.days)
	case intervalVal:
		b = binary.AppendVarint(append(b, tagInterval), t.months)
		b = binary.AppendVarint(b, t.days)
		return binary.AppendVarint(b, t.us)
	}
	return append(b, tagNil)
}

// appendRat writes a rational as its sign and the bytes of its numerator's
// magnitude and its denominator.
func appendRat(b []byte, r *big.Rat) []byte {
	sign := byte(0)
	if r.Sign() < 0 {
		sign = 1
	}
	b = append(b, sign)
	b = appendBytes(b, r.Num().Bytes())
	return appendBytes(b, r.Denom().Bytes())
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

// stateReader reads a binary state; the first malformed read sticks as err.
type stateReader struct {
	b   []byte
	err error
}

var errBadState = errors.New("malformed fold state")

func (r *stateReader) fail() {
	if r.err == nil {
		r.err = errBadState
	}
	r.b = nil
}

func (r *stateReader) byte() byte {
	if len(r.b) == 0 {
		r.fail()
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *stateReader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *stateReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

// count reads a length no larger than the bytes left, so a corrupt length
// cannot ask for a huge allocation.
func (r *stateReader) count() int {
	v := r.uvarint()
	if v > uint64(len(r.b)) {
		r.fail()
		return 0
	}
	return int(v) //nolint:gosec // G115: v is at most len(r.b)
}

func (r *stateReader) bytes() []byte {
	n := r.count()
	if r.err != nil {
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func (r *stateReader) rat() *big.Rat {
	sign := r.byte()
	num := new(big.Int).SetBytes(r.bytes())
	den := new(big.Int).SetBytes(r.bytes())
	if r.err != nil || sign > 1 || den.Sign() == 0 {
		r.fail()
		return nil
	}
	if sign == 1 {
		num.Neg(num)
	}
	return new(big.Rat).SetFrac(num, den)
}

func (r *stateReader) value() any {
	switch r.byte() {
	case tagNil:
		return nil
	case tagRat:
		return r.rat()
	case tagString:
		return string(r.bytes())
	case tagFalse:
		return false
	case tagTrue:
		return true
	case tagTimestamp:
		return timestampVal{us: r.varint()}
	case tagZonedTimestamp:
		return timestampVal{us: r.varint(), zoned: true}
	case tagDate:
		return dateVal{days: r.varint()}
	case tagInterval:
		months := r.varint()
		days := r.varint()
		return intervalVal{months: months, days: days, us: r.varint()}
	}
	r.fail()
	return nil
}

func (r *stateReader) slot() slotState {
	ss := slotState{n: r.varint()}
	switch r.byte() {
	case 0:
	case 1:
		ss.sum = r.rat()
	default:
		r.fail()
	}
	ss.best = r.value()
	items := r.count()
	for range items {
		if r.err != nil {
			break
		}
		it := stateItem{inKey: r.bytes(), val: r.value()}
		if n := r.count(); n > 0 {
			it.ord = make([]any, n)
			for i := range it.ord {
				it.ord[i] = r.value()
			}
		}
		ss.items = append(ss.items, it)
	}
	return ss
}

func (r *stateReader) slots() []slotState {
	out := make([]slotState, r.count())
	for i := range out {
		out[i] = r.slot()
	}
	return out
}

// parseAggState reads a binary state without a stage to check it against.
func parseAggState(raw []byte) (*aggState, error) {
	if len(raw) == 0 || raw[0] != stateBinaryV1 {
		return nil, errBadState
	}
	r := &stateReader{b: raw[1:]}
	as := &aggState{count: r.varint()}
	as.slots = r.slots()
	as.emits = r.slots()
	if r.err == nil && len(r.b) != 0 {
		r.fail()
	}
	if r.err != nil {
		return nil, r.err
	}
	return as, nil
}

func decodeAggState(st *Stage, raw []byte) (*aggState, error) {
	if len(raw) > 0 && raw[0] == '{' {
		return decodeAggStateJSON(st, raw)
	}
	as, err := parseAggState(raw)
	if err != nil {
		return nil, err
	}
	if len(as.slots) != len(st.aggSlots) || len(as.emits) != len(st.Emit) {
		return nil, fmt.Errorf("state shape does not match the stage")
	}
	for i := range as.slots {
		for _, it := range as.slots[i].items {
			if len(it.ord) != len(st.aggSlots[i].order) {
				return nil, fmt.Errorf("item has %d order keys, want %d", len(it.ord), len(st.aggSlots[i].order))
			}
		}
	}
	for i := range as.emits {
		for _, it := range as.emits[i].items {
			if len(it.ord) != 0 {
				return nil, fmt.Errorf("item has %d order keys, want 0", len(it.ord))
			}
		}
	}
	return as, nil
}

// The JSON form earlier releases wrote: values in the window codec's typed
// tags. Read only here; encodeAggStateJSON renders a state in it for the
// store format reference, which pins a state's contents in this form.

type storedItem struct {
	K []byte `json:"k"`
	V any    `json:"v"`
	O []any  `json:"o,omitempty"`
}

type storedSlot struct {
	N     int64        `json:"n,omitempty"`
	Sum   string       `json:"s,omitempty"`
	Best  any          `json:"b,omitempty"`
	Items []storedItem `json:"i,omitempty"`
}

type storedState struct {
	Count int64        `json:"c"`
	Slots []storedSlot `json:"a"`
	Emits []storedSlot `json:"e"`
}

func encodeSlot(ss *slotState) storedSlot {
	out := storedSlot{N: ss.n, Best: encodeStateValue(ss.best)}
	if ss.sum != nil {
		out.Sum = ss.sum.RatString()
	}
	for i := range ss.items {
		it := &ss.items[i]
		si := storedItem{K: it.inKey, V: encodeStateValue(it.val)}
		for _, o := range it.ord {
			si.O = append(si.O, encodeStateValue(o))
		}
		if len(it.ord) > 0 && si.O == nil {
			si.O = []any{}
		}
		out.Items = append(out.Items, si)
	}
	return out
}

// encodeStateValue tags a value; nil stays nil.
func encodeStateValue(v any) any {
	if v == nil {
		return nil
	}
	return encodeWinValue(v)
}

func encodeAggStateJSON(as *aggState) ([]byte, error) {
	s := storedState{Count: as.count, Slots: make([]storedSlot, len(as.slots)), Emits: make([]storedSlot, len(as.emits))}
	for i := range as.slots {
		s.Slots[i] = encodeSlot(&as.slots[i])
	}
	for i := range as.emits {
		s.Emits[i] = encodeSlot(&as.emits[i])
	}
	return json.Marshal(s)
}

func decodeSlot(s storedSlot, nOrd int) (slotState, error) {
	ss := slotState{n: s.N}
	if s.Sum != "" {
		r, ok := new(big.Rat).SetString(s.Sum)
		if !ok {
			return ss, fmt.Errorf("bad sum %q", s.Sum)
		}
		ss.sum = r
	}
	var err error
	if ss.best, err = decodeStateValue(s.Best); err != nil {
		return ss, err
	}
	for _, si := range s.Items {
		it := stateItem{inKey: si.K, ord: make([]any, nOrd)}
		if it.val, err = decodeStateValue(si.V); err != nil {
			return ss, err
		}
		if len(si.O) != nOrd {
			return ss, fmt.Errorf("item has %d order keys, want %d", len(si.O), nOrd)
		}
		for i, o := range si.O {
			if it.ord[i], err = decodeStateValue(o); err != nil {
				return ss, err
			}
		}
		ss.items = append(ss.items, it)
	}
	return ss, nil
}

func decodeStateValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return decodeWinValue(v)
}

func decodeAggStateJSON(st *Stage, raw []byte) (*aggState, error) {
	var s storedState
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	if len(s.Slots) != len(st.aggSlots) || len(s.Emits) != len(st.Emit) {
		return nil, fmt.Errorf("state shape does not match the stage")
	}
	as := &aggState{count: s.Count, slots: make([]slotState, len(s.Slots)), emits: make([]slotState, len(s.Emits))}
	var err error
	for i := range s.Slots {
		if as.slots[i], err = decodeSlot(s.Slots[i], len(st.aggSlots[i].order)); err != nil {
			return nil, err
		}
	}
	for i := range s.Emits {
		if as.emits[i], err = decodeSlot(s.Emits[i], 0); err != nil {
			return nil, err
		}
	}
	return as, nil
}

// refoldAggregateNoObject is refoldAggregate in refoldAggregateFull's shape.
func (g *Graph) refoldAggregateNoObject(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, any, bool, error) {
	out, live, err := g.refoldAggregate(tx, st, outKey)
	return out, nil, live, err
}
