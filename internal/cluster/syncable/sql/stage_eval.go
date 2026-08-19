package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The stage evaluator: the steady-state per-key refold over the stage
// store. Every stage is the same machine — an input arrives (a topic
// entity, or an upstream stage's output), lands in the key's retained
// input set, and the key REFOLDS from that set: never delta arithmetic,
// always recompute-from-inputs, so redelivery, rekeying, retraction, and
// replay all converge to the same bytes (log-prefix determinism).
//
// Identity model (the design note the store's src bucket exists for):
//   - inKey  — the input's identity: a topic entity's Key, or the
//     producing stage's output key.
//   - outKey — the stage's key for that input: keyPath over the input
//     object, canonically rendered.
//   - src: inKey → outKey answers deletes (a tombstone carries only
//     inKey) and rekeys (a re-emitted input whose keyPath moved must
//     retract from its OLD key's fold).
//
// A stage's output object marshals with sorted keys (Go map marshal) and
// exact numbers (json.Number in, formatRat out), so equal folds are
// byte-equal — the determinism the self-healing arguments lean on.

// stageNode is one stage's runtime: its definition and the downstream
// stages consuming its output.
type stageNode struct {
	def       *ProjectionStage
	consumers []*stageNode
}

// stageGraph is the compiled stage topology of one projection: manifest
// order is the DAG (validated), so cascades simply recurse downstream.
// dimRef addresses one join of one stage — a topic's entities feed it as
// dimension rows.
type dimRef struct {
	node *stageNode
	join *StageJoin
}

type stageGraph struct {
	byName  map[string]*stageNode
	byTopic map[string][]*stageNode
	// order is manifest order — the validated topology, and Drain's
	// processing order: a stage's consumers always drain after it.
	order []*stageNode
	// dims routes topics consumed as JOIN DIMENSIONS: rows stored by
	// entity key, dependents refolded on change (reverse-index fan-out).
	dims map[string][]dimRef
	// onDelta, when set, observes every stage's post-refold delta — the
	// chaining terminal's feed (table sources consuming a stage). live=false
	// is a retraction. Set per Sync call by the single worker goroutine.
	onDelta func(stage string, outKey []byte, obj any, live bool) error
}

// buildStageGraph compiles the validated stage list into its runtime
// topology.
func buildStageGraph(stages []ProjectionStage) *stageGraph {
	g := &stageGraph{byName: map[string]*stageNode{}, byTopic: map[string][]*stageNode{}, dims: map[string][]dimRef{}}
	nodes := make([]*stageNode, len(stages))
	for i := range stages {
		nodes[i] = &stageNode{def: &stages[i]}
		g.byName[stages[i].Name] = nodes[i]
	}
	g.order = nodes
	for i := range stages {
		if up, ok := g.byName[stages[i].From]; ok {
			up.consumers = append(up.consumers, nodes[i])
		} else {
			g.byTopic[stages[i].From] = append(g.byTopic[stages[i].From], nodes[i])
		}
		for j := range stages[i].Joins {
			jn := &stages[i].Joins[j]
			g.dims[jn.Topic] = append(g.dims[jn.Topic], dimRef{node: nodes[i], join: jn})
		}
	}
	return g
}

// ConsumesTopic reports whether any stage reads this topic — as its
// input or as a join dimension.
func (g *stageGraph) ConsumesTopic(topic string) bool {
	return len(g.byTopic[topic]) > 0 || len(g.dims[topic]) > 0
}

// dirtySet collects the keys each stage must refold — the set-at-a-time
// half of the two execution modes: retention updates mark keys dirty,
// and Drain refolds each dirty key ONCE per batch (the 87-minutes-vs-
// 5-seconds field lesson), suppressing cascades whose output bytes did
// not change (sound because folds are deterministic).
type dirtySet map[*stageNode]map[string]bool

func (d dirtySet) mark(n *stageNode, outKey []byte) {
	m, ok := d[n]
	if !ok {
		m = map[string]bool{}
		d[n] = m
	}
	m[string(outKey)] = true
}

// FoldTopicUpsertNow folds one entity and drains immediately — the
// single-event convenience (tests; the projection batches per Actual).
func (g *stageGraph) FoldTopicUpsertNow(tx *stagestore.Tx, topic string, inKey []byte, payload any) error {
	dirty := dirtySet{}
	if err := g.FoldTopicUpsert(tx, topic, inKey, payload, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicDeleteNow is FoldTopicUpsertNow's tombstone twin.
func (g *stageGraph) FoldTopicDeleteNow(tx *stagestore.Tx, topic string, inKey []byte) error {
	dirty := dirtySet{}
	if err := g.FoldTopicDelete(tx, topic, inKey, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicUpsert routes one topic entity's upsert through every stage
// consuming that topic (and their downstream chains).
func (g *stageGraph) FoldTopicUpsert(tx *stagestore.Tx, topic string, inKey []byte, payload any, dirty dirtySet) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldUpsert(tx, n, inKey, payload, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		bs, err := marshalStageObject(payload)
		if err != nil {
			return err
		}
		if err := tx.PutDim(d.node.def.Name, d.join.Topic, inKey, bs); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, inKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// FoldTopicDelete routes one topic tombstone (key only, no payload).
func (g *stageGraph) FoldTopicDelete(tx *stagestore.Tx, topic string, inKey []byte, dirty dirtySet) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldDelete(tx, n, inKey, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		if err := tx.DeleteDim(d.node.def.Name, d.join.Topic, inKey); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, inKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// markDimDependents marks every key whose inputs reference this
// dimension row (the fan-out) for refold at Drain. Reverse-index entries
// can be stale (an input rekeyed or retracted since registering) — a
// stale entry's refold is an idempotent no-op, never wrong output; a
// rebuild clears them.
func (g *stageGraph) markDimDependents(tx *stagestore.Tx, d dimRef, dimKey []byte, dirty dirtySet) error {
	return tx.DependentsOf(d.node.def.Name, d.join.Topic, dimKey, func(outKey []byte) error {
		dirty.mark(d.node, outKey)
		return nil
	})
}

// foldUpsert lands one input in a stage: filter, key, retain, refold the
// affected key(s), cascade the delta. An input that stops matching the
// stage's when RETRACTS (filtering is refold, not skip).
func (g *stageGraph) foldUpsert(tx *stagestore.Tx, n *stageNode, inKey []byte, payload any, dirty dirtySet) error {
	st := n.def
	if !matchWhen(st.When, payload) {
		return g.foldDelete(tx, n, inKey, dirty)
	}

	kv, err := resolveScopedPath(st.KeyPath[0], payload, nil)
	if err != nil || kv == nil {
		// A matched input without a key cannot fold; treat as non-membership
		// (and retract any prior membership) rather than erroring the topic.
		return g.foldDelete(tx, n, inKey, dirty)
	}
	outKey := []byte(keyString(coerceKeyScalar(kv)))

	// Rekey: if this input previously fed a DIFFERENT key, retract it
	// there first — its old key refolds without it.
	prior, err := tx.GetSrc(st.Name, inKey)
	if err != nil {
		return err
	}
	if prior != nil && !bytes.Equal(prior, outKey) {
		if err := tx.DeleteIn(st.Name, prior, inKey); err != nil {
			return err
		}
		dirty.mark(n, prior)
	}

	// Retain the input as the stage sees it (the refold working set).
	inputBytes, err := marshalStageObject(payload)
	if err != nil {
		return fmt.Errorf("stage %q: retain input: %w", st.Name, err)
	}
	if err := tx.PutIn(st.Name, outKey, inKey, inputBytes); err != nil {
		return err
	}
	if err := tx.PutSrc(st.Name, inKey, outKey); err != nil {
		return err
	}
	for i := range st.Joins {
		j := &st.Joins[i]
		if onV, err := resolveScopedPath(j.On, payload, nil); err == nil && onV != nil {
			if err := tx.PutRev(st.Name, j.Topic, []byte(keyString(onV)), outKey); err != nil {
				return err
			}
		}
	}
	dirty.mark(n, outKey)
	return nil
}

// foldDelete retracts one input from a stage: the key it fed refolds
// without it (possibly to deletion), and the delta cascades.
func (g *stageGraph) foldDelete(tx *stagestore.Tx, n *stageNode, inKey []byte, dirty dirtySet) error {
	st := n.def
	outKey, err := tx.GetSrc(st.Name, inKey)
	if err != nil || outKey == nil {
		return err // never fed this stage — a no-op, not an error
	}
	if err := tx.DeleteIn(st.Name, outKey, inKey); err != nil {
		return err
	}
	if err := tx.DeleteSrc(st.Name, inKey); err != nil {
		return err
	}
	dirty.mark(n, outKey)
	return nil
}

// Drain refolds every dirty key exactly once, in stage topology order
// (a stage's consumers drain after it — retention marks they receive
// here are processed later in the same pass), in sorted key order for
// determinism. A refold whose output bytes did not change is SUPPRESSED
// — no store write, no delta, no cascade — which determinism makes
// sound and which turns a batch touching one key N times into one
// refold and at most one downstream write.
func (g *stageGraph) Drain(tx *stagestore.Tx, dirty dirtySet) error {
	for _, n := range g.order {
		keys := dirty[n]
		if len(keys) == 0 {
			continue
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			if err := g.refoldKey(tx, n, []byte(k), dirty); err != nil {
				return err
			}
		}
	}
	return nil
}

// refoldKey recomputes one key's output from its retained input set,
// stores it, and — when the bytes changed — cascades the delta (upsert
// or retraction) to sinks and consumer retention.
func (g *stageGraph) refoldKey(tx *stagestore.Tx, n *stageNode, outKey []byte, dirty dirtySet) error {
	st := n.def
	prior, err := tx.GetOut(st.Name, outKey)
	if err != nil {
		return err
	}
	out, live, err := refoldOutput(tx, st, outKey)
	if err != nil {
		return err
	}
	if !live {
		if prior == nil {
			return nil // was absent, still absent — nothing to say
		}
		if err := tx.DeleteOut(st.Name, outKey); err != nil {
			return err
		}
		if g.onDelta != nil {
			if err := g.onDelta(st.Name, outKey, nil, false); err != nil {
				return err
			}
		}
		for _, c := range n.consumers {
			if err := g.foldDelete(tx, c, outKey, dirty); err != nil {
				return err
			}
		}
		return nil
	}
	if prior != nil && bytes.Equal(prior, out) {
		return nil // unchanged — suppressed (deterministic bytes make this sound)
	}
	if err := tx.PutOut(st.Name, outKey, out); err != nil {
		return err
	}
	obj, err := decodeStageObject(out)
	if err != nil {
		return fmt.Errorf("stage %q: decode own output: %w", st.Name, err)
	}
	if g.onDelta != nil {
		if err := g.onDelta(st.Name, outKey, obj, true); err != nil {
			return err
		}
	}
	for _, c := range n.consumers {
		if err := g.foldUpsert(tx, c, outKey, obj, dirty); err != nil {
			return err
		}
	}
	return nil
}

// refoldOutput computes a key's output object from its retained inputs.
// live=false means the key has no qualifying inputs and its output (and
// downstream memberships) retract.
func refoldOutput(tx *stagestore.Tx, st *ProjectionStage, outKey []byte) (out []byte, live bool, err error) {
	if st.Reduce == "aggregate" {
		return refoldAggregate(tx, st, outKey)
	}
	if st.Reduce == "latest" {
		return refoldLatest(tx, st, outKey)
	}
	// Reshape: one output object per key — deterministically the emitted
	// object of the bytewise-LARGEST input identity (real reshape inputs
	// are 1:1 with keys; the tiebreak only decides pathological
	// collisions, deterministically).
	var winner any
	scanErr := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		obj, err := decodeStageObject(val)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, obj); err != nil || !ok {
			return err
		}
		winner = obj
		return nil
	})
	if scanErr != nil || winner == nil {
		return nil, false, scanErr
	}
	return emitReshape(st, winner)
}

// inputQualifies applies a stage's filtering joins to one input: it
// participates only while EVERY join's dimension row — addressed by the
// input's on value — exists and matches the join's where. A dimension
// that has not arrived yet fails participation and heals when it lands
// (the fan-out refolds dependents).
func inputQualifies(tx *stagestore.Tx, st *ProjectionStage, obj any) (bool, error) {
	for i := range st.Joins {
		j := &st.Joins[i]
		onV, err := resolveScopedPath(j.On, obj, nil)
		if err != nil || onV == nil {
			return false, nil
		}
		payload, err := tx.GetDim(st.Name, j.Topic, []byte(keyString(onV)))
		if err != nil {
			return false, err
		}
		if payload == nil {
			return false, nil
		}
		dimObj, err := decodeStageObject(payload)
		if err != nil {
			return false, err
		}
		if !matchWhen(j.Where, dimObj) {
			return false, nil
		}
	}
	return true, nil
}

// emitReshape renders a stage's emit fields from one input object — the
// reshape path, and the winner path of reduce = "latest".
func emitReshape(st *ProjectionStage, obj any) ([]byte, bool, error) {
	emitted := make(map[string]any, len(st.Emit))
	var err error
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		if e.From != "" {
			if v, err = resolveScopedPath(e.From, obj, nil); err != nil {
				v = nil // a missing field is null, per the language
			}
		} else {
			if v, err = evalExpr(e.compiled, obj, nil); err != nil {
				return nil, false, fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err)
			}
		}
		emitted[e.Field], err = stageValue(v)
		if err != nil {
			return nil, false, fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err)
		}
	}
	bs, err := marshalStageObject(emitted)
	return bs, true, err
}

// refoldLatest recomputes an argmax key: the winner is the retained input
// with the greatest OrderBy value — a BUSINESS field, never arrival order,
// so backfills in keyset order and steady-state in log order converge —
// with TieBy the mandatory deterministic tiebreak. The stage's `when`
// filtered inputs before they were retained, so the argmax runs over the
// qualifying set only (an unapproved newer input never shadows an
// approved older one). The winner's object then emits like a reshape.
func refoldLatest(tx *stagestore.Tx, st *ProjectionStage, outKey []byte) ([]byte, bool, error) {
	type ranked struct {
		order, tie any
		obj        any
	}
	var winner *ranked
	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		obj, err := decodeStageObject(val)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, obj); err != nil || !ok {
			return err
		}
		ov, _ := resolveScopedPath(st.OrderBy, obj, nil)
		tv, _ := resolveScopedPath(st.TieBy, obj, nil)
		cand := &ranked{order: ov, tie: tv, obj: obj}
		if winner == nil {
			winner = cand
			return nil
		}
		if c := compareStageValues(cand.order, winner.order, st.OrderByType == elementKeyTypeNumber); c > 0 {
			winner = cand
		} else if c == 0 {
			if compareStageValues(cand.tie, winner.tie, st.TieByType == elementKeyTypeNumber) > 0 {
				winner = cand
			}
		}
		return nil
	})
	if err != nil || winner == nil {
		return nil, false, err
	}
	return emitReshape(st, winner.obj)
}

// compareStageValues orders two payload values for argmax: nulls sort
// lowest (a missing business timestamp never wins), then numeric or
// lexical per the declared type. In the numeric mode an unparsable value
// ranks with the nulls — below every real number, deterministically.
func compareStageValues(a, b any, numeric bool) int {
	ar, aOK := stageComparable(a, numeric)
	br, bOK := stageComparable(b, numeric)
	if !aOK || !bOK {
		switch {
		case aOK:
			return 1
		case bOK:
			return -1
		default:
			return 0
		}
	}
	if numeric {
		return ar.(*big.Rat).Cmp(br.(*big.Rat))
	}
	as, bs := ar.(string), br.(string)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func stageComparable(v any, numeric bool) (any, bool) {
	if v == nil {
		return nil, false
	}
	if numeric {
		switch t := v.(type) {
		case json.Number:
			if r, ok := new(big.Rat).SetString(string(t)); ok {
				return r, true
			}
		case string:
			if r, ok := new(big.Rat).SetString(t); ok {
				return r, true
			}
		}
		return nil, false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return string(t), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	}
	return nil, false
}

// refoldAggregate recomputes an aggregate key: every fold arm evaluates
// over the FULL retained input set (recompute, never increment). SQL
// empty-set semantics: no inputs → the key retracts entirely; a null arm
// operand skips that input for that arm (sum/min/max of nothing is null,
// count counts rows).
func refoldAggregate(tx *stagestore.Tx, st *ProjectionStage, outKey []byte) ([]byte, bool, error) {
	sums := make([]*big.Rat, len(st.Emit))
	mins := make([]*big.Rat, len(st.Emit))
	maxs := make([]*big.Rat, len(st.Emit))
	count := 0

	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		obj, err := decodeStageObject(val)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, obj); err != nil || !ok {
			return err
		}
		count++
		for i := range st.Emit {
			e := &st.Emit[i]
			if e.Count {
				continue
			}
			v, err := evalExpr(e.compiled, obj, nil)
			if err != nil {
				return fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err)
			}
			r, ok := v.(*big.Rat)
			if !ok {
				continue // null (or non-numeric passthrough) skips, like SQL
			}
			switch {
			case e.Sum != "":
				if sums[i] == nil {
					sums[i] = new(big.Rat)
				}
				sums[i].Add(sums[i], r)
			case e.Min != "":
				if mins[i] == nil || r.Cmp(mins[i]) < 0 {
					mins[i] = r
				}
			case e.Max != "":
				if maxs[i] == nil || r.Cmp(maxs[i]) > 0 {
					maxs[i] = r
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	emitted := make(map[string]any, len(st.Emit))
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		switch {
		case e.Count:
			v = json.Number(fmt.Sprintf("%d", count))
		case e.Sum != "":
			if sums[i] != nil {
				v = sums[i]
			}
		case e.Min != "":
			if mins[i] != nil {
				v = mins[i]
			}
		case e.Max != "":
			if maxs[i] != nil {
				v = maxs[i]
			}
		}
		emitted[e.Field], err = stageValue(v)
		if err != nil {
			return nil, false, fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err)
		}
	}
	bs, err := marshalStageObject(emitted)
	return bs, true, err
}

// stageValue renders an evaluator value into its stored JSON form:
// rationals become exact minimal-decimal json.Numbers, scalars pass
// through, null stays null.
func stageValue(v any) (any, error) {
	if r, ok := v.(*big.Rat); ok {
		text, err := formatRat(r)
		if err != nil {
			return nil, err
		}
		return json.Number(text), nil
	}
	return v, nil
}

// marshalStageObject renders a stage object deterministically: Go maps
// marshal with sorted keys, and json.Number values keep their exact
// digits — equal folds are byte-equal.
func marshalStageObject(obj any) ([]byte, error) { return json.Marshal(obj) }

// decodeStageObject decodes with UseNumber so exact digits survive the
// store round-trip.
func decodeStageObject(bs []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// coerceKeyScalar renders a key path value into the canonical key space
// (numbers keep source digits; strings pass through).
func coerceKeyScalar(v any) any { return v }
