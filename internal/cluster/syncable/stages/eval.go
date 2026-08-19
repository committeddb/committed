package stages

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/PaesslerAG/jsonpath"

	"github.com/committeddb/committed/internal/cluster"
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
// exact numbers (json.Number in, FormatRat out), so equal folds are
// byte-equal — the determinism the self-healing arguments lean on.

// graphNode is one stage's runtime: its definition and the downstream
// stages consuming its output.
type graphNode struct {
	def       *Stage
	consumers []*graphNode
	// dimConsumers are the joins (of LATER stages) that use THIS stage's
	// outputs as their dimension rows — maintained by refoldKey as the
	// outputs change, fan-out marking their dependents dirty.
	dimConsumers []dimRef
}

// Graph is the compiled stage topology of one projection: manifest
// order is the DAG (validated), so cascades simply recurse downstream.
// dimRef addresses one join of one stage — a topic's entities feed it as
// dimension rows.
type dimRef struct {
	node *graphNode
	join *Join
}

type Graph struct {
	byName  map[string]*graphNode
	byTopic map[string][]*graphNode
	// order is manifest order — the validated topology, and Drain's
	// processing order: a stage's consumers always drain after it.
	order []*graphNode
	// dims routes topics consumed as JOIN DIMENSIONS: rows stored by
	// entity key, dependents refolded on change (reverse-index fan-out).
	dims map[string][]dimRef
	// OnDelta, when set, observes every stage's post-refold delta — the
	// chaining terminal's feed (table sources consuming a stage). live=false
	// is a retraction. Set per Sync call by the single worker goroutine.
	OnDelta func(stage string, outKey []byte, obj any, live bool) error
}

// BuildGraph compiles the validated stage list into its runtime
// topology.
func BuildGraph(stages []Stage) *Graph {
	g := &Graph{byName: map[string]*graphNode{}, byTopic: map[string][]*graphNode{}, dims: map[string][]dimRef{}}
	nodes := make([]*graphNode, len(stages))
	for i := range stages {
		nodes[i] = &graphNode{def: &stages[i]}
		g.byName[stages[i].Name] = nodes[i]
	}
	g.order = nodes
	for i := range stages {
		st := &stages[i]
		if up, ok := g.byName[st.From]; ok {
			up.consumers = append(up.consumers, nodes[i])
		} else {
			g.byTopic[st.From] = append(g.byTopic[st.From], nodes[i])
		}
		for j := range st.Joins {
			jn := &st.Joins[j]
			if jn.From != "" {
				if producer, ok := g.byName[jn.From]; ok {
					producer.dimConsumers = append(producer.dimConsumers, dimRef{node: nodes[i], join: jn})
				}
				continue
			}
			g.dims[jn.Topic] = append(g.dims[jn.Topic], dimRef{node: nodes[i], join: jn})
		}
	}
	return g
}

// ConsumesTopic reports whether any stage reads this topic — as its
// input or as a join dimension.
func (g *Graph) ConsumesTopic(topic string) bool {
	return len(g.byTopic[topic]) > 0 || len(g.dims[topic]) > 0
}

// SweepEpochs applies a refresh-boundary marker for one topic: retained
// inputs and dimension rows captured under an OLDER refresh epoch
// (1 <= gen < marker) were not re-asserted by the re-snapshot — the
// source deleted them in the lost window — so they retract, and every
// affected key refolds through the drain as EXPLICIT deltas. Generation
// 0 (direct writes, stage-fed retention) is never swept: the >= 1 floor,
// and the absorption rule — downstream stages see deltas, never sweeps.
func (g *Graph) SweepEpochs(tx *stagestore.Tx, topic string, marker uint64, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		st := n.def
		type stale struct{ outKey, inKey []byte }
		var stales []stale
		if err := tx.InputsAll(st.Name, func(outKey, inKey, val []byte) error {
			if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
				stales = append(stales, stale{append([]byte(nil), outKey...), append([]byte(nil), inKey...)})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, sl := range stales {
			if err := tx.DeleteIn(st.Name, sl.outKey, sl.inKey); err != nil {
				return err
			}
			if err := tx.DeleteSrc(st.Name, sl.inKey); err != nil {
				return err
			}
			dirty.Mark(n, sl.outKey)
		}
	}
	for _, d := range g.dims[topic] {
		var staleDims [][]byte
		if err := tx.DimsAll(d.node.def.Name, d.join.Topic, func(dimKey, val []byte) error {
			if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
				staleDims = append(staleDims, append([]byte(nil), dimKey...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, dk := range staleDims {
			if err := tx.DeleteDim(d.node.def.Name, d.join.Topic, dk); err != nil {
				return err
			}
			if err := g.markDimDependents(tx, d, dk, dirty); err != nil {
				return err
			}
		}
	}
	return nil
}

// Dirty collects the keys each stage must refold — the set-at-a-time
// half of the two execution modes: retention updates mark keys dirty,
// and Drain refolds each dirty key ONCE per batch (the 87-minutes-vs-
// 5-seconds field lesson), suppressing cascades whose output bytes did
// not change (sound because folds are deterministic).
type Dirty map[*graphNode]map[string]bool

func (d Dirty) Mark(n *graphNode, outKey []byte) {
	m, ok := d[n]
	if !ok {
		m = map[string]bool{}
		d[n] = m
	}
	m[string(outKey)] = true
}

// FoldTopicUpsertNow folds one entity and drains immediately — the
// single-event convenience (tests; the projection batches per Actual).
func (g *Graph) FoldTopicUpsertNow(tx *stagestore.Tx, topic string, inKey []byte, payload any) error {
	dirty := Dirty{}
	if err := g.FoldTopicUpsert(tx, topic, inKey, payload, 0, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicDeleteNow is FoldTopicUpsertNow's tombstone twin.
func (g *Graph) FoldTopicDeleteNow(tx *stagestore.Tx, topic string, inKey []byte) error {
	dirty := Dirty{}
	if err := g.FoldTopicDelete(tx, topic, inKey, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicUpsert routes one topic entity's upsert through every stage
// consuming that topic (and their downstream chains).
func (g *Graph) FoldTopicUpsert(tx *stagestore.Tx, topic string, inKey []byte, payload any, gen uint64, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldUpsert(tx, n, inKey, payload, gen, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		bs, err := marshalStageObject(payload)
		if err != nil {
			return err
		}
		if err := tx.PutDim(d.node.def.Name, d.join.target(), inKey, encodeRetained(gen, bs)); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, inKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// FoldTopicDelete routes one topic tombstone (key only, no payload).
func (g *Graph) FoldTopicDelete(tx *stagestore.Tx, topic string, inKey []byte, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldDelete(tx, n, inKey, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		if err := tx.DeleteDim(d.node.def.Name, d.join.target(), inKey); err != nil {
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
func (g *Graph) markDimDependents(tx *stagestore.Tx, d dimRef, dimKey []byte, dirty Dirty) error {
	return tx.DependentsOf(d.node.def.Name, d.join.target(), dimKey, func(outKey []byte) error {
		dirty.Mark(d.node, outKey)
		return nil
	})
}

// foldUpsert lands one input in a stage: filter, key, retain, refold the
// affected key(s), cascade the delta. An input that stops matching the
// stage's when RETRACTS (filtering is refold, not skip).
func (g *Graph) foldUpsert(tx *stagestore.Tx, n *graphNode, inKey []byte, payload any, gen uint64, dirty Dirty) error {
	if n.def.ForEach != "" {
		return g.foldFan(tx, n, inKey, payload, gen, dirty)
	}
	return g.foldOneInput(tx, n, inKey, payload, nil, gen, dirty)
}

// foldFan fans one input through a forEach stage: each element folds as
// its own input — identity (parent key, element key) — and the fan
// reverse-index reconciles: elements the re-emitted input no longer
// carries retract, and the input's tombstone retracts them all.
func (g *Graph) foldFan(tx *stagestore.Tx, n *graphNode, parentKey []byte, payload any, gen uint64, dirty Dirty) error {
	st := n.def
	var elems []any
	if v, err := jsonpath.Get(st.ForEach, payload); err == nil {
		if list, ok := v.([]any); ok {
			elems = list
		}
	}

	prior := map[string]bool{}
	if err := tx.DependentsOf(st.Name, fanJoinName, parentKey, func(elemIn []byte) error {
		prior[string(elemIn)] = true
		return nil
	}); err != nil {
		return err
	}

	identityPath := st.ElementKey
	if identityPath == "" {
		identityPath = st.KeyPath[0]
	}
	current := map[string]bool{}
	for _, el := range elems {
		kv, err := ResolvePath(identityPath, el, payload)
		if err != nil || kv == nil {
			continue // an unidentifiable element fans nothing (and reconciles away)
		}
		elemIn := fanInKey(parentKey, []byte(KeyString(kv)))
		if current[string(elemIn)] {
			continue
		}
		current[string(elemIn)] = true
		if err := g.foldOneInput(tx, n, elemIn, el, payload, gen, dirty); err != nil {
			return err
		}
		if err := tx.PutRev(st.Name, fanJoinName, parentKey, elemIn); err != nil {
			return err
		}
	}
	for pk := range prior {
		if current[pk] {
			continue
		}
		if err := g.foldDeleteInput(tx, n, []byte(pk), dirty); err != nil {
			return err
		}
		if err := tx.DeleteRev(st.Name, fanJoinName, parentKey, []byte(pk)); err != nil {
			return err
		}
	}
	return nil
}

// foldOneInput lands one input (an entity, an upstream delta, or a
// fanned element with its parent in scope) in a stage's retained set.
func (g *Graph) foldOneInput(tx *stagestore.Tx, n *graphNode, inKey []byte, data, parent any, gen uint64, dirty Dirty) error {
	st := n.def
	deleteShaped := st.Reduce == "liveSet" && Match(st.DeleteWhen, data)
	if !deleteShaped && !Match(st.When, data) {
		return g.foldDeleteInput(tx, n, inKey, dirty)
	}

	parts := make([]string, len(st.KeyPath))
	for i, kp := range st.KeyPath {
		kv, err := ResolvePath(kp, data, parent)
		if err != nil || kv == nil {
			// A matched input without a (complete) key cannot fold; treat as
			// non-membership (and retract any prior membership) rather than
			// erroring the topic.
			return g.foldDeleteInput(tx, n, inKey, dirty)
		}
		parts[i] = KeyString(coerceKeyScalar(kv))
	}
	outKey := []byte(OutKey(parts))

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
		dirty.Mark(n, prior)
	}

	// Retain the input as the stage sees it (the refold working set) —
	// forEach stages retain the element WITH its parent, so drain-time
	// refolds can still reach `$parent.`.
	inputBytes, err := packStageInput(st, data, parent)
	if err != nil {
		return fmt.Errorf("stage %q: retain input: %w", st.Name, err)
	}
	if err := tx.PutIn(st.Name, outKey, inKey, encodeRetained(gen, inputBytes)); err != nil {
		return err
	}
	if err := tx.PutSrc(st.Name, inKey, outKey); err != nil {
		return err
	}
	for i := range st.Joins {
		j := &st.Joins[i]
		if onV, err := ResolvePath(j.On, data, parent); err == nil && onV != nil {
			if err := tx.PutRev(st.Name, j.target(), []byte(KeyString(onV)), outKey); err != nil {
				return err
			}
		}
	}
	dirty.Mark(n, outKey)
	return nil
}

// foldDelete retracts one input from a stage: the key it fed refolds
// without it (possibly to deletion), and the delta cascades.
func (g *Graph) foldDelete(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
	if n.def.ForEach != "" {
		// The input's tombstone retracts every element it fanned.
		st := n.def
		var elems [][]byte
		if err := tx.DependentsOf(st.Name, fanJoinName, inKey, func(elemIn []byte) error {
			elems = append(elems, append([]byte(nil), elemIn...))
			return nil
		}); err != nil {
			return err
		}
		for _, elemIn := range elems {
			if err := g.foldDeleteInput(tx, n, elemIn, dirty); err != nil {
				return err
			}
			if err := tx.DeleteRev(st.Name, fanJoinName, inKey, elemIn); err != nil {
				return err
			}
		}
		return nil
	}
	return g.foldDeleteInput(tx, n, inKey, dirty)
}

// foldDeleteInput retracts one retained input from a stage.
func (g *Graph) foldDeleteInput(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
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
	dirty.Mark(n, outKey)
	return nil
}

// Drain refolds every dirty key exactly once, in stage topology order
// (a stage's consumers drain after it — retention marks they receive
// here are processed later in the same pass), in sorted key order for
// determinism. A refold whose output bytes did not change is SUPPRESSED
// — no store write, no delta, no cascade — which determinism makes
// sound and which turns a batch touching one key N times into one
// refold and at most one downstream write.
func (g *Graph) Drain(tx *stagestore.Tx, dirty Dirty) error {
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
func (g *Graph) refoldKey(tx *stagestore.Tx, n *graphNode, outKey []byte, dirty Dirty) error {
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
		if g.OnDelta != nil {
			if err := g.OnDelta(st.Name, outKey, nil, false); err != nil {
				return err
			}
		}
		for _, d := range n.dimConsumers {
			if err := tx.DeleteDim(d.node.def.Name, d.join.target(), outKey); err != nil {
				return err
			}
			if err := g.markDimDependents(tx, d, outKey, dirty); err != nil {
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
	obj, err := DecodeObject(out)
	if err != nil {
		return fmt.Errorf("stage %q: decode own output: %w", st.Name, err)
	}
	if g.OnDelta != nil {
		if err := g.OnDelta(st.Name, outKey, obj, true); err != nil {
			return err
		}
	}
	for _, d := range n.dimConsumers {
		// This stage's output IS the consumer join's dimension row —
		// stage dims carry generation 0 (absorption: never swept).
		if err := tx.PutDim(d.node.def.Name, d.join.target(), outKey, encodeRetained(0, out)); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, outKey, dirty); err != nil {
			return err
		}
	}
	for _, c := range n.consumers {
		if err := g.foldUpsert(tx, c, outKey, obj, 0, dirty); err != nil {
			return err
		}
	}
	return nil
}

// refoldOutput computes a key's output object from its retained inputs.
// live=false means the key has no qualifying inputs and its output (and
// downstream memberships) retract.
func refoldOutput(tx *stagestore.Tx, st *Stage, outKey []byte) (out []byte, live bool, err error) {
	if st.Reduce == "aggregate" {
		return refoldAggregate(tx, st, outKey)
	}
	if st.Reduce == "latest" {
		return refoldLatest(tx, st, outKey)
	}
	if st.Reduce == "liveSet" {
		return refoldLiveSet(tx, st, outKey)
	}
	// Reshape: one output object per key — deterministically the emitted
	// object of the bytewise-LARGEST input identity (real reshape inputs
	// are 1:1 with keys; the tiebreak only decides pathological
	// collisions, deterministically).
	type scoped struct{ data, parent any }
	var winner *scoped
	scanErr := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent}
		return nil
	})
	if scanErr != nil || winner == nil {
		return nil, false, scanErr
	}
	return emitReshape(st, winner.data, winner.parent)
}

// inputQualifies applies a stage's filtering joins to one input: it
// participates only while EVERY join's dimension row — addressed by the
// input's on value — exists and matches the join's where. A dimension
// that has not arrived yet fails participation and heals when it lands
// (the fan-out refolds dependents).
func inputQualifies(tx *stagestore.Tx, st *Stage, obj, parent any) (bool, error) {
	for i := range st.Joins {
		j := &st.Joins[i]
		// present: a dimension row exists at the input's On value AND
		// matches the join's Where. A normal join requires it; an Absent
		// (anti-)join forbids it — one rule: fail when present == Absent.
		present := false
		if onV, err := ResolvePath(j.On, obj, parent); err == nil && onV != nil {
			stored, err := tx.GetDim(st.Name, j.target(), []byte(KeyString(onV)))
			if err != nil {
				return false, err
			}
			if stored != nil {
				_, payload := decodeRetained(stored)
				dimObj, err := DecodeObject(payload)
				if err != nil {
					return false, err
				}
				present = Match(j.Where, dimObj)
			}
		}
		if present == j.Absent {
			return false, nil
		}
	}
	return true, nil
}

// emitReshape renders a stage's emit fields from one input object — the
// reshape path, and the winner path of reduce = "latest".
func emitReshape(st *Stage, obj, parent any) ([]byte, bool, error) {
	emitted := make(map[string]any, len(st.Emit))
	var err error
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		if e.From != "" {
			if v, err = ResolvePath(e.From, obj, parent); err != nil {
				v = nil // a missing field is null, per the language
			}
		} else {
			if v, err = Eval(e.compiled, obj, parent); err != nil {
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

// refoldLiveSet recomputes a created-minus-deleted key: LIVE iff at
// least one qualifying non-delete input and ZERO delete-shaped inputs —
// a set difference, so no ordering is needed and retracting the delete
// event itself un-deletes the key. The live key emits from its
// bytewise-largest non-delete input, like a reshape.
func refoldLiveSet(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type scoped struct{ data, parent any }
	var winner *scoped
	dead := false
	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		if dead {
			return nil
		}
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if Match(st.DeleteWhen, data) {
			dead = true
			winner = nil
			return nil
		}
		if ok, err := inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent}
		return nil
	})
	if err != nil || dead || winner == nil {
		return nil, false, err
	}
	return emitReshape(st, winner.data, winner.parent)
}

// refoldLatest recomputes an argmax key: the winner is the retained input
// with the greatest OrderBy value — a BUSINESS field, never arrival order,
// so backfills in keyset order and steady-state in log order converge —
// with TieBy the mandatory deterministic tiebreak. The stage's `when`
// filtered inputs before they were retained, so the argmax runs over the
// qualifying set only (an unapproved newer input never shadows an
// approved older one). The winner's object then emits like a reshape.
func refoldLatest(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type ranked struct {
		order, tie  any
		obj, parent any
	}
	var winner *ranked
	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return err
		}
		ov, _ := ResolvePath(st.OrderBy, obj, parent)
		tv, _ := ResolvePath(st.TieBy, obj, parent)
		cand := &ranked{order: ov, tie: tv, obj: obj, parent: parent}
		if winner == nil {
			winner = cand
			return nil
		}
		if c := compareStageValues(cand.order, winner.order, st.OrderByType == KeyTypeNumber); c > 0 {
			winner = cand
		} else if c == 0 {
			if compareStageValues(cand.tie, winner.tie, st.TieByType == KeyTypeNumber) > 0 {
				winner = cand
			}
		}
		return nil
	})
	if err != nil || winner == nil {
		return nil, false, err
	}
	return emitReshape(st, winner.obj, winner.parent)
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
func refoldAggregate(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	sums := make([]*big.Rat, len(st.Emit))
	mins := make([]*big.Rat, len(st.Emit))
	maxs := make([]*big.Rat, len(st.Emit))
	count := 0

	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return err
		}
		count++
		for i := range st.Emit {
			e := &st.Emit[i]
			if e.Count {
				continue
			}
			v, err := Eval(e.compiled, obj, parent)
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
		text, err := FormatRat(r)
		if err != nil {
			return nil, err
		}
		return json.Number(text), nil
	}
	return v, nil
}

// fanJoinName is the reserved reverse-index name a forEach stage uses to
// enumerate the element-inputs each parent currently fans (its
// reconciliation set). The NUL prefix keeps it out of any real join's
// topic namespace.
const fanJoinName = "\x00fan"

// fanInKey is a fanned element-input's identity: (parent input key,
// element out-key rendering), length-framed so any parent key bytes scan
// correctly.
func fanInKey(parentKey, elemKey []byte) []byte {
	var lead [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lead[:], uint64(len(parentKey)))
	out := make([]byte, 0, n+len(parentKey)+len(elemKey))
	out = append(out, lead[:n]...)
	out = append(out, parentKey...)
	return append(out, elemKey...)
}

// packStageInput renders the retained object for one input of a stage.
// forEach stages retain a WRAPPER per element — the element plus its
// enclosing input under fixed keys "e"/"p" — because refolds happen at
// drain time, after the enclosing event is gone, and emits/joins may
// reach `$parent.`.
func packStageInput(st *Stage, data, parent any) ([]byte, error) {
	if st.ForEach == "" {
		return marshalStageObject(data)
	}
	return marshalStageObject(map[string]any{"e": data, "p": parent})
}

// unpackStageInput decodes a retained payload back into (element scope,
// parent scope) for refold-time path resolution.
func unpackStageInput(st *Stage, payload []byte) (data, parent any, err error) {
	obj, err := DecodeObject(payload)
	if err != nil {
		return nil, nil, err
	}
	if st.ForEach == "" {
		return obj, nil, nil
	}
	m, ok := obj.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("forEach retention is not a wrapper object")
	}
	return m["e"], m["p"], nil
}

// encodeRetained prefixes a retained value with its 8-byte BE
// generation — the refresh epoch the input was captured under. Stage-fed
// retention and direct user writes carry generation 0, which no sweep
// ever removes (the sink sweep's >= 1 floor, mirrored: upstream
// refreshes reach stage-fed retention as EXPLICIT deltas, never sweeps —
// no epoch transits a stateful stage).
func encodeRetained(gen uint64, payload []byte) []byte {
	out := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint64(out, gen)
	return append(out, payload...)
}

// decodeRetained splits a retained value into generation and payload.
func decodeRetained(val []byte) (uint64, []byte) {
	if len(val) < 8 {
		return 0, val // pre-encoding store state: rebuilt anyway (format/fingerprint)
	}
	return binary.BigEndian.Uint64(val[:8]), val[8:]
}

// marshalStageObject renders a stage object deterministically: Go maps
// marshal with sorted keys, and json.Number values keep their exact
// digits — equal folds are byte-equal.
func marshalStageObject(obj any) ([]byte, error) { return json.Marshal(obj) }

// DecodeObject decodes with UseNumber so exact digits survive the
// store round-trip.
func DecodeObject(bs []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// OutKey renders a stage key: a single part verbatim, several parts
// through the producers' composite encoding (cluster.CompositeKey with
// synthetic column names), so a stage-fed table's composite tombstone
// machinery decodes it unchanged.
func OutKey(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	m := make(map[string]any, len(parts))
	cols := make([]string, len(parts))
	for i, part := range parts {
		c := fmt.Sprintf("k%d", i)
		cols[i] = c
		m[c] = part
	}
	return cluster.CompositeKey(m, cols)
}

// coerceKeyScalar renders a key path value into the canonical key space
// (numbers keep source digits; strings pass through).
func coerceKeyScalar(v any) any { return v }
