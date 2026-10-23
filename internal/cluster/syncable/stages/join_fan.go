package stages

import (
	"bytes"
	"errors"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Fan joins (Join.Fan): SQL's row-multiplying join. The stage's fold unit
// is the pair (input, matched row), one element per row the input's
// reference matches — identity (input key, matched producer key), framed
// like a forEach element, reconciled the same way (the fan reverse index
// records each input's current pairs; vanished pairs retract).
//
// Three structures keep the pairs exact as both sides change:
//   - the multi-match index (mdim): every producer row filed under the
//     value of the join's field, so an input's matches are one prefix scan
//     (mpos remembers where each producer row is filed, for its move or
//     delete);
//   - the retained inputs (parent): a matched row's arrival, change, or
//     departure re-fans every input referencing its value, from the
//     retained copy;
//   - a reverse index from reference value to referencing inputs
//     (fanOnRevName).
//
// The matched row is COPIED into the pair (unlike a lookup join, whose
// row is read at refold): the pair is the fold unit, and keyPath may key
// by the matched row. A dimension change re-fans, so the copies track
// their source.

// fanOnRevName names the reverse index from a fan join's reference values
// to the inputs making them (NUL lead: the reserved namespace).
func fanOnRevName(target string) string { return "\x00fanon:" + target }

// fanMatchElemKey is a pair's element key for a matched producer row;
// fanNoMatchElemKey is an optional fan join's single null pair. The lead
// byte keeps the two disjoint.
func fanMatchElemKey(producerKey []byte) []byte {
	return append([]byte{0x01}, producerKey...)
}

var fanNoMatchElemKey = []byte{0x00}

// fieldOnKey renders the value a producer row is filed under in a fan
// join's index: its field, in the join's declared key space — the
// rendering the input's reference gets (joinOnKey), so they meet byte for
// byte. nil = the row carries no usable value and matches nothing.
func fieldOnKey(j *Join, row any) []byte {
	v, err := ResolvePath(j.Field, row, nil)
	if err != nil || v == nil {
		return nil
	}
	part, ok := TypedKeyPart(KeyTypeAt(j.OnType, 0), coerceKeyScalar(v))
	if !ok {
		return nil
	}
	return []byte(NormalizeKeyPart(j.Normalize, part))
}

// putMultiDim files (or refiles) one producer row in a fan join's index
// and re-fans the inputs whose matches it changed.
func (g *Graph) putMultiDim(tx *stagestore.Tx, d dimRef, producerKey []byte, row any, gen uint64, dirty Dirty) error {
	stage, target := d.node.def.Name, d.join.target()
	old, err := tx.GetMPos(stage, target, producerKey)
	if err != nil {
		return err
	}
	old = append([]byte(nil), old...)
	onKey := fieldOnKey(d.join, row)
	if len(old) > 0 && !bytes.Equal(old, onKey) {
		if err := g.unfileMultiDim(tx, d, old, producerKey, dirty); err != nil {
			return err
		}
	}
	if onKey == nil {
		return nil
	}
	bs, err := marshalStageObject(row)
	if err != nil {
		return err
	}
	val := encodeRetained(gen, bs)
	if bytes.Equal(old, onKey) {
		cur, err := tx.GetMDim(stage, target, onKey, producerKey)
		if err != nil {
			return err
		}
		if bytes.Equal(cur, val) {
			return nil // unchanged: no pair moves
		}
	}
	if err := tx.PutMDim(stage, target, onKey, producerKey, val); err != nil {
		return err
	}
	if err := tx.PutMPos(stage, target, producerKey, onKey); err != nil {
		return err
	}
	return g.refanDependents(tx, d, onKey, producerKey, row, dirty)
}

// deleteMultiDim removes one producer row from a fan join's index.
func (g *Graph) deleteMultiDim(tx *stagestore.Tx, d dimRef, producerKey []byte, dirty Dirty) error {
	old, err := tx.GetMPos(d.node.def.Name, d.join.target(), producerKey)
	if err != nil || old == nil {
		return err
	}
	return g.unfileMultiDim(tx, d, append([]byte(nil), old...), producerKey, dirty)
}

func (g *Graph) unfileMultiDim(tx *stagestore.Tx, d dimRef, onKey, producerKey []byte, dirty Dirty) error {
	stage, target := d.node.def.Name, d.join.target()
	if err := tx.DeleteMDim(stage, target, onKey, producerKey); err != nil {
		return err
	}
	if err := tx.DeleteMPos(stage, target, producerKey); err != nil {
		return err
	}
	return g.refanDependents(tx, d, onKey, producerKey, nil, dirty)
}

// refanDependents updates ONE producer row's pair in every input
// referencing onKey — the row arrived or changed (row set) or left it
// (row nil). Each dependent gains, changes, or loses that one pair; its
// other pairs are untouched.
func (g *Graph) refanDependents(tx *stagestore.Tx, d dimRef, onKey, producerKey []byte, row any, dirty Dirty) error {
	n := d.node
	var parents [][]byte
	if err := tx.DependentsOf(n.def.Name, fanOnRevName(d.join.target()), onKey, func(inKey []byte) error {
		parents = append(parents, append([]byte(nil), inKey...))
		return nil
	}); err != nil {
		return err
	}
	matches := row != nil && Match(d.join.Where, row)
	for _, inKey := range parents {
		val, err := tx.GetParent(n.def.Name, inKey)
		if err != nil {
			return err
		}
		if val == nil {
			continue // a stale reverse entry: the input is gone
		}
		gen, payload := decodeRetained(val)
		obj, err := DecodeObject(payload)
		if err != nil {
			return err
		}
		base, isObj := obj.(map[string]any)
		if !isObj || !bytes.Equal(joinOnKey(d.join, obj, nil), onKey) {
			continue // a stale reverse entry: the input references another value now
		}
		if err := g.refanPair(tx, n, inKey, producerKey, base, row, matches, gen, dirty); err != nil {
			return err
		}
	}
	return nil
}

// refanPair sets one input's pair for one producer row: folded when the
// row matches, retracted otherwise — with an optional join's null pair
// standing in exactly while the input has no matched pair.
func (g *Graph) refanPair(tx *stagestore.Tx, n *graphNode, inKey, producerKey []byte, base map[string]any, row any, matches bool, gen uint64, dirty Dirty) error {
	st := n.def
	j := &st.Joins[n.fanJoin]
	src := st.identitySource(inKey)
	pairKey := fanInKey(inKey, fanMatchElemKey(producerKey))
	nullKey := fanInKey(inKey, fanNoMatchElemKey)
	if matches {
		n.fanned.Add(1)
		if err := g.foldOneInput(tx, n, src, pairKey, withAlias(base, j.As, row), nil, gen, nil, dirty); err != nil {
			return err
		}
		if err := tx.PutRev(st.Name, fanJoinName, inKey, pairKey); err != nil {
			return err
		}
		if j.Optional {
			return g.dropPair(tx, n, inKey, nullKey, dirty)
		}
		return nil
	}
	if err := g.dropPair(tx, n, inKey, pairKey, dirty); err != nil {
		return err
	}
	if !j.Optional {
		return nil
	}
	hasPair := false
	if err := tx.DependentsOf(st.Name, fanJoinName, inKey, func([]byte) error {
		hasPair = true
		return errStopScan
	}); err != nil && !errors.Is(err, errStopScan) {
		return err
	}
	if hasPair {
		return nil
	}
	n.fanned.Add(1)
	if err := g.foldOneInput(tx, n, src, nullKey, withAlias(base, j.As, nil), nil, gen, nil, dirty); err != nil {
		return err
	}
	return tx.PutRev(st.Name, fanJoinName, inKey, nullKey)
}

// dropPair retracts one pair if the input has it.
func (g *Graph) dropPair(tx *stagestore.Tx, n *graphNode, inKey, pairKey []byte, dirty Dirty) error {
	if err := g.foldDeleteInput(tx, n, pairKey, dirty); err != nil {
		return err
	}
	return tx.DeleteRev(n.def.Name, fanJoinName, inKey, pairKey)
}

// errStopScan ends a store scan early (never surfaced).
var errStopScan = errors.New("stop scan")

// foldJoinFan lands one input of a fan-join stage: it retains the input,
// files its reference, and folds one pair per matched row (or the null
// pair of an optional join), retracting pairs that no longer match.
func (g *Graph) foldJoinFan(tx *stagestore.Tx, n *graphNode, src string, inKey []byte, payload any, gen uint64, dirty Dirty) error {
	st := n.def
	j := &st.Joins[n.fanJoin]
	target, revName := j.target(), fanOnRevName(j.target())
	onKey := joinOnKey(j, payload, nil)

	// Re-file the input's reference: its retained copy says what it
	// referenced before.
	prior, err := tx.GetParent(st.Name, inKey)
	if err != nil {
		return err
	}
	if prior != nil {
		_, pp := decodeRetained(prior)
		if pobj, err := DecodeObject(pp); err == nil {
			if old := joinOnKey(j, pobj, nil); old != nil && !bytes.Equal(old, onKey) {
				if err := tx.DeleteRev(st.Name, revName, old, inKey); err != nil {
					return err
				}
			}
		}
	}
	bs, err := marshalStageObject(payload)
	if err != nil {
		return err
	}
	if err := tx.PutParent(st.Name, inKey, encodeRetained(gen, bs)); err != nil {
		return err
	}
	if onKey != nil {
		if err := tx.PutRev(st.Name, revName, onKey, inKey); err != nil {
			return err
		}
	}

	type pair struct {
		key  []byte
		data any
	}
	var pairs []pair
	base, isObj := payload.(map[string]any)
	if isObj && onKey != nil {
		if err := tx.MDimsFor(st.Name, target, onKey, func(pk, val []byte) error {
			_, rp := decodeRetained(val)
			row, err := DecodeObject(rp)
			if err != nil {
				return err
			}
			if Match(j.Where, row) {
				pairs = append(pairs, pair{fanInKey(inKey, fanMatchElemKey(pk)), withAlias(base, j.As, row)})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if len(pairs) > 0 {
		n.joinFlows[n.fanJoin].hits.Add(1)
	} else {
		n.joinFlows[n.fanJoin].misses.Add(1)
		if j.Optional && isObj {
			pairs = append(pairs, pair{fanInKey(inKey, fanNoMatchElemKey), withAlias(base, j.As, nil)})
		}
	}

	prev := map[string]bool{}
	if err := tx.DependentsOf(st.Name, fanJoinName, inKey, func(elemIn []byte) error {
		prev[string(elemIn)] = true
		return nil
	}); err != nil {
		return err
	}
	current := make(map[string]bool, len(pairs))
	for _, p := range pairs {
		n.fanned.Add(1)
		current[string(p.key)] = true
		if err := g.foldOneInput(tx, n, src, p.key, p.data, nil, gen, nil, dirty); err != nil {
			return err
		}
		if err := tx.PutRev(st.Name, fanJoinName, inKey, p.key); err != nil {
			return err
		}
	}
	for pk := range prev {
		if current[pk] {
			continue
		}
		if err := g.foldDeleteInput(tx, n, []byte(pk), dirty); err != nil {
			return err
		}
		if err := tx.DeleteRev(st.Name, fanJoinName, inKey, []byte(pk)); err != nil {
			return err
		}
	}
	return nil
}

// unfanJoin retracts one input of a fan-join stage: its pairs, its
// reference, and its retained copy.
func (g *Graph) unfanJoin(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
	st := n.def
	j := &st.Joins[n.fanJoin]
	prior, err := tx.GetParent(st.Name, inKey)
	if err != nil {
		return err
	}
	if prior != nil {
		_, pp := decodeRetained(prior)
		if pobj, err := DecodeObject(pp); err == nil {
			if old := joinOnKey(j, pobj, nil); old != nil {
				if err := tx.DeleteRev(st.Name, fanOnRevName(j.target()), old, inKey); err != nil {
					return err
				}
			}
		}
		if err := tx.DeleteParent(st.Name, inKey); err != nil {
			return err
		}
	}
	return g.retractElements(tx, n, inKey, dirty)
}

// sweepFanJoinInputs applies a refresh marker to a fan-join stage: inputs
// of this topic captured under an older epoch retract with their pairs.
func (g *Graph) sweepFanJoinInputs(tx *stagestore.Tx, n *graphNode, topic string, marker uint64, dirty Dirty) error {
	st := n.def
	var stale [][]byte
	if err := tx.ParentsAll(st.Name, func(inKey, val []byte) error {
		if st.isUnion() && st.identitySource(inKey) != topic {
			return nil
		}
		if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
			stale = append(stale, append([]byte(nil), inKey...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, inKey := range stale {
		if err := g.unfanJoin(tx, n, inKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// sweepMultiDims applies a refresh marker to a topic fan join's index.
func (g *Graph) sweepMultiDims(tx *stagestore.Tx, d dimRef, marker uint64, dirty Dirty) error {
	type filed struct{ onKey, producerKey []byte }
	var stale []filed
	if err := tx.MDimsAll(d.node.def.Name, d.join.target(), func(onKey, pk, val []byte) error {
		if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
			stale = append(stale, filed{append([]byte(nil), onKey...), append([]byte(nil), pk...)})
		}
		return nil
	}); err != nil {
		return err
	}
	for _, f := range stale {
		if err := g.unfileMultiDim(tx, d, f.onKey, f.producerKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// withAlias copies an input object with a matched row under alias.
func withAlias(base map[string]any, alias string, row any) map[string]any {
	out := make(map[string]any, len(base)+1)
	for k, v := range base {
		out[k] = v
	}
	out[alias] = row
	return out
}
