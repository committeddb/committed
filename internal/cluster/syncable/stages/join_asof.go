package stages

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Ordered joins (Join.OrderBy/TieBy/AsOf): a join that chooses WHICH of
// the rows matching its reference it resolves to — the greatest by its
// business order, or, with asOf, the greatest at or below the input's own
// time (SQL's ASOF JOIN; PostgreSQL's LATERAL … ORDER BY … DESC LIMIT 1).
//
// Every version of a reference lives in the join's version index, keyed
// by (frame(reference), sort key of orderBy then tieBy, producer key) under
// the order-preserving encoding, so resolution is one backward seek. An
// as-of join also indexes its dependents by (frame(reference), sort key of
// the input's asOf, output key): a version at T can only change what the
// inputs bounded in [T, the next version) resolve to, so a version insert,
// correction, move, or delete re-resolves exactly that range. A join
// without asOf re-resolves every dependent of the reference, as any join
// does.
//
// The index is per join (orderedJoinName): two ordered joins may address
// one target with different fields or orders.

func orderedJoinName(index int, j *Join) string { return fmt.Sprintf("%d:%s", index, j.target()) }

// joinRow resolves one lookup join for an input: the dimension row its
// reference selects (passing the join's where), or nil.
func (g *Graph) joinRow(tx *stagestore.Tx, st *Stage, index int, j *Join, obj, parent any) (any, error) {
	if j.ordered() {
		return g.resolveVersion(tx, st, index, j, obj, parent)
	}
	onKey := joinOnKey(j, obj, parent)
	if onKey == nil {
		return nil, nil
	}
	stored, err := tx.GetDim(st.Name, j.target(), onKey)
	if err != nil || stored == nil {
		return nil, err
	}
	_, payload := decodeRetained(stored)
	row, err := DecodeObject(payload)
	if err != nil {
		return nil, err
	}
	if !Match(j.Where, row) {
		return nil, nil
	}
	return row, nil
}

// versionVal / decodeVersionVal: a version entry's value is the absolute
// end of its order part in the key, then the retained row.
func versionVal(orderEnd int, gen uint64, row []byte) []byte {
	return append(binary.AppendUvarint(nil, uint64(orderEnd)), encodeRetained(gen, row)...) //nolint:gosec // G115: a length, never negative
}

func decodeVersionVal(v []byte) (orderEnd int, gen uint64, row []byte) {
	n, w := binary.Uvarint(v)
	if w <= 0 {
		return 0, 0, nil
	}
	gen, row = decodeRetained(v[w:])
	return int(n), gen, row //nolint:gosec // G115: written by versionVal from a key length
}

// versionSortKey encodes a version's orderBy then tieBy over the joined
// row; orderLen is the order part's length.
func versionSortKey(j *Join, row any) (key []byte, orderLen int, err error) {
	appendKeys := func(keys []winKey) error {
		for _, k := range keys {
			v, err := Eval(k.expr, row, nil)
			if err != nil {
				return err
			}
			if key, err = appendSortKey(key, v, k.desc, k.nullsFirst); err != nil {
				return err
			}
		}
		return nil
	}
	if err := appendKeys(j.orderKeys); err != nil {
		return nil, 0, err
	}
	orderLen = len(key)
	if err := appendKeys(j.tieKeys); err != nil {
		return nil, 0, err
	}
	return key, orderLen, nil
}

// resolveVersion seeks an ordered join's resolution for one input.
func (g *Graph) resolveVersion(tx *stagestore.Tx, st *Stage, index int, j *Join, obj, parent any) (any, error) {
	onKey := joinOnKey(j, obj, parent)
	if onKey == nil {
		return nil, nil
	}
	name := orderedJoinName(index, j)
	prefix := stagestore.OrdPrefix(onKey)
	var from []byte // nil: below the reference's greatest version
	var tag byte
	if j.asOfExpr != nil {
		av, err := Eval(j.asOfExpr, obj, parent)
		if err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q join %d asOf: %w", st.Name, index+1, err))
		}
		if av == nil {
			return nil, nil // a null bound: nothing is eligible
		}
		enc, err := appendSortKey(nil, av, false, false)
		if err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q join %d asOf: %w", st.Name, index+1, err))
		}
		tag = enc[0]
		from = stagestore.PrefixEnd(append(append([]byte(nil), prefix...), enc...))
	}
	var row any
	mismatch := false
	if err := tx.ScanJoinIndex(st.Name, stagestore.JoinIndexVersions, name, prefix, from, true, func(k, v []byte) (bool, error) {
		if j.asOfExpr != nil && k[len(prefix)] != tag {
			mismatch = true
			return false, nil
		}
		_, _, payload := decodeVersionVal(v)
		r, err := DecodeObject(payload)
		if err != nil {
			return false, err
		}
		if Match(j.Where, r) {
			row = r
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if j.asOfExpr != nil && row == nil && !mismatch {
		// Versions of a greater family sort after the bound: the
		// reference's first version tells.
		if err := tx.ScanJoinIndex(st.Name, stagestore.JoinIndexVersions, name, prefix, nil, false, func(k, _ []byte) (bool, error) {
			mismatch = k[len(prefix)] != tag && k[len(prefix)] != sortNullLast
			return false, nil
		}); err != nil {
			return nil, err
		}
	}
	if mismatch {
		return nil, cluster.Permanent(fmt.Errorf("stage %q join %d asOf: the input's bound and the versions' orderBy values are different kinds (a time against a number, or text) — cast both alike", st.Name, index+1))
	}
	return row, nil
}

// putVersion files (row set) or removes (row nil) one producer row of an
// ordered join and re-resolves the dependents whose resolution it can
// change.
func (g *Graph) putVersion(tx *stagestore.Tx, d dimRef, producerKey []byte, row any, gen uint64, dirty Dirty) error {
	st, j := d.node.def, d.join
	name := orderedJoinName(d.index, j)
	oldKey, err := tx.GetJoinIndex(st.Name, stagestore.JoinIndexPositions, name, producerKey)
	if err != nil {
		return err
	}
	oldKey = cloneBytes(oldKey)
	var oldVal []byte
	if oldKey != nil {
		v, err := tx.GetJoinIndex(st.Name, stagestore.JoinIndexVersions, name, oldKey)
		if err != nil {
			return err
		}
		oldVal = cloneBytes(v)
	}
	var newKey, newVal []byte
	if row != nil {
		if onKey := fieldOnKey(j, row); onKey != nil {
			sk, orderLen, err := versionSortKey(j, row)
			if err != nil {
				g.noteVersionOrderError(d, err)
			} else {
				bs, err := marshalStageObject(row)
				if err != nil {
					return err
				}
				prefix := stagestore.OrdPrefix(onKey)
				newKey = append(append(append([]byte(nil), prefix...), sk...), producerKey...)
				newVal = versionVal(len(prefix)+orderLen, gen, bs)
			}
		}
	}
	sameKey := bytes.Equal(oldKey, newKey)
	if sameKey && bytes.Equal(oldVal, newVal) {
		return nil
	}
	if oldKey != nil {
		if err := tx.DeleteJoinIndex(st.Name, stagestore.JoinIndexVersions, name, oldKey); err != nil {
			return err
		}
	}
	if newKey != nil {
		if err := tx.PutJoinIndex(st.Name, stagestore.JoinIndexVersions, name, newKey, newVal); err != nil {
			return err
		}
		if err := tx.PutJoinIndex(st.Name, stagestore.JoinIndexPositions, name, producerKey, newKey); err != nil {
			return err
		}
	} else if oldKey != nil {
		if err := tx.DeleteJoinIndex(st.Name, stagestore.JoinIndexPositions, name, producerKey); err != nil {
			return err
		}
	}
	// Ranges are taken after the change: a removed version's range falls
	// to its predecessor, bounded by the next version that remains.
	if oldKey != nil {
		if err := g.reresolve(tx, d, oldKey, oldVal, dirty); err != nil {
			return err
		}
	}
	if newKey != nil && !sameKey {
		return g.reresolve(tx, d, newKey, newVal, dirty)
	}
	return nil
}

// reresolve marks the dependents a version (its key and value) can move.
func (g *Graph) reresolve(tx *stagestore.Tx, d dimRef, key, val []byte, dirty Dirty) error {
	st, j, n := d.node.def, d.join, d.node
	onKey, rest, ok := stagestore.SplitFrame(key)
	if !ok {
		return nil
	}
	prefix := key[:len(key)-len(rest)]
	mark := func(outKey []byte) error {
		dirty.MarkFull(n, outKey)
		if n.win != nil {
			n.win.markFull(outKey) // membership may move every position
		}
		n.joinFlows[d.index].reresolved.Add(1)
		return nil
	}
	if j.asOfExpr == nil {
		return tx.DependentsOf(st.Name, j.target(), onKey, mark)
	}
	orderEnd, _, _ := decodeVersionVal(val)
	if orderEnd < len(prefix) || orderEnd > len(key) {
		return nil
	}
	lo := cloneBytes(key[:orderEnd])
	// The range ends at the next version (strictly later, passing where):
	// inputs bounded at or past it resolve there or later.
	var hi []byte
	name := orderedJoinName(d.index, j)
	if after := stagestore.PrefixEnd(lo); after != nil {
		if err := tx.ScanJoinIndex(st.Name, stagestore.JoinIndexVersions, name, prefix, after, false, func(k, v []byte) (bool, error) {
			end, _, payload := decodeVersionVal(v)
			r, err := DecodeObject(payload)
			if err != nil {
				return false, err
			}
			if Match(j.Where, r) && end <= len(k) {
				hi = cloneBytes(k[:end])
				return false, nil
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	var outKeys [][]byte
	if err := tx.ScanJoinIndex(st.Name, stagestore.JoinIndexDependents, name, prefix, lo, false, func(k, v []byte) (bool, error) {
		if hi != nil && bytes.Compare(k, hi) >= 0 {
			return false, nil
		}
		end, w := binary.Uvarint(v)
		if w <= 0 || end > uint64(len(k)) {
			return true, nil
		}
		outKeys = append(outKeys, cloneBytes(k[end:]))
		return true, nil
	}); err != nil {
		return err
	}
	for _, ok := range outKeys {
		if err := mark(ok); err != nil {
			return err
		}
	}
	return nil
}

// putAsOfDependent indexes one input's as-of bound for its output key. A
// null or unevaluable bound is never eligible, so no version can move it
// (resolution reports an unevaluable bound as the data error it is).
func (g *Graph) putAsOfDependent(tx *stagestore.Tx, st *Stage, index int, j *Join, data, parent any, outKey []byte) error {
	onKey := joinOnKey(j, data, parent)
	if onKey == nil {
		return nil
	}
	av, err := Eval(j.asOfExpr, data, parent)
	if err != nil || av == nil {
		return nil //nolint:nilerr // the refold reports it
	}
	enc, err := appendSortKey(nil, av, false, false)
	if err != nil {
		return nil //nolint:nilerr // the refold reports it
	}
	prefix := stagestore.OrdPrefix(onKey)
	key := append(append(append([]byte(nil), prefix...), enc...), outKey...)
	return tx.PutJoinIndex(st.Name, stagestore.JoinIndexDependents, orderedJoinName(index, j), key,
		binary.AppendUvarint(nil, uint64(len(prefix)+len(enc)))) //nolint:gosec // G115: a length, never negative
}

// sweepVersions applies a refresh marker to a topic ordered join's
// versions: rows captured under an older epoch retract.
func (g *Graph) sweepVersions(tx *stagestore.Tx, d dimRef, marker uint64, dirty Dirty) error {
	st := d.node.def
	name := orderedJoinName(d.index, d.join)
	var stale [][]byte
	if err := tx.AllJoinIndex(st.Name, stagestore.JoinIndexPositions, name, func(pk, vkey []byte) error {
		v, err := tx.GetJoinIndex(st.Name, stagestore.JoinIndexVersions, name, vkey)
		if err != nil || v == nil {
			return err
		}
		if _, gen, _ := decodeVersionVal(v); gen >= 1 && gen < marker {
			stale = append(stale, cloneBytes(pk))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, pk := range stale {
		if err := g.putVersion(tx, d, pk, nil, 0, dirty); err != nil {
			return err
		}
	}
	return nil
}

// noteVersionOrderError warns once that a version's orderBy/tieBy failed:
// that row is no version of anything (worker goroutine only).
func (g *Graph) noteVersionOrderError(d dimRef, err error) {
	if d.node.orderWarned {
		return
	}
	d.node.orderWarned = true
	zap.L().Warn("[stage] an ordered join's orderBy/tieBy expression failed on a joined row — such rows are never resolved to; if every row fails, the join never matches",
		zap.String("stage", d.node.def.Name), zap.String("join", d.join.target()), zap.Error(err))
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
