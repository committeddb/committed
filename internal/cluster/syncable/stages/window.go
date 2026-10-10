package stages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The window stage runtime. A window stage retains its inputs per
// partition like any stage (the in/src buckets, the partition playing the
// key's role), and additionally keeps every row in an ORDER-INDEXED bucket
// — (partition, order values, tie values, input key) under an
// order-preserving encoding — so a partition walks in business order and
// a change seeks to its position. Each row's output is keyed by its input
// key, and each row stores its RUNNING STATE (row number, rank, the
// running aggregates' accumulators, first_value's values), so a recompute
// continues from the predecessor's state instead of re-reading the
// prefix.
//
// The recompute is a SUFFIX: a change at position p can only move rows at
// or after the start of p's peer group (peers share rank and frame), and
// rows up to maxLead before it (their lead reaches it). The pass runs
// forward from there and stops at the first peer group G such that the
// rows before G include at least max(maxLag, 1) rows past every change in
// the batch (so lag from G cannot reach a change), and the last of them —
// an UNCHANGED row, so its stored state is the old sequence's state just
// before G — came out with byte-identical state: from there on every
// input to every row is unchanged. (Comparing a changed row's state would
// be unsound: a row moved in from another partition, or past a deleted
// one, can carry a stored state that matches by coincidence.) An append in
// business order therefore recomputes one or two rows; a late event
// recomputes the rows after it, which really change. A join dimension
// change recomputes the dependent partitions whole (membership may move
// every position).

// WindowLargePartitionRows is the partition size (rows) at which a window
// stage warns once (and a dry-run reports a finding): each out-of-order
// change recomputes the rows after it.
const WindowLargePartitionRows = 50_000

// windowRuntime is a window stage's runtime: compiled shape plus the
// batch's pending changes (positions per partition, and inputs that left
// a partition), consumed by Drain.
type windowRuntime struct {
	maxLag, maxLead int
	// pending: partition → the range of ord keys this batch changed (or
	// full, when membership may have moved everywhere).
	pending map[string]*winPending
	// gone: inputs whose row left its partition this batch — at drain,
	// those not re-placed elsewhere retract.
	gone        map[string]bool
	orderWarned bool
	largeWarned bool
	// recomputed counts rows recomputed since the worker started;
	// largest is the largest partition (rows) a pass walked to its end.
	recomputed atomic.Int64
	largest    atomic.Int64
}

type winPending struct {
	lo, hi     []byte
	loOrderEnd int
	full       bool
}

func newWindowRuntime(st *Stage) *windowRuntime {
	w := &windowRuntime{pending: map[string]*winPending{}, gone: map[string]bool{}}
	for _, s := range st.winSlots {
		switch s.fn {
		case "lag":
			w.maxLag = max(w.maxLag, int(s.offset))
		case "lead":
			w.maxLead = max(w.maxLead, int(s.offset))
		}
	}
	return w
}

// note records that the ord key k (with its peer prefix ending at
// orderEnd) changed in partition part this batch.
func (w *windowRuntime) note(part, k []byte, orderEnd int) {
	p := w.pending[string(part)]
	if p == nil {
		p = &winPending{}
		w.pending[string(part)] = p
	}
	if p.full {
		return
	}
	if p.lo == nil || bytes.Compare(k, p.lo) < 0 {
		p.lo, p.loOrderEnd = append([]byte(nil), k...), orderEnd
	}
	if p.hi == nil || bytes.Compare(k, p.hi) > 0 {
		p.hi = append([]byte(nil), k...)
	}
}

// markFull schedules a whole-partition recompute.
func (w *windowRuntime) markFull(part []byte) {
	w.pending[string(part)] = &winPending{full: true}
}

func (w *windowRuntime) reset() {
	clear(w.pending)
	clear(w.gone)
}

// windowSortKey evaluates an input's order and tie values (fold-time
// scope: the input alone) into its sort key; orderLen is the order part's
// length (the peer group's shared bytes).
func windowSortKey(st *Stage, data any) (key []byte, orderLen int, err error) {
	for _, k := range st.winOrder {
		if key, err = appendWinKey(key, k, data); err != nil {
			return nil, 0, err
		}
	}
	orderLen = len(key)
	for _, k := range st.winTie {
		if key, err = appendWinKey(key, k, data); err != nil {
			return nil, 0, err
		}
	}
	return key, orderLen, nil
}

func appendWinKey(dst []byte, k winKey, data any) ([]byte, error) {
	v, err := Eval(k.expr, data, nil)
	if err != nil {
		return nil, err
	}
	return appendSortKey(dst, v, k.desc, k.nullsFirst)
}

// windowPlace (re)positions an input's row in the order index. ok=false
// means the input's order could not evaluate — it is not a row of any
// partition (like an input whose key cannot resolve), warned once.
func (g *Graph) windowPlace(tx *stagestore.Tx, n *graphNode, part, inKey []byte, data any) (bool, error) {
	st, w := n.def, n.win
	sortKey, orderLen, err := windowSortKey(st, data)
	if err != nil {
		if !w.orderWarned {
			w.orderWarned = true
			zap.L().Warn("[stage] a window orderBy/tieBy expression failed on an input — such inputs are not rows of any partition; if every input fails, the stage's output is silently empty",
				zap.String("stage", st.Name), zap.Error(err))
		}
		return false, nil
	}
	ordKey := stagestore.OrdKey(part, sortKey, inKey)
	prior, err := tx.GetPos(st.Name, inKey)
	if err != nil {
		return false, err
	}
	if prior != nil && !bytes.Equal(prior, ordKey) {
		if err := g.windowUnplace(tx, n, append([]byte(nil), prior...)); err != nil {
			return false, err
		}
	}
	if !bytes.Equal(prior, ordKey) {
		if err := tx.PutOrd(st.Name, ordKey, orderLen, len(sortKey)); err != nil {
			return false, err
		}
		if err := tx.PutPos(st.Name, inKey, ordKey); err != nil {
			return false, err
		}
	}
	w.note(part, ordKey, len(stagestore.OrdPrefix(part))+orderLen)
	return true, nil
}

// windowUnplace removes a row's ord entry, noting its old position.
func (g *Graph) windowUnplace(tx *stagestore.Tx, n *graphNode, ordKey []byte) error {
	e, ok, err := tx.GetOrd(n.def.Name, ordKey)
	if err != nil {
		return err
	}
	if err := tx.DeleteOrd(n.def.Name, ordKey); err != nil {
		return err
	}
	if ok {
		n.win.note(e.Partition, ordKey, e.OrderEnd)
	}
	return nil
}

// windowRemove takes an input out of the order index entirely (a delete,
// a filter flip, an unkeyable re-emit); its output retracts at drain
// unless a later fold in the batch placed it again.
func (g *Graph) windowRemove(tx *stagestore.Tx, n *graphNode, inKey []byte) error {
	prior, err := tx.GetPos(n.def.Name, inKey)
	if err != nil || prior == nil {
		return err
	}
	if err := g.windowUnplace(tx, n, append([]byte(nil), prior...)); err != nil {
		return err
	}
	if err := tx.DeletePos(n.def.Name, inKey); err != nil {
		return err
	}
	n.win.gone[string(inKey)] = true
	return nil
}

// drainWindow recomputes a window stage's dirty partitions, then retracts
// the rows that left every partition.
func (g *Graph) drainWindow(tx *stagestore.Tx, n *graphNode, keys map[string]bool, dirty Dirty) error {
	parts := make([]string, 0, len(keys))
	for k := range keys {
		parts = append(parts, k)
	}
	sort.Strings(parts)
	for _, part := range parts {
		if err := g.refoldWindowPartition(tx, n, []byte(part), dirty); err != nil {
			return err
		}
	}
	gone := make([]string, 0, len(n.win.gone))
	for k := range n.win.gone {
		gone = append(gone, k)
	}
	sort.Strings(gone)
	for _, k := range gone {
		inKey := []byte(k)
		pos, err := tx.GetPos(n.def.Name, inKey)
		if err != nil {
			return err
		}
		if pos != nil {
			continue // re-placed in this batch (a move between partitions)
		}
		if err := g.windowDisqualify(tx, n, inKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// windowDisqualify drops a row's state and retracts its output — a row
// that left the index, or one a join no longer admits.
func (g *Graph) windowDisqualify(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
	if err := tx.DeleteRow(n.def.Name, inKey); err != nil {
		return err
	}
	prior, err := tx.GetOut(n.def.Name, inKey)
	if err != nil || prior == nil {
		return err
	}
	return g.retractOutput(tx, n, inKey, dirty)
}

// winRow is one qualifying row read in a pass.
type winRow struct {
	key   []byte // ord key
	peer  []byte // the peer group's shared prefix
	inKey []byte
	scope any // the join-lookup scope emits and window arguments see
}

// winReader walks a partition forward from a start key, yielding the
// QUALIFYING rows (inputs every required join admits) and handling the
// others as it passes them — their state drops and their output retracts.
// It re-seeks for every entry instead of holding a cursor, because the
// pass writes as it goes.
type winReader struct {
	g      *Graph
	tx     *stagestore.Tx
	n      *graphNode
	part   []byte
	prefix []byte
	after  []byte // the last ord key read (nil = start at start)
	start  []byte
	eof    bool
	buf    []*winRow
	dirty  Dirty
}

// fill reads until the buffer holds k qualifying rows or the partition
// ends.
func (r *winReader) fill(k int) error {
	for len(r.buf) < k && !r.eof {
		cur, err := r.tx.OrdCursor(r.n.def.Name)
		if err != nil {
			return err
		}
		var e stagestore.OrdEntry
		var ok bool
		if r.after == nil {
			e, ok = cur.Seek(r.start)
		} else {
			e, ok = cur.Seek(r.after)
			if ok && bytes.Equal(e.Key, r.after) {
				e, ok = cur.Next()
			}
		}
		if !ok || !bytes.HasPrefix(e.Key, r.prefix) {
			r.eof = true
			return nil
		}
		key := append([]byte(nil), e.Key...)
		row := &winRow{key: key, peer: key[:e.OrderEnd], inKey: key[e.SortEnd:]}
		r.after = key
		qualifies, err := r.g.loadWindowRow(r.tx, r.n, r.part, row)
		if err != nil {
			return err
		}
		if !qualifies {
			if err := r.g.windowDisqualify(r.tx, r.n, row.inKey, r.dirty); err != nil {
				return err
			}
			continue
		}
		r.buf = append(r.buf, row)
	}
	return nil
}

// loadWindowRow reads a row's retained input, applies the stage's
// required joins, and builds its lookup scope.
func (g *Graph) loadWindowRow(tx *stagestore.Tx, n *graphNode, part []byte, row *winRow) (bool, error) {
	st := n.def
	val, err := tx.GetIn(st.Name, part, row.inKey)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, fmt.Errorf("window stage %q: an ordered row has no retained input (store inconsistency)", st.Name)
	}
	_, payload := decodeRetained(val)
	data, _, err := unpackStageInput(st, payload)
	if err != nil {
		return false, err
	}
	if ok, err := g.inputQualifies(tx, st, data, nil); err != nil || !ok {
		return false, err
	}
	row.scope, err = g.joinLookupScope(tx, st, data, nil)
	return err == nil, err
}

// winState is a row's running state: what the next row continues from.
type winState struct {
	rn, rank, dense int64
	accs            []aggAcc // per slot; used by running-aggregate slots
	first           []any    // per slot; first_value's value at the partition's first row
	hasFirst        bool
}

// refoldWindowPartition recomputes one partition's changed suffix.
func (g *Graph) refoldWindowPartition(tx *stagestore.Tx, n *graphNode, part []byte, dirty Dirty) error {
	st, w := n.def, n.win
	pend := w.pending[string(part)]
	full := pend == nil || pend.full
	prefix := stagestore.OrdPrefix(part)

	start, err := g.windowStart(tx, n, prefix, pend, full)
	if err != nil {
		return err
	}
	state, history, err := g.windowPredecessor(tx, n, part, prefix, start)
	if err != nil {
		return err
	}

	r := &winReader{g: g, tx: tx, n: n, part: part, prefix: prefix, start: start, dirty: dirty}
	pastHi, stateSame := 0, false
	for {
		if err := r.fill(1); err != nil {
			return err
		}
		if len(r.buf) == 0 {
			break
		}
		if !full && pastHi >= max(w.maxLag, 1) && stateSame {
			return nil // every later row's inputs are unchanged (see the file comment)
		}
		// The peer group: buffered rows sharing the first row's order
		// values, plus maxLead rows of lookahead for lead.
		size := 1
		for {
			if err := r.fill(size + 1); err != nil {
				return err
			}
			if len(r.buf) <= size || !bytes.Equal(r.buf[size].peer, r.buf[0].peer) {
				break
			}
			size++
		}
		if err := r.fill(size + w.maxLead); err != nil {
			return err
		}
		same, err := g.windowGroup(tx, n, state, history, r.buf, size, dirty)
		if err != nil {
			return err
		}
		for _, row := range r.buf[:size] {
			if !full && bytes.Compare(row.key, pend.hi) > 0 {
				pastHi++
			}
		}
		// Only an unchanged row's stored state witnesses the old sequence;
		// the group's last row is unchanged exactly when it is past hi.
		stateSame = same && !full && bytes.Compare(r.buf[size-1].key, pend.hi) > 0
		history = append(history, r.buf[:size]...)
		if len(history) > w.maxLag {
			history = history[len(history)-w.maxLag:]
		}
		r.buf = r.buf[size:]
	}
	if state.rn > w.largest.Load() {
		w.largest.Store(state.rn)
	}
	if state.rn >= WindowLargePartitionRows && !w.largeWarned {
		w.largeWarned = true
		zap.L().Warn("[stage] a window partition is large — every out-of-order change recomputes the rows after it in its partition; partition by entity (an account, a job) rather than a tenant or the whole stream",
			zap.String("stage", st.Name), zap.Int64("rows", state.rn))
	}
	return nil
}

// windowStart finds where a pass begins: the start of the peer group of
// the batch's earliest change, backed up maxLead qualifying rows (their
// lead reaches the change) to that row's peer group — or the partition's
// start for a full pass.
func (g *Graph) windowStart(tx *stagestore.Tx, n *graphNode, prefix []byte, pend *winPending, full bool) ([]byte, error) {
	if full {
		return prefix, nil
	}
	start := append([]byte(nil), pend.lo[:pend.loOrderEnd]...)
	if n.win.maxLead == 0 {
		return start, nil
	}
	cur, err := tx.OrdCursor(n.def.Name)
	if err != nil {
		return nil, err
	}
	e, ok := cur.Seek(start)
	if ok {
		e, ok = cur.Prev()
	} else {
		e, ok = cur.Last()
	}
	count := 0
	for ok && bytes.HasPrefix(e.Key, prefix) {
		rs, err := tx.GetRow(n.def.Name, e.InKey())
		if err != nil {
			return nil, err
		}
		if rs != nil {
			if count++; count == n.win.maxLead {
				return append([]byte(nil), e.PeerPrefix()...), nil
			}
		}
		e, ok = cur.Prev()
	}
	return prefix, nil // fewer than maxLead rows before: from the start
}

// windowPredecessor walks back from start: the last qualifying row's
// state (the zero state at a partition's start) and the maxLag rows lag
// reads, oldest first. Rows before start are settled — a row has stored
// state exactly when it qualified at its last recompute.
func (g *Graph) windowPredecessor(tx *stagestore.Tx, n *graphNode, part, prefix, start []byte) (*winState, []*winRow, error) {
	st, w := n.def, n.win
	state := &winState{accs: make([]aggAcc, len(st.winSlots)), first: make([]any, len(st.winSlots))}
	cur, err := tx.OrdCursor(st.Name)
	if err != nil {
		return nil, nil, err
	}
	e, ok := cur.Seek(start)
	if ok {
		e, ok = cur.Prev()
	} else {
		e, ok = cur.Last()
	}
	var history []*winRow
	found := false
	for ok && bytes.HasPrefix(e.Key, prefix) && (!found || len(history) < w.maxLag) {
		rs, err := tx.GetRow(st.Name, e.InKey())
		if err != nil {
			return nil, nil, err
		}
		if rs != nil {
			if !found {
				found = true
				if state, err = decodeWinState(rs, len(st.winSlots)); err != nil {
					return nil, nil, fmt.Errorf("window stage %q: row state: %w", st.Name, err)
				}
			}
			if len(history) < w.maxLag {
				key := append([]byte(nil), e.Key...)
				row := &winRow{key: key, peer: key[:e.OrderEnd], inKey: key[e.SortEnd:]}
				if _, err := g.loadWindowRow(tx, n, part, row); err != nil {
					return nil, nil, err
				}
				history = append([]*winRow{row}, history...)
			}
		}
		e, ok = cur.Prev()
	}
	return state, history, nil
}

// windowGroup computes one peer group (buf[:size], with buf[size:] the
// lookahead) from the predecessor state, which it advances in place:
// every row's slot values, output, and stored state. same reports whether
// the group's last row's state came out byte-identical to what was
// stored.
func (g *Graph) windowGroup(tx *stagestore.Tx, n *graphNode, state *winState, history, buf []*winRow, size int, dirty Dirty) (same bool, err error) {
	st, w := n.def, n.win
	slots := st.winSlots
	rnBefore := state.rn
	state.rank = rnBefore + 1
	state.dense++
	stateBytes := make([][]byte, size)
	for j := 0; j < size; j++ {
		row := buf[j]
		for k := range slots {
			s := &slots[k]
			switch {
			case s.isAggregate():
				agg := s.agg()
				if err := agg.add(&state.accs[k], row.scope, nil); err != nil {
					return false, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", st.Name, s.fn, err))
				}
			case s.fn == "first_value" && !state.hasFirst:
				v, err := Eval(s.arg, row.scope, nil)
				if err != nil {
					return false, cluster.Permanent(fmt.Errorf("stage %q first_value(…): %w", st.Name, err))
				}
				state.first[k] = v
			}
		}
		state.hasFirst = true
		state.rn++
		if stateBytes[j], err = encodeWinState(state); err != nil {
			return false, err
		}
	}
	aggRes := make([]any, len(slots))
	for k := range slots {
		if slots[k].isAggregate() {
			agg := slots[k].agg()
			aggRes[k] = agg.result(&state.accs[k])
		}
	}
	// lag reads history then this group; lead reads this group then the
	// lookahead.
	behind := append(append([]*winRow(nil), history...), buf[:size]...)
	for j := 0; j < size; j++ {
		row := buf[j]
		res := make([]any, len(slots))
		for k := range slots {
			s := &slots[k]
			var v any
			switch s.fn {
			case "row_number":
				v = new(big.Rat).SetInt64(rnBefore + int64(j) + 1)
			case "rank":
				v = new(big.Rat).SetInt64(state.rank)
			case "dense_rank":
				v = new(big.Rat).SetInt64(state.dense)
			case "first_value":
				v = state.first[k]
			case "last_value":
				v, err = Eval(s.arg, buf[size-1].scope, nil)
			case "lag":
				if t := len(history) + j - int(s.offset); t >= 0 {
					v, err = Eval(s.arg, behind[t].scope, nil)
				} else if s.def != nil {
					v, err = Eval(s.def, row.scope, nil)
				}
			case "lead":
				if t := j + int(s.offset); t < len(buf) {
					v, err = Eval(s.arg, buf[t].scope, nil)
				} else if s.def != nil {
					v, err = Eval(s.def, row.scope, nil)
				}
			default:
				v = aggRes[k]
			}
			if err != nil {
				return false, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", st.Name, s.fn, err))
			}
			res[k] = v
		}
		out, err := g.emitWindowRow(st, row.scope, res)
		if err != nil {
			return false, err
		}
		w.recomputed.Add(1)
		old, err := tx.GetRow(st.Name, row.inKey)
		if err != nil {
			return false, err
		}
		same = bytes.Equal(old, stateBytes[j])
		if !same {
			if err := tx.PutRow(st.Name, row.inKey, stateBytes[j]); err != nil {
				return false, err
			}
		}
		prior, err := tx.GetOut(st.Name, row.inKey)
		if err != nil {
			return false, err
		}
		if prior != nil && bytes.Equal(prior, out) {
			continue
		}
		if err := g.publishOutput(tx, n, row.inKey, out, dirty); err != nil {
			return false, err
		}
	}
	return same, nil
}

// emitWindowRow renders a window row's emit object: paths read the row,
// window calls their slot results.
func (g *Graph) emitWindowRow(st *Stage, scope any, res []any) ([]byte, error) {
	emitted := make(map[string]any, len(st.Emit))
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		var err error
		if e.From != "" {
			if v, err = ResolvePath(e.From, scope, nil); err != nil {
				v = nil // a missing field is null, per the language
			}
		} else if v, err = Eval(e.compiled, windowRow{obj: scope, res: res}, nil); err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
		}
		if emitted[e.Field], err = stageValue(v); err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
		}
	}
	return marshalStageObject(emitted)
}

// ── row state codec ─────────────────────────────────────────────────────
//
// A row's state is stored as canonical JSON (fixed field order, typed
// values tagged), so equal states are byte-equal — the early stop
// compares bytes.

type winStateJSON struct {
	RN    int64        `json:"rn"`
	Rank  int64        `json:"rank"`
	Dense int64        `json:"dense"`
	Accs  []winAccJSON `json:"accs,omitempty"`
	First []any        `json:"first,omitempty"`
	HasF  bool         `json:"hasFirst,omitempty"`
}

type winAccJSON struct {
	N    int64 `json:"n,omitempty"`
	Sum  any   `json:"sum,omitempty"`
	Iv   any   `json:"iv,omitempty"`
	Best any   `json:"best,omitempty"`
}

func encodeWinState(s *winState) ([]byte, error) {
	j := winStateJSON{RN: s.rn, Rank: s.rank, Dense: s.dense, HasF: s.hasFirst}
	j.Accs = make([]winAccJSON, len(s.accs))
	j.First = make([]any, len(s.first))
	for k := range s.accs {
		a := &s.accs[k]
		j.Accs[k].N = a.n
		if a.sum != nil {
			j.Accs[k].Sum = encodeWinValue(a.sum)
		}
		if a.ivSum != nil {
			j.Accs[k].Iv = encodeWinValue(*a.ivSum)
		}
		j.Accs[k].Best = encodeWinValue(a.best)
	}
	for k, v := range s.first {
		j.First[k] = encodeWinValue(v)
	}
	return json.Marshal(j)
}

func decodeWinState(bs []byte, slots int) (*winState, error) {
	obj, err := DecodeObject(bs)
	if err != nil {
		return nil, err
	}
	m, ok := obj.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("not an object")
	}
	s := &winState{accs: make([]aggAcc, slots), first: make([]any, slots)}
	s.rn, _ = jsonInt(m["rn"])
	s.rank, _ = jsonInt(m["rank"])
	s.dense, _ = jsonInt(m["dense"])
	s.hasFirst, _ = m["hasFirst"].(bool)
	accs, _ := m["accs"].([]any)
	for k := 0; k < slots && k < len(accs); k++ {
		am, _ := accs[k].(map[string]any)
		a := &s.accs[k]
		a.n, _ = jsonInt(am["n"])
		if v, err := decodeWinValue(am["sum"]); err != nil {
			return nil, err
		} else if r, ok := v.(*big.Rat); ok {
			a.sum = r
		}
		if v, err := decodeWinValue(am["iv"]); err != nil {
			return nil, err
		} else if iv, ok := v.(intervalVal); ok {
			a.ivSum = &iv
		}
		if a.best, err = decodeWinValue(am["best"]); err != nil {
			return nil, err
		}
	}
	first, _ := m["first"].([]any)
	for k := 0; k < slots && k < len(first); k++ {
		if s.first[k], err = decodeWinValue(first[k]); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func jsonInt(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	return i, err == nil
}

// encodeWinValue tags an evaluator value for the row-state codec.
func encodeWinValue(v any) any {
	switch t := v.(type) {
	case *big.Rat:
		return map[string]any{"n": t.RatString()}
	case string:
		return map[string]any{"s": t}
	case bool:
		return map[string]any{"b": t}
	case timestampVal:
		return map[string]any{"ts": t.us, "z": t.zoned}
	case dateVal:
		return map[string]any{"d": t.days}
	case intervalVal:
		return map[string]any{"i": []int64{t.months, t.days, t.us}}
	}
	return nil
}

func decodeWinValue(v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, nil
	}
	switch {
	case m["n"] != nil:
		s, _ := m["n"].(string)
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			return nil, fmt.Errorf("bad number %q", s)
		}
		return r, nil
	case m["s"] != nil:
		s, _ := m["s"].(string)
		return s, nil
	case m["b"] != nil:
		b, _ := m["b"].(bool)
		return b, nil
	case m["ts"] != nil:
		us, _ := jsonInt(m["ts"])
		z, _ := m["z"].(bool)
		return timestampVal{us: us, zoned: z}, nil
	case m["d"] != nil:
		d, _ := jsonInt(m["d"])
		return dateVal{days: d}, nil
	case m["i"] != nil:
		parts, _ := m["i"].([]any)
		if len(parts) != 3 {
			return nil, fmt.Errorf("bad interval")
		}
		var iv [3]int64
		for i := range parts {
			iv[i], _ = jsonInt(parts[i])
		}
		return intervalVal{months: iv[0], days: iv[1], us: iv[2]}, nil
	}
	return nil, fmt.Errorf("unknown tagged value")
}
