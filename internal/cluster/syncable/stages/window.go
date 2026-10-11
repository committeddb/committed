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
// prefix accumulators of the calls whose frame starts at the partition's
// first row, first_value's values), so a recompute continues from the
// predecessor's state instead of re-reading the prefix.
//
// The recompute is a SUFFIX (window_pass.go): a change at position p can
// only move rows whose frame — or lag/lead — reaches p. The pass starts at
// the earliest such row (the start of p's peer group, backed up by the
// stage's forward reach: lead offsets, ROWS n FOLLOWING, RANGE offset
// FOLLOWING) and runs forward, stopping at the first peer group G whose
// rows can no longer reach a change: G's backward reach (lag offsets, ROWS
// n PRECEDING, a RANGE offset PRECEDING measured from G's own value) lies
// wholly past every change in the batch, and the last row before G — an
// UNCHANGED row, so its stored state is the old sequence's state just
// before G — came out with the same state, as far as anything emitted
// reads it. From there on every input to every row is unchanged.
// (Comparing a changed row's state would be unsound: a row moved in from
// another partition, or past a deleted one, can carry a stored state that
// matches by coincidence.) An append in business order therefore
// recomputes the rows its frames reach; a late event recomputes the rows
// after it that really change. A frame ending UNBOUNDED FOLLOWING makes
// every row depend on every later one, so its stage recomputes whole
// partitions; and a join dimension change recomputes the dependent
// partitions whole (membership may move every position).

// WindowLargePartitionRows is the partition size (rows) at which a window
// stage warns once (and a dry-run reports a finding): each out-of-order
// change recomputes the rows after it.
const WindowLargePartitionRows = 50_000

// windowRuntime is a window stage's runtime: its compiled reach (how far
// a row's computation looks back and forward), plus the batch's pending
// changes (positions per partition, and inputs that left a partition),
// consumed by Drain.
type windowRuntime struct {
	st *Stage
	// rowsFwd: rows ahead a row's computation reads (lead, ROWS n
	// FOLLOWING); rowsBack: rows behind (lag, ROWS n PRECEDING), at least 1.
	rowsFwd, rowsBack int
	// rangeFwd: RANGE offsets FOLLOWING (the start rule backs up by them);
	// rangeStartBack: RANGE offsets PRECEDING that bound a frame's start;
	// rangeEndBack: RANGE offsets PRECEDING that end a frame starting at
	// the partition's first row.
	rangeFwd, rangeStartBack, rangeEndBack []any
	// fullPass: some frame ends UNBOUNDED FOLLOWING.
	fullPass bool
	// What the emitted values read of a row's state — the early stop
	// compares only these.
	usesRN, usesRank bool
	prefixSlot       []bool // running accumulators kept in state (frame starts unbounded)
	firstSlot        []bool // first_value carried in state (frame starts unbounded)
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
	lo, hi                 []byte
	loOrderEnd, hiOrderEnd int
	// loVal is lo's first order value (kept only when RANGE FOLLOWING
	// offsets need it for the start rule); loValOK says it is known.
	loVal   any
	loValOK bool
	full    bool
}

func newWindowRuntime(st *Stage) *windowRuntime {
	w := &windowRuntime{
		st: st, rowsBack: 1, pending: map[string]*winPending{}, gone: map[string]bool{},
		prefixSlot: make([]bool, len(st.winSlots)), firstSlot: make([]bool, len(st.winSlots)),
	}
	for k, s := range st.winSlots {
		switch s.fn {
		case "lag":
			w.rowsBack = max(w.rowsBack, int(s.offset))
		case "lead":
			w.rowsFwd = max(w.rowsFwd, int(s.offset))
		case "row_number":
			w.usesRN = true
		case "rank", "dense_rank":
			w.usesRank = true
		}
		if !s.isAggregate() && s.fn != "first_value" && s.fn != "last_value" {
			continue
		}
		f := s.frameOf()
		unboundedStart := f.start.kind == boundUnboundedPreceding
		w.prefixSlot[k] = unboundedStart && s.isAggregate()
		w.firstSlot[k] = unboundedStart && s.fn == "first_value"
		if f.end.kind == boundUnboundedFollowing {
			w.fullPass = true
		}
		for _, b := range []winBound{f.start, f.end} {
			switch {
			case b.kind == boundPreceding && f.rows:
				w.rowsBack = max(w.rowsBack, int(b.rows))
			case b.kind == boundFollowing && f.rows:
				w.rowsFwd = max(w.rowsFwd, int(b.rows))
			case b.kind == boundFollowing:
				w.rangeFwd = append(w.rangeFwd, b.off)
			}
		}
		if !f.rows && f.start.kind == boundPreceding {
			w.rangeStartBack = append(w.rangeStartBack, f.start.off)
		}
		if !f.rows && unboundedStart && f.end.kind == boundPreceding {
			w.rangeEndBack = append(w.rangeEndBack, f.end.off)
		}
	}
	return w
}

// note records that the ord key k (its peer prefix ending at orderEnd,
// its first order value val when valOK) changed in partition part this
// batch.
func (w *windowRuntime) note(part, k []byte, orderEnd int, val any, valOK bool) {
	p := w.pending[string(part)]
	if p == nil {
		p = &winPending{}
		w.pending[string(part)] = p
	}
	if p.full {
		return
	}
	if p.lo == nil || bytes.Compare(k, p.lo) < 0 {
		p.lo, p.loOrderEnd, p.loVal, p.loValOK = append([]byte(nil), k...), orderEnd, val, valOK
	}
	if p.hi == nil || bytes.Compare(k, p.hi) > 0 {
		p.hi, p.hiOrderEnd = append([]byte(nil), k...), orderEnd
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

// witness renders the part of a row's state that emitted values read —
// the early stop's comparison.
func (w *windowRuntime) witness(s *winState) ([]byte, error) {
	m := &winState{accs: make([]aggAcc, len(s.accs)), first: make([]any, len(s.first))}
	if w.usesRN {
		m.rn = s.rn
	}
	if w.usesRank {
		m.rank, m.dense = s.rank, s.dense
	}
	for k := range s.accs {
		if w.prefixSlot[k] {
			m.accs[k] = s.accs[k]
		}
		if w.firstSlot[k] {
			m.first[k] = s.first[k]
			m.hasFirst = s.hasFirst
		}
	}
	return encodeWinState(m)
}

// windowSortKey evaluates an input's order and tie values (fold-time
// scope: the input alone) into its sort key; orderLen is the order part's
// length (the peer group's shared bytes) and first the first order value.
func windowSortKey(st *Stage, data any) (key []byte, orderLen int, first any, err error) {
	for i, k := range st.winOrder {
		v, err := Eval(k.expr, data, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if i == 0 {
			first = v
		}
		if key, err = appendSortKey(key, v, k.desc, k.nullsFirst); err != nil {
			return nil, 0, nil, err
		}
	}
	orderLen = len(key)
	for _, k := range st.winTie {
		v, err := Eval(k.expr, data, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if key, err = appendSortKey(key, v, k.desc, k.nullsFirst); err != nil {
			return nil, 0, nil, err
		}
	}
	return key, orderLen, first, nil
}

// windowPlace (re)positions an input's row in the order index. ok=false
// means the input's order could not evaluate — it is not a row of any
// partition (like an input whose key cannot resolve), warned once.
func (g *Graph) windowPlace(tx *stagestore.Tx, n *graphNode, part, inKey []byte, data any) (bool, error) {
	st, w := n.def, n.win
	sortKey, orderLen, first, err := windowSortKey(st, data)
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
	w.note(part, ordKey, len(stagestore.OrdPrefix(part))+orderLen, first, true)
	return true, nil
}

// windowUnplace removes a row's ord entry, noting its old position (and,
// when the start rule needs it, its old order value, read from the input
// still retained under that position's partition).
func (g *Graph) windowUnplace(tx *stagestore.Tx, n *graphNode, ordKey []byte) error {
	e, ok, err := tx.GetOrd(n.def.Name, ordKey)
	if err != nil {
		return err
	}
	var val any
	valOK := false
	if ok && len(n.win.rangeFwd) > 0 {
		if raw, err := tx.GetIn(n.def.Name, e.Partition, e.InKey()); err != nil {
			return err
		} else if raw != nil {
			_, payload := decodeRetained(raw)
			if data, _, err := unpackStageInput(n.def, payload); err == nil {
				if _, _, first, err := windowSortKey(n.def, withSource(data, n.def.inputSource(e.InKey()))); err == nil {
					val, valOK = first, true
				}
			}
		}
	}
	if err := tx.DeleteOrd(n.def.Name, ordKey); err != nil {
		return err
	}
	if ok {
		n.win.note(e.Partition, ordKey, e.OrderEnd, val, valOK)
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
	scope, err := g.joinLookupScope(tx, st, data, nil)
	if err != nil {
		return false, err
	}
	row.scope = withSource(scope, st.inputSource(row.inKey))
	return true, nil
}

// winState is a row's running state: what the next row continues from.
type winState struct {
	rn, rank, dense int64
	accs            []aggAcc // per slot; the prefix accumulators of prefixSlot calls
	first           []any    // per slot; first_value's value at the partition's first row
	hasFirst        bool
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
