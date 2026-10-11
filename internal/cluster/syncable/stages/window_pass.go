package stages

import (
	"bytes"
	"fmt"
	"math/big"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The recompute pass of a window partition (see window.go for the start
// and stop rules). Rows live in an in-memory sequence indexed by absolute
// position: the pass's first row is 0, rows before it are negative and load
// lazily from the store as frames, lag, and the predecessor state reach
// back; rows after load lazily as frames and lead reach forward. Rows the
// pass has left behind beyond any reach are dropped, so memory follows the
// frames, not the partition (except under an UNBOUNDED FOLLOWING frame,
// which needs the partition's end).

// winRow is one qualifying row of a pass.
type winRow struct {
	key, peer, order, inKey []byte
	scope                   any       // the join-lookup scope emits and window arguments see
	state                   *winState // the running state AFTER this row
	val                     any       // the first order value, once evaluated
	valDone                 bool
}

type winPass struct {
	g       *Graph
	tx      *stagestore.Tx
	n       *graphNode
	st      *Stage
	w       *windowRuntime
	part    []byte
	prefix  []byte
	start   []byte
	dirty   Dirty
	seq     []*winRow
	base    int // absolute position of seq[0]
	fwdNext int // absolute position the next forward row gets
	after   []byte
	atStart bool // nothing qualifying precedes seq[0]
	eof     bool
	// lowWater is the lowest position the current group's computations
	// read — the trim keeps it.
	lowWater int
}

// refoldWindowPartition recomputes one partition's changed suffix.
func (g *Graph) refoldWindowPartition(tx *stagestore.Tx, n *graphNode, part []byte, dirty Dirty) error {
	st, w := n.def, n.win
	pend := w.pending[string(part)]
	full := pend == nil || pend.full || w.fullPass
	prefix := stagestore.OrdPrefix(part)
	start, err := g.windowStart(tx, n, prefix, pend, full)
	if err != nil {
		return err
	}
	p := &winPass{g: g, tx: tx, n: n, st: st, w: w, part: part, prefix: prefix, start: start, dirty: dirty}
	// The predecessor: the last qualifying row before start (its state is
	// what the first row continues from), or the partition's start.
	if _, err := p.extendBack(); err != nil {
		return err
	}

	var hiOrder, firstPastHi []byte
	if !full {
		hiOrder = pend.hi[len(prefix):pend.hiOrderEnd]
	}
	pastHi, stateSame := 0, false
	for cur := 0; ; {
		r, err := p.rowAt(cur)
		if err != nil {
			return err
		}
		if r == nil {
			break
		}
		if !full && stateSame && pastHi >= w.rowsBack {
			ok, err := p.rangeReachClear(r, hiOrder, firstPastHi)
			if err != nil {
				return err
			}
			if ok {
				return nil // every later row's inputs are unchanged (see window.go)
			}
		}
		size := 1
		for {
			nx, err := p.rowAt(cur + size)
			if err != nil {
				return err
			}
			if nx == nil || !bytes.Equal(nx.peer, r.peer) {
				break
			}
			size++
		}
		p.lowWater = cur - w.rowsBack - 1
		same := false
		for i := cur; i < cur+size; i++ {
			if same, err = p.computeRow(i); err != nil {
				return err
			}
			row := p.seq[i-p.base]
			if !full && bytes.Compare(row.key, pend.hi) > 0 {
				pastHi++
				if firstPastHi == nil {
					firstPastHi = row.order
				}
			}
		}
		last := p.seq[cur+size-1-p.base]
		stateSame = same && !full && bytes.Compare(last.key, pend.hi) > 0
		cur += size
		p.trim(cur)
	}
	if len(p.seq) > 0 {
		rows := p.seq[len(p.seq)-1].state.rn
		if rows > w.largest.Load() {
			w.largest.Store(rows)
		}
		if rows >= WindowLargePartitionRows && !w.largeWarned {
			w.largeWarned = true
			zap.L().Warn("[stage] a window partition is large — every out-of-order change recomputes the rows after it in its partition; partition by entity (an account, a job) rather than a tenant or the whole stream",
				zap.String("stage", st.Name), zap.Int64("rows", rows))
		}
	}
	return nil
}

// windowStart finds where a pass begins: the start of the peer group of
// the batch's earliest change, backed up by the stage's forward reach — the
// rows whose lead or frame end reaches the change — or the partition's
// start for a full pass.
func (g *Graph) windowStart(tx *stagestore.Tx, n *graphNode, prefix []byte, pend *winPending, full bool) ([]byte, error) {
	w := n.win
	if full {
		return prefix, nil
	}
	start := append([]byte(nil), pend.lo[:pend.loOrderEnd]...)
	if len(w.rangeFwd) > 0 {
		if !pend.loValOK {
			return prefix, nil // the change's order value is unknown: from the start
		}
		if pend.loVal != nil {
			k := n.def.winOrder[0]
			for _, off := range w.rangeFwd {
				bv, err := rangeBound(pend.loVal, off, true, k.desc)
				if err != nil {
					return prefix, nil //nolint:nilerr // an unmovable value bounds nothing: recompute from the start
				}
				enc, err := appendSortKey(append([]byte(nil), prefix...), bv, k.desc, k.nullsFirst)
				if err != nil {
					return prefix, nil //nolint:nilerr // as above
				}
				if bytes.Compare(enc, start) < 0 {
					start = enc
				}
			}
		}
	}
	if w.rowsFwd == 0 {
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
			if count++; count == w.rowsFwd {
				return append([]byte(nil), e.PeerPrefix()...), nil
			}
		}
		e, ok = cur.Prev()
	}
	return prefix, nil // fewer than rowsFwd rows before: from the start
}

// rowAt returns the row at an absolute position, loading back or forward
// as needed; nil past either end of the partition.
func (p *winPass) rowAt(abs int) (*winRow, error) {
	for abs < p.base {
		ok, err := p.extendBack()
		if err != nil || !ok {
			return nil, err
		}
	}
	for abs >= p.fwdNext && !p.eof {
		if err := p.readForward(); err != nil {
			return nil, err
		}
	}
	if abs >= p.fwdNext {
		return nil, nil
	}
	if abs < p.lowWater {
		p.lowWater = abs
	}
	return p.seq[abs-p.base], nil
}

// extendBack loads the qualifying row before seq[0] (before start when the
// sequence is empty) with its stored state; false at the partition's
// start. Rows before the pass are settled: a row has stored state exactly
// when it qualified at its last recompute — and rows the pass already
// recomputed have their new state stored.
func (p *winPass) extendBack() (bool, error) {
	if p.atStart {
		return false, nil
	}
	from := p.start
	if len(p.seq) > 0 {
		from = p.seq[0].key
	}
	cur, err := p.tx.OrdCursor(p.st.Name)
	if err != nil {
		return false, err
	}
	e, ok := cur.Seek(from)
	if ok {
		e, ok = cur.Prev()
	} else {
		e, ok = cur.Last()
	}
	for ok && bytes.HasPrefix(e.Key, p.prefix) {
		rs, err := p.tx.GetRow(p.st.Name, e.InKey())
		if err != nil {
			return false, err
		}
		if rs != nil {
			state, err := decodeWinState(rs, len(p.st.winSlots))
			if err != nil {
				return false, fmt.Errorf("window stage %q: row state: %w", p.st.Name, err)
			}
			row := p.newRow(e)
			if _, err := p.g.loadWindowRow(p.tx, p.n, p.part, row); err != nil {
				return false, err
			}
			row.state = state
			p.seq = append([]*winRow{row}, p.seq...)
			p.base--
			return true, nil
		}
		e, ok = cur.Prev()
	}
	p.atStart = true
	return false, nil
}

func (p *winPass) newRow(e stagestore.OrdEntry) *winRow {
	key := append([]byte(nil), e.Key...)
	return &winRow{key: key, peer: key[:e.OrderEnd], order: key[len(p.prefix):e.OrderEnd], inKey: key[e.SortEnd:]}
}

// readForward reads the next ord entry after the last forward row: a
// qualifying row joins the sequence with its running state advanced from
// its predecessor's; any other row's state drops and its output retracts.
// It re-seeks for every entry instead of holding a cursor, because the
// pass writes as it goes.
func (p *winPass) readForward() error {
	cur, err := p.tx.OrdCursor(p.st.Name)
	if err != nil {
		return err
	}
	var e stagestore.OrdEntry
	var ok bool
	if p.after == nil {
		e, ok = cur.Seek(p.start)
	} else {
		e, ok = cur.Seek(p.after)
		if ok && bytes.Equal(e.Key, p.after) {
			e, ok = cur.Next()
		}
	}
	if !ok || !bytes.HasPrefix(e.Key, p.prefix) {
		p.eof = true
		return nil
	}
	row := p.newRow(e)
	p.after = row.key
	qualifies, err := p.g.loadWindowRow(p.tx, p.n, p.part, row)
	if err != nil {
		return err
	}
	if !qualifies {
		return p.g.windowDisqualify(p.tx, p.n, row.inKey, p.dirty)
	}
	var prev *winRow
	if len(p.seq) > 0 {
		prev = p.seq[len(p.seq)-1]
	}
	if row.state, err = p.advance(prev, row); err != nil {
		return err
	}
	p.seq = append(p.seq, row)
	p.fwdNext++
	return nil
}

// advance computes a row's running state from its predecessor's.
func (p *winPass) advance(prev, row *winRow) (*winState, error) {
	slots := p.st.winSlots
	ps := &winState{accs: make([]aggAcc, len(slots)), first: make([]any, len(slots))}
	if prev != nil {
		ps = prev.state
	}
	s := &winState{rn: ps.rn + 1, accs: make([]aggAcc, len(slots)), first: make([]any, len(slots)), hasFirst: true}
	if prev != nil && bytes.Equal(prev.peer, row.peer) {
		s.rank, s.dense = ps.rank, ps.dense
	} else {
		s.rank, s.dense = s.rn, ps.dense+1
	}
	for k := range slots {
		sl := &slots[k]
		if p.w.prefixSlot[k] {
			s.accs[k] = cloneAcc(ps.accs[k])
			agg := sl.agg()
			if err := agg.add(&s.accs[k], row.scope, nil); err != nil {
				return nil, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", p.st.Name, sl.fn, err))
			}
		}
		if p.w.firstSlot[k] {
			if ps.hasFirst {
				s.first[k] = ps.first[k]
			} else {
				v, err := Eval(sl.arg, row.scope, nil)
				if err != nil {
					return nil, cluster.Permanent(fmt.Errorf("stage %q first_value(…): %w", p.st.Name, err))
				}
				s.first[k] = v
			}
		}
	}
	return s, nil
}

// cloneAcc copies an accumulator so a later row's fold cannot reach an
// earlier row's state.
func cloneAcc(a aggAcc) aggAcc {
	if a.sum != nil {
		a.sum = new(big.Rat).Set(a.sum)
	}
	if a.ivSum != nil {
		iv := *a.ivSum
		a.ivSum = &iv
	}
	return a
}

// trim drops rows behind every reach of the group starting at cur.
func (p *winPass) trim(cur int) {
	keep := min(p.lowWater, cur-p.w.rowsBack-1)
	if keep <= p.base || p.w.fullPass {
		return
	}
	if keep-p.base >= len(p.seq) {
		keep = p.base + len(p.seq) - 1 // keep the last row: the next one continues from it
	}
	p.seq = p.seq[keep-p.base:]
	p.base = keep
	p.atStart = false
}

// value is a row's first order value (the RANGE frame's measure).
func (p *winPass) value(r *winRow) (any, error) {
	if !r.valDone {
		v, err := Eval(p.st.winOrder[0].expr, r.scope, nil)
		if err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q orderBy: %w", p.st.Name, err))
		}
		r.val, r.valDone = v, true
	}
	return r.val, nil
}

// boundKey is the sort key of a RANGE bound: the row's order value moved
// by off toward the preceding (back) or following rows.
func (p *winPass) boundKey(v, off any, back bool) ([]byte, error) {
	k := p.st.winOrder[0]
	bv, err := rangeBound(v, off, back, k.desc)
	if err != nil {
		return nil, cluster.Permanent(fmt.Errorf("stage %q: %w", p.st.Name, err))
	}
	enc, err := appendSortKey(nil, bv, k.desc, k.nullsFirst)
	if err != nil {
		return nil, cluster.Permanent(fmt.Errorf("stage %q: %w", p.st.Name, err))
	}
	return enc, nil
}

// rangeReachClear reports whether group start g's RANGE frames reach back
// only to rows past every change: a bounded start must lie beyond the last
// change, and a frame end measured back from g (with the start at the
// partition's first row) must still reach a row past it.
func (p *winPass) rangeReachClear(g *winRow, hiOrder, firstPastHi []byte) (bool, error) {
	if len(p.w.rangeStartBack) == 0 && len(p.w.rangeEndBack) == 0 {
		return true, nil
	}
	v, err := p.value(g)
	if err != nil || v == nil {
		return v == nil, err // a null order's frame is its peers, all past the changes
	}
	for _, off := range p.w.rangeStartBack {
		enc, err := p.boundKey(v, off, true)
		if err != nil {
			return false, err
		}
		if bytes.Compare(enc, hiOrder) <= 0 {
			return false, nil
		}
	}
	for _, off := range p.w.rangeEndBack {
		enc, err := p.boundKey(v, off, true)
		if err != nil {
			return false, err
		}
		if firstPastHi == nil || bytes.Compare(firstPastHi, enc) > 0 {
			return false, nil
		}
	}
	return true, nil
}

// firstPeer/lastPeer bound row i's peer group.
func (p *winPass) firstPeer(i int, r *winRow) (int, error) {
	for {
		pr, err := p.rowAt(i - 1)
		if err != nil || pr == nil || !bytes.Equal(pr.peer, r.peer) {
			return i, err
		}
		i--
	}
}

func (p *winPass) lastPeer(i int, r *winRow) (int, error) {
	for {
		nx, err := p.rowAt(i + 1)
		if err != nil || nx == nil || !bytes.Equal(nx.peer, r.peer) {
			return i, err
		}
		i++
	}
}

// lastRow is the partition's last position.
func (p *winPass) lastRow() (int, error) {
	for !p.eof {
		if err := p.readForward(); err != nil {
			return 0, err
		}
	}
	return p.fwdNext - 1, nil
}

// frameStart is the first position of row i's frame; ok=false when the
// frame starts past the partition's end (an empty frame).
func (p *winPass) frameStart(i int, r *winRow, f *winFrame) (int, bool, error) {
	b := f.start
	switch {
	case b.kind == boundCurrent && f.rows:
		return i, true, nil
	case b.kind == boundCurrent:
		j, err := p.firstPeer(i, r)
		return j, true, err
	case f.rows && b.kind == boundPreceding:
		for j := i - int(b.rows); j < i; j++ {
			if pr, err := p.rowAt(j); err != nil || pr != nil {
				return j, true, err
			}
		}
		return i, true, nil
	case f.rows: // n following
		nx, err := p.rowAt(i + int(b.rows))
		return i + int(b.rows), nx != nil, err
	}
	v, err := p.value(r)
	if err != nil {
		return 0, false, err
	}
	if v == nil {
		j, err := p.firstPeer(i, r)
		return j, true, err
	}
	enc, err := p.boundKey(v, b.off, b.kind == boundPreceding)
	if err != nil {
		return 0, false, err
	}
	if b.kind == boundPreceding {
		j := i
		for {
			pr, err := p.rowAt(j - 1)
			if err != nil {
				return 0, false, err
			}
			if pr == nil || bytes.Compare(pr.order, enc) < 0 {
				return j, true, nil
			}
			j--
		}
	}
	for j := i; ; j++ {
		nx, err := p.rowAt(j)
		if err != nil || nx == nil {
			return 0, false, err
		}
		if bytes.Compare(nx.order, enc) >= 0 {
			return j, true, nil
		}
	}
}

// frameEnd is the last position of row i's frame; ok=false when the frame
// ends before the partition's start (an empty frame).
func (p *winPass) frameEnd(i int, r *winRow, f *winFrame) (int, bool, error) {
	b := f.end
	switch {
	case b.kind == boundUnboundedFollowing:
		j, err := p.lastRow()
		return j, true, err
	case b.kind == boundCurrent && f.rows:
		return i, true, nil
	case b.kind == boundCurrent:
		j, err := p.lastPeer(i, r)
		return j, true, err
	case f.rows && b.kind == boundFollowing:
		for j := i + int(b.rows); j > i; j-- {
			if nx, err := p.rowAt(j); err != nil || nx != nil {
				return j, true, err
			}
		}
		return i, true, nil
	case f.rows: // n preceding
		pr, err := p.rowAt(i - int(b.rows))
		return i - int(b.rows), pr != nil, err
	}
	v, err := p.value(r)
	if err != nil {
		return 0, false, err
	}
	if v == nil {
		j, err := p.lastPeer(i, r)
		return j, true, err
	}
	enc, err := p.boundKey(v, b.off, b.kind == boundPreceding)
	if err != nil {
		return 0, false, err
	}
	if b.kind == boundFollowing {
		j := i
		for {
			nx, err := p.rowAt(j + 1)
			if err != nil {
				return 0, false, err
			}
			if nx == nil || bytes.Compare(nx.order, enc) > 0 {
				return j, true, nil
			}
			j++
		}
	}
	for j := i; ; j-- {
		pr, err := p.rowAt(j)
		if err != nil || pr == nil {
			return 0, false, err
		}
		if bytes.Compare(pr.order, enc) <= 0 {
			return j, true, nil
		}
	}
}

// frameValue evaluates one framed call (an aggregate, first_value,
// last_value) for row i: a frame starting at the partition's first row
// reads the running state at its end; any other frame folds its rows.
func (p *winPass) frameValue(i int, r *winRow, k int) (any, error) {
	s := &p.st.winSlots[k]
	f := s.frameOf()
	agg := s.agg()
	empty := func() any {
		if s.isAggregate() {
			return agg.result(&aggAcc{})
		}
		return nil
	}
	hi, ok, err := p.frameEnd(i, r, f)
	if err != nil || !ok {
		return empty(), err
	}
	hr, err := p.rowAt(hi)
	if err != nil || hr == nil {
		return empty(), err
	}
	argAt := func(row *winRow) (any, error) {
		v, err := Eval(s.arg, row.scope, nil)
		if err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", p.st.Name, s.fn, err))
		}
		return v, nil
	}
	if f.start.kind == boundUnboundedPreceding {
		switch {
		case s.isAggregate():
			return agg.result(&hr.state.accs[k]), nil
		case s.fn == "first_value":
			return r.state.first[k], nil
		default:
			return argAt(hr)
		}
	}
	lo, ok, err := p.frameStart(i, r, f)
	if err != nil || !ok || lo > hi {
		return empty(), err
	}
	switch s.fn {
	case "first_value":
		lr, err := p.rowAt(lo)
		if err != nil || lr == nil {
			return nil, err
		}
		return argAt(lr)
	case "last_value":
		return argAt(hr)
	}
	var acc aggAcc
	for j := lo; j <= hi; j++ {
		row, err := p.rowAt(j)
		if err != nil {
			return nil, err
		}
		if err := agg.add(&acc, row.scope, nil); err != nil {
			return nil, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", p.st.Name, s.fn, err))
		}
	}
	return agg.result(&acc), nil
}

// computeRow evaluates row i's window calls and emit, writes its state
// and (when changed) its output; same reports whether its state came out
// the same as stored, as far as emitted values read it.
func (p *winPass) computeRow(i int) (same bool, err error) {
	st := p.st
	row := p.seq[i-p.base]
	res := make([]any, len(st.winSlots))
	for k := range st.winSlots {
		s := &st.winSlots[k]
		var v any
		switch s.fn {
		case "row_number":
			v = new(big.Rat).SetInt64(row.state.rn)
		case "rank":
			v = new(big.Rat).SetInt64(row.state.rank)
		case "dense_rank":
			v = new(big.Rat).SetInt64(row.state.dense)
		case "lag", "lead":
			t := i - int(s.offset)
			if s.fn == "lead" {
				t = i + int(s.offset)
			}
			target, err := p.rowAt(t)
			if err != nil {
				return false, err
			}
			switch {
			case target != nil:
				v, err = Eval(s.arg, target.scope, nil)
			case s.def != nil:
				v, err = Eval(s.def, row.scope, nil)
			}
			if err != nil {
				return false, cluster.Permanent(fmt.Errorf("stage %q %s(…): %w", st.Name, s.fn, err))
			}
		default:
			if v, err = p.frameValue(i, row, k); err != nil {
				return false, err // data errors arrive wrapped Permanent; store errors wedge
			}
		}
		res[k] = v
	}
	// The sequence may have grown backward while evaluating (a frame or
	// lag reaching behind it); row is unaffected, positions are absolute.
	out, err := p.g.emitWindowRow(st, row.scope, res)
	if err != nil {
		return false, err
	}
	p.w.recomputed.Add(1)
	stateBytes, err := encodeWinState(row.state)
	if err != nil {
		return false, err
	}
	old, err := p.tx.GetRow(st.Name, row.inKey)
	if err != nil {
		return false, err
	}
	if old != nil {
		if bytes.Equal(old, stateBytes) {
			same = true
		} else if prior, err := decodeWinState(old, len(st.winSlots)); err == nil {
			a, errA := p.w.witness(prior)
			b, errB := p.w.witness(row.state)
			same = errA == nil && errB == nil && bytes.Equal(a, b)
		}
	}
	if !bytes.Equal(old, stateBytes) {
		if err := p.tx.PutRow(st.Name, row.inKey, stateBytes); err != nil {
			return false, err
		}
	}
	prior, err := p.tx.GetOut(st.Name, row.inKey)
	if err != nil {
		return false, err
	}
	if prior != nil && bytes.Equal(prior, out) {
		return same, nil
	}
	return same, p.g.publishOutput(p.tx, p.n, row.inKey, out, p.dirty)
}
