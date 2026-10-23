package stages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The fold state of the winner-shaped reduces — reshape, latest, liveSet —
// whose output is one input's emitted object: reshape's bytewise-largest
// qualifying input key, latest's greatest by its order (and tie) keys,
// liveSet's largest qualifying input unless any retained input is delete
// evidence. A key keeps its winner (and latest's rank of it), its count
// of retained inputs, and liveSet's count of delete evidence, so a changed
// input is compared with the winner instead of every input refolding:
//
//   - an input that beats the winner becomes it, and the output re-emits
//     from that input alone;
//   - an input that does not, or that leaves without having been the
//     winner, moves nothing (reshape and latest read nothing to retract an
//     input; liveSet decodes the old input to know whether it was delete
//     evidence);
//   - the winner changing to a value that still ranks at least as high
//     re-emits it; the winner leaving, failing its filters, or ranking lower
//     refolds the key in full, as do a key with no state yet and a key marked
//     full (a join's dimension row moved).
//
// As with aggregate fold states (agg_incremental.go), it is an evaluation
// strategy under the refold contract: the state is what the full refold
// over the same retained inputs computes, so the output bytes are equal.

// winnerReduce reports whether a stage keeps a winner state.
func (st *Stage) winnerReduce() bool {
	switch st.Reduce {
	case "", "latest", "liveSet":
		return len(st.Merge) == 0
	}
	return false
}

// keepsFoldState reports whether a stage keeps a fold state, so its drain
// needs each key's input changes.
func (st *Stage) keepsFoldState() bool {
	return st.incrementalAggregate() || st.winnerReduce()
}

// errWinnerFallback asks for a full refold of the key.
var errWinnerFallback = errors.New("winner key needs a full refold")

// winnerState is a winner-shaped key's fold state.
type winnerState struct {
	inputs uint64 // retained inputs
	dead   uint64 // liveSet: retained inputs that are delete evidence
	inKey  []byte // the winner; nil when no input qualifies
	rank   []byte // latest: the winner's rank (latestRank)
}

// winnerScan is a full refold's result: the state, and the winner's input.
type winnerScan struct {
	state       winnerState
	obj, parent any
}

// scanWinner refolds a key over every retained input — the plain refold
// of reshape, latest and liveSet, and the state it leaves.
func (g *Graph) scanWinner(tx *stagestore.Tx, st *Stage, outKey []byte) (*winnerScan, error) {
	ws := &winnerScan{}
	err := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		ws.state.inputs++
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		rank, eligible, err := g.winnerCandidate(tx, st, inKey, obj, parent)
		if err != nil {
			return err
		}
		if eligible == candidateEvidence {
			ws.state.dead++
			return nil
		}
		if eligible != candidateEligible {
			return nil
		}
		if ws.state.inKey == nil || winnerBeats(st, inKey, rank, &ws.state) {
			ws.state.inKey, ws.state.rank = append([]byte(nil), inKey...), rank
			ws.obj, ws.parent = obj, parent
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ws, nil
}

// live reports whether a key with this state has an output.
func (s *winnerState) live() bool {
	return s.inKey != nil && s.dead == 0
}

// emitWinner renders a scanned key's output.
func (g *Graph) emitWinner(tx *stagestore.Tx, st *Stage, ws *winnerScan) ([]byte, bool, error) {
	if !ws.state.live() {
		return nil, false, nil
	}
	return g.emitReshape(tx, st, ws.obj, ws.parent, st.inputSource(ws.state.inKey))
}

// candidateKind is how one input stands in its key.
type candidateKind int

const (
	candidateIneligible candidateKind = iota // filtered out by a join, or its latest order failed
	candidateEligible                        // may win
	candidateEvidence                        // liveSet delete evidence
)

// winnerCandidate classifies one input exactly as the full refold does, and
// ranks it (latest only).
func (g *Graph) winnerCandidate(tx *stagestore.Tx, st *Stage, inKey []byte, obj, parent any) ([]byte, candidateKind, error) {
	src := st.inputSource(inKey)
	if st.Reduce == "liveSet" && Match(st.DeleteWhen, withSource(obj, src)) {
		return nil, candidateEvidence, nil
	}
	if ok, err := g.inputQualifies(tx, st, obj, parent); err != nil || !ok {
		return nil, candidateIneligible, err
	}
	if st.Reduce != "latest" {
		return nil, candidateEligible, nil
	}
	if len(st.winOrder) > 0 {
		key, err := latestSortKey(st, withSource(obj, src), parent)
		if err != nil {
			g.noteLatestOrderError(st, err)
			return nil, candidateIneligible, nil
		}
		return key, candidateEligible, nil
	}
	order, _ := ResolvePath(st.OrderBy, obj, parent)
	tie, _ := ResolvePath(st.TieBy, obj, parent)
	rank := appendRankPart(nil, order, st.OrderByType == KeyTypeNumber)
	return appendRankPart(rank, tie, st.TieByType == KeyTypeNumber), candidateEligible, nil
}

// winnerBeats reports whether an eligible input displaces the current
// winner, as the full refold's scan in input-key order decides.
func winnerBeats(st *Stage, inKey, rank []byte, s *winnerState) bool {
	if st.Reduce != "latest" {
		return bytes.Compare(inKey, s.inKey) > 0 // the largest input key wins
	}
	c := compareRank(st, rank, s.rank)
	if len(st.winOrder) > 0 {
		// The list form: an exact tie goes to the larger input key.
		return c > 0 || (c == 0 && bytes.Compare(inKey, s.inKey) > 0)
	}
	// The path form keeps the first of an exact tie in input-key order.
	return c > 0 || (c == 0 && bytes.Compare(inKey, s.inKey) < 0)
}

// compareRank orders two latest ranks: list-form sort keys bytewise, path
// form by order then tie, as compareStageValues compares them.
func compareRank(st *Stage, a, b []byte) int {
	if len(st.winOrder) > 0 {
		return bytes.Compare(a, b)
	}
	ao, arest := splitRankPart(a)
	bo, brest := splitRankPart(b)
	if c := compareRankPart(ao, bo, st.OrderByType == KeyTypeNumber); c != 0 {
		return c
	}
	at, _ := splitRankPart(arest)
	bt, _ := splitRankPart(brest)
	return compareRankPart(at, bt, st.TieByType == KeyTypeNumber)
}

// A path-form rank part is a value in compareStageValues' comparable form:
// absent (nil, or not comparable in its declared type), or its text — a
// string, or an exact rational's.

func appendRankPart(b []byte, v any, numeric bool) []byte {
	c, ok := stageComparable(v, numeric)
	if !ok {
		return appendPart(b, nil)
	}
	if numeric {
		return appendPart(b, []byte(c.(*big.Rat).RatString()))
	}
	return appendPart(b, []byte(c.(string)))
}

// splitRankPart returns a rank's first part (nil: absent) and the rest.
func splitRankPart(b []byte) ([]byte, []byte) {
	part, rest, _ := splitPart(b)
	return part, rest
}

// appendPart writes an optional byte string: its length + 1, then its
// bytes; 0 for none (nil).
func appendPart(b, part []byte) []byte {
	if part == nil {
		return binary.AppendUvarint(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(part))+1)
	return append(b, part...)
}

// splitPart reads what appendPart wrote: the part (nil for none), the rest,
// and whether b held one.
func splitPart(b []byte) (part, rest []byte, ok bool) {
	n, w := binary.Uvarint(b)
	if w <= 0 {
		return nil, nil, false
	}
	if n == 0 {
		return nil, b[w:], true
	}
	if n-1 > uint64(len(b)-w) { //nolint:gosec // G115: a length is never negative
		return nil, nil, false
	}
	end := w + int(n-1) //nolint:gosec // G115: bounded by len(b) above
	return b[w:end:end], b[end:], true
}

func compareRankPart(a, b []byte, numeric bool) int {
	switch {
	case a == nil && b == nil:
		return 0
	case b == nil:
		return 1
	case a == nil:
		return -1
	}
	if numeric {
		ar, _ := new(big.Rat).SetString(string(a))
		br, _ := new(big.Rat).SetString(string(b))
		return ar.Cmp(br)
	}
	return bytes.Compare(a, b)
}

// refoldWinner refolds a winner-shaped key from its state and this drain's
// input changes, or in full when the state cannot show the result. Either
// way the key's state is left in step with its retained set.
func (g *Graph) refoldWinner(tx *stagestore.Tx, st *Stage, outKey []byte, delta *aggDelta) ([]byte, bool, error) {
	n := g.byName[st.Name]
	if delta != nil {
		out, live, err := g.tryWinner(tx, st, outKey, delta)
		if err == nil {
			n.foldStateRefolds.Add(1)
			if checkFoldState != nil {
				checkFoldState(g, tx, st, outKey, out, live)
			}
			return out, live, nil
		}
		if !errors.Is(err, errWinnerFallback) {
			return nil, false, err
		}
	}
	n.fullRefolds.Add(1)
	ws, err := g.scanWinner(tx, st, outKey)
	if err != nil {
		return nil, false, err
	}
	out, live, err := g.emitWinner(tx, st, ws)
	if err != nil {
		return nil, false, err
	}
	return out, live, storeWinnerState(tx, st, outKey, &ws.state)
}

func (g *Graph) tryWinner(tx *stagestore.Tx, st *Stage, outKey []byte, delta *aggDelta) ([]byte, bool, error) {
	raw, err := tx.GetAcc(st.Name, outKey)
	if err != nil {
		return nil, false, err
	}
	s, err := decodeWinnerState(raw)
	if err != nil {
		return nil, false, errWinnerFallback
	}
	wasLive := s.live()
	keys := make([]string, 0, len(delta.inputs))
	for k := range delta.inputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// win is the winner's input when this drain changed it (nil: the winner
	// is the one already emitted).
	var win *decodedInput
	for _, k := range keys {
		ch := delta.inputs[k]
		if bytes.Equal(ch.old, ch.cur) {
			continue // re-retained unchanged (a redelivery): nothing moves
		}
		inKey := []byte(k)
		if ch.old != nil {
			s.inputs--
			if st.Reduce == "liveSet" {
				old, err := decodeInput(st, ch.old)
				if err != nil {
					return nil, false, errWinnerFallback
				}
				if Match(st.DeleteWhen, withSource(old.data, st.inputSource(inKey))) {
					s.dead--
				}
			}
		}
		isWinner := s.inKey != nil && bytes.Equal(inKey, s.inKey)
		if ch.cur == nil {
			if isWinner {
				return nil, false, errWinnerFallback
			}
			continue
		}
		s.inputs++
		cur := ch.curIn
		if cur == nil {
			if cur, err = decodeInput(st, ch.cur); err != nil {
				return nil, false, errWinnerFallback
			}
		}
		rank, kind, err := g.winnerCandidate(tx, st, inKey, cur.data, cur.parent)
		if err != nil {
			return nil, false, errWinnerFallback
		}
		if kind == candidateEvidence {
			s.dead++
		}
		switch {
		case isWinner && kind != candidateEligible:
			return nil, false, errWinnerFallback
		case isWinner:
			if st.Reduce == "latest" && compareRank(st, rank, s.rank) < 0 {
				return nil, false, errWinnerFallback // it may no longer lead
			}
			s.rank, win = rank, cur
		case kind == candidateEligible && (s.inKey == nil || winnerBeats(st, inKey, rank, s)):
			s.inKey, s.rank, win = inKey, rank, cur
		}
	}
	if s.inputs > 1<<62 || s.dead > s.inputs {
		return nil, false, errWinnerFallback // a count went below zero
	}
	if err := storeWinnerState(tx, st, outKey, s); err != nil {
		return nil, false, err
	}
	if !s.live() {
		return nil, false, nil
	}
	if win == nil && wasLive {
		// The winner, its value and the key's liveness are unchanged: so is
		// the output.
		out, err := tx.GetOut(st.Name, outKey)
		if err != nil {
			return nil, false, err
		}
		if out != nil {
			return out, true, nil
		}
	}
	if win == nil {
		// Live again (liveSet evidence left) with an unchanged winner: read it.
		raw, err := tx.GetIn(st.Name, outKey, s.inKey)
		if err != nil {
			return nil, false, err
		}
		if raw == nil {
			return nil, false, errWinnerFallback
		}
		if win, err = decodeInput(st, raw); err != nil {
			return nil, false, errWinnerFallback
		}
	}
	out, live, err := g.emitReshape(tx, st, win.data, win.parent, st.inputSource(s.inKey))
	if err != nil {
		return nil, false, errWinnerFallback // the full refold raises it
	}
	return out, live, nil
}

// decodeInput decodes a retained input.
func decodeInput(st *Stage, retained []byte) (*decodedInput, error) {
	_, payload := decodeRetained(retained)
	data, parent, err := unpackStageInput(st, payload)
	if err != nil {
		return nil, err
	}
	return &decodedInput{data: data, parent: parent}, nil
}

// The stored winner state: a version byte (distinct from an aggregate
// state's, so a store reader can tell the two apart), the input and
// evidence counts, the winner key (length + 1; 0 for none), and the rank.

const winnerStateV1 = 0x81

func storeWinnerState(tx *stagestore.Tx, st *Stage, outKey []byte, s *winnerState) error {
	if s.inputs == 0 {
		return tx.DeleteAcc(st.Name, outKey)
	}
	b := make([]byte, 0, 16+len(s.inKey)+len(s.rank))
	b = append(b, winnerStateV1)
	b = binary.AppendUvarint(b, s.inputs)
	b = binary.AppendUvarint(b, s.dead)
	b = appendPart(b, s.inKey)
	b = append(b, s.rank...)
	return tx.PutAcc(st.Name, outKey, b)
}

func decodeWinnerState(raw []byte) (*winnerState, error) {
	if len(raw) == 0 || raw[0] != winnerStateV1 {
		return nil, fmt.Errorf("no winner state")
	}
	b := raw[1:]
	s := &winnerState{}
	var w int
	if s.inputs, w = binary.Uvarint(b); w <= 0 {
		return nil, fmt.Errorf("winner state: inputs")
	}
	b = b[w:]
	if s.dead, w = binary.Uvarint(b); w <= 0 {
		return nil, fmt.Errorf("winner state: evidence")
	}
	b = b[w:]
	inKey, b, ok := splitPart(b)
	if !ok {
		return nil, fmt.Errorf("winner state: winner")
	}
	if inKey != nil {
		s.inKey = append([]byte(nil), inKey...)
	}
	if len(b) > 0 {
		s.rank = append([]byte(nil), b...)
	}
	return s, nil
}
