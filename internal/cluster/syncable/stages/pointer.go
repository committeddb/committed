package stages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// Retained inputs as pointers. A stage that reads a topic keeps each input
// as a pointer into the event log — the raft index of its entry, plus the
// migration the input arrived through — instead of a copy of its data,
// when the graph has a log to read it back from (Graph.Log; a projection's
// worker has one, a dry run does not). A refold that needs the input reads
// it back (loadInput).
//
// A forEach stage's element of a topic input points to the input's entity:
// reading it back re-runs the fan over the entity and takes the element
// with its identity (fanInKey's element key), with the entity as its
// parent.
//
// Inputs of other kinds stay copies: stage-fed inputs and merge sides (the
// log holds no such entity), fan-join pairs (each pairs its element with a
// dimension row), window rows (a recompute reads every row it walks), and
// join dimension rows (every input's lookup reads its row).
//
// A pointer's entity can be gone from the log: a scrub erases an entity
// once a later entry for its key deletes or supersedes it, and a stage
// lagging behind the scrub still points to it until it folds that later
// entry, which retracts the pointer. Until then the input is absent
// (cluster.EntityReader), as a fold over the scrubbed log would have it:
// a full refold leaves it out, and a fold state that would have to read it
// refolds the key in full instead.

// inputOrigin is where a topic input came from: the raft index of its
// entry, and the type versions its data was migrated between on its way to
// the stage (from 0: as stored).
type inputOrigin struct {
	index    uint64
	from, to int
}

// retainedPointerTag starts a pointer's payload in place of a copy's JSON,
// which never starts with it.
const retainedPointerTag = 0x00

// errInputAbsent is a pointer whose entity the log no longer holds.
var errInputAbsent = errors.New("a retained input's entity was scrubbed from the log")

// encodeRetainedPointer is encodeRetained for a pointer.
func encodeRetainedPointer(gen uint64, o *inputOrigin) []byte {
	b := make([]byte, 8, 9+3*binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(b, gen)
	b = append(b, retainedPointerTag)
	b = binary.AppendUvarint(b, o.index)
	b = binary.AppendUvarint(b, uint64(o.from))  //nolint:gosec // G115: type versions are positive
	return binary.AppendUvarint(b, uint64(o.to)) //nolint:gosec // G115: type versions are positive
}

// decodeRetainedPointer reads a pointer from a retained payload (the value
// after its generation); ok is false for a copy.
func decodeRetainedPointer(payload []byte) (*inputOrigin, bool, error) {
	if len(payload) == 0 || payload[0] != retainedPointerTag {
		return nil, false, nil
	}
	b := payload[1:]
	var o inputOrigin
	var vals [3]uint64
	for i := range vals {
		v, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, true, fmt.Errorf("malformed retained pointer")
		}
		vals[i], b = v, b[w:]
	}
	o.index = vals[0]
	o.from, o.to = int(vals[1]), int(vals[2]) //nolint:gosec // G115: written from ints above
	return &o, true, nil
}

// keepsPointers reports whether a stage keeps its topic inputs as pointers.
func (n *graphNode) keepsPointers() bool {
	st := n.def
	return n.fanJoin < 0 && n.win == nil && len(st.Merge) == 0
}

// splitFanInKey is fanInKey's inverse: the parent's input key.
func splitFanInKey(elemIn []byte) ([]byte, bool) {
	l, w := binary.Uvarint(elemIn)
	if w <= 0 || uint64(len(elemIn)-w) < l { //nolint:gosec // G115: a length, never negative
		return nil, false
	}
	return elemIn[w : w+int(l)], true //nolint:gosec // G115: bounded by len above
}

// inputEntity names the topic and entity key a topic input's identity
// stands for.
func (st *Stage) inputEntity(inKey []byte) (string, []byte, bool) {
	if !st.isUnion() {
		return st.From, inKey, true
	}
	parts, err := cluster.DecodeCompositeKey(string(inKey), 2)
	if err != nil {
		return "", nil, false
	}
	return parts[0], []byte(parts[1]), true
}

// loadInput decodes a retained input: a copy in place, a pointer read back
// from the log. errInputAbsent is a pointer whose entity a scrub erased.
func (g *Graph) loadInput(st *Stage, inKey, retained []byte) (*decodedInput, error) {
	_, payload := decodeRetained(retained)
	o, isPointer, err := decodeRetainedPointer(payload)
	if err != nil {
		return nil, fmt.Errorf("stage %q: %w", st.Name, err)
	}
	if isPointer && g.refolding != nil {
		if ch := g.refolding.inputs[string(inKey)]; ch != nil && ch.curIn != nil && bytes.Equal(ch.cur, retained) {
			return ch.curIn, nil // folded in this batch: no need to read it back
		}
	}
	if !isPointer {
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return nil, err
		}
		return &decodedInput{data: data, parent: parent}, nil
	}
	if g.Log == nil {
		return nil, fmt.Errorf("stage %q: a retained input points into the log, and this graph has no log to read it from", st.Name)
	}
	entityIn := inKey
	if st.hasFan() {
		var ok bool
		if entityIn, ok = splitFanInKey(inKey); !ok {
			return nil, fmt.Errorf("stage %q: a retained element's identity names no parent", st.Name)
		}
	}
	topic, key, ok := st.inputEntity(entityIn)
	if !ok {
		return nil, fmt.Errorf("stage %q: a retained input's identity names no entity", st.Name)
	}
	if st.hasFan() {
		return g.loadElement(st, topic, entityIn, key, inKey, o)
	}
	obj, err := g.readEntity(st, topic, key, o)
	if err != nil {
		return nil, err
	}
	return &decodedInput{data: obj}, nil
}

// readEntity reads a pointer's entity back from the log, decoded.
func (g *Graph) readEntity(st *Stage, topic string, key []byte, o *inputOrigin) (any, error) {
	data, found, err := g.Log.ReadEntity(o.index, topic, key, o.from, o.to)
	if err != nil {
		return nil, fmt.Errorf("stage %q: read a retained input back: %w", st.Name, err)
	}
	if !found {
		if n := g.byName[st.Name]; n != nil {
			n.scrubbedInputs.Add(1)
		}
		return nil, errInputAbsent
	}
	obj, err := DecodeObject(data)
	if err != nil {
		return nil, cluster.Permanent(fmt.Errorf("[projection.stage] unmarshal entity data: %w", err))
	}
	return obj, nil
}

// fannedParent is the last entity loadElement fanned, by its pointer: a
// refold reads a key's inputs in identity order, so the elements of one
// entity come together.
type fannedParent struct {
	stage, topic string
	key          []byte
	origin       inputOrigin
	parent       any
	elems        map[string]any
}

// loadElement reads a forEach stage's element back: its parent entity
// from the log, fanned again.
func (g *Graph) loadElement(st *Stage, topic string, parentIn, key, elemIn []byte, o *inputOrigin) (*decodedInput, error) {
	f := g.lastFanned
	if f == nil || f.stage != st.Name || f.topic != topic || f.origin != *o || !bytes.Equal(f.key, key) {
		parent, err := g.readEntity(st, topic, key, o)
		if err != nil {
			return nil, err
		}
		n := g.byName[st.Name]
		if n == nil {
			return nil, fmt.Errorf("stage %q: not in the graph", st.Name)
		}
		elems, _, _, _ := n.fanOut(topic, parentIn, parent)
		f = &fannedParent{stage: st.Name, topic: topic, key: key, origin: *o, parent: parent, elems: make(map[string]any, len(elems))}
		for _, e := range elems {
			f.elems[string(e.in)] = e.el
		}
		g.lastFanned = f
	}
	el, ok := f.elems[string(elemIn)]
	if !ok {
		// The entry fans the same elements it did when the element was
		// kept: the same bytes through the same migration.
		return nil, fmt.Errorf("stage %q: a retained element is not among its parent's elements", st.Name)
	}
	return &decodedInput{data: el, parent: f.parent}, nil
}

// testInputLog, when set, stands in for the log for the inputs this
// package's tests fold: it keeps each topic input it is handed and returns
// the pointer to it, so the whole suite folds through pointers and reads
// back through a log (see plan_test.go).
var testInputLog func(g *Graph, topic string, key []byte, payload any) *inputOrigin

// originOf is the pointer a topic entity's input is kept as, nil for a
// copy: the entity's place in the log when the graph can read it back.
func (g *Graph) originOf(a *cluster.Actual, e *cluster.Entity, payload any) *inputOrigin {
	if testInputLog != nil {
		return testInputLog(g, e.ID, e.Key, payload)
	}
	if g.Log == nil || a == nil {
		return nil
	}
	return &inputOrigin{index: a.Index, from: e.MigratedFrom, to: e.Version}
}
