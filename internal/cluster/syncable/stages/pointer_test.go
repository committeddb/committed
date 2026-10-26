package stages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// suiteLog is the stand-in event log the whole suite folds topic inputs
// through (testInputLog, set in TestMain): one log for every graph, since a
// test may reopen a store under a new graph, with scrubs a test can apply.
var suiteLog = &fakeLog{entries: map[uint64]fakeEntity{}, scrubbed: map[uint64]bool{}}

type fakeLog struct {
	mu       sync.Mutex
	next     uint64
	entries  map[uint64]fakeEntity
	scrubbed map[uint64]bool
	bound    uint64 // every scrubbed index is at or below it
}

type fakeEntity struct {
	topic     string
	key, data []byte
}

func (l *fakeLog) append(topic string, key []byte, payload any) uint64 {
	data, err := marshalStageObject(payload)
	if err != nil {
		panic(err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	l.entries[l.next] = fakeEntity{topic: topic, key: bytes.Clone(key), data: data}
	return l.next
}

// scrub erases the entity at index, as a scrub erases a deleted or
// superseded one.
func (l *fakeLog) scrub(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.scrubbed[index] = true
	l.bound = max(l.bound, index)
}

func (l *fakeLog) ReadEntity(index uint64, topic string, key []byte, from, to int) ([]byte, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.scrubbed[index] {
		return nil, false, nil
	}
	e, ok := l.entries[index]
	if !ok || e.topic != topic || !bytes.Equal(e.key, key) {
		return nil, false, fmt.Errorf("no %s entity %q at index %d", topic, key, index)
	}
	if from != 0 || to != 0 {
		return nil, false, fmt.Errorf("the suite's log migrates nothing")
	}
	return e.data, true, nil
}

// suiteInputLog is testInputLog for the suite: the input goes into the
// suite's log and the graph reads it back from there.
func suiteInputLog(g *Graph, topic string, key []byte, payload any) *inputOrigin {
	if g.Log == nil {
		g.Log = suiteLog
	}
	return &inputOrigin{index: suiteLog.append(topic, key, payload)}
}

// renderRetained renders an in bucket's value for the store reference: a
// pointer as the data it reads back (its index depends on the tests run
// before), a copy as stored.
func renderRetained(v []byte) []byte {
	gen, payload := decodeRetained(v)
	o, ok, err := decodeRetainedPointer(payload)
	if err != nil || !ok {
		return v
	}
	suiteLog.mu.Lock()
	e, found := suiteLog.entries[o.index]
	suiteLog.mu.Unlock()
	if !found {
		return v
	}
	return fmt.Appendf(nil, "pointer(gen %d) %s", gen, e.data)
}

// In production a projection's graph has the engine's reader: a topic
// entity's input is kept as a pointer naming its entry and the migration it
// arrived through, read back the same way.
func TestTopicInputsArePointersWithALog(t *testing.T) {
	log, check := testInputLog, checkFoldState
	testInputLog, checkFoldState = nil, nil // count only the graph's own reads
	defer func() { testInputLog, checkFoldState = log, check }()
	sts := []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"}, Reduce: "latest", OrderBy: "$.o", OrderByType: "number", TieBy: "$.id",
		Emit: []Emit{{Field: "o", From: "$.o"}},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	reads := &recordingReader{data: map[uint64]string{}}
	g.Log = reads
	store := openTestStore(t, Fingerprint(sts))
	rows := &cluster.Type{ID: "rows", Version: 3}
	fold := func(index uint64, key, data string, migratedFrom int) {
		reads.data[index] = data
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldActual(tx, &cluster.Actual{Index: index, Entities: []*cluster.Entity{
				{Type: rows, Key: []byte(key), Data: []byte(data), MigratedFrom: migratedFrom},
			}})
		}))
	}
	fold(10, "a", `{"k":"x","o":2,"id":"a"}`, 1)
	fold(11, "b", `{"k":"x","o":1,"id":"b"}`, 0)
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		return tx.InputsFor("g", []byte("x"), func(inKey, val []byte) error {
			_, payload := decodeRetained(val)
			_, ok, err := decodeRetainedPointer(payload)
			require.NoError(t, err)
			require.True(t, ok, "input %s is a copy", inKey)
			return nil
		})
	}))
	// The winner leaves: the key refolds from its inputs, read back.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldActual(tx, &cluster.Actual{Index: 12, Entities: []*cluster.Entity{cluster.NewDeleteEntity(rows, []byte("a"))}})
	}))
	require.Equal(t, []string{"11 rows b 0→3"}, reads.calls)
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		out, err := g.Output(tx, "g", []byte("x"))
		require.NoError(t, err)
		require.JSONEq(t, `{"o":1}`, string(out))
		return nil
	}))
	reads.calls = nil
	fold(13, "c", `{"k":"x","o":0,"id":"c"}`, 2)
	require.Empty(t, reads.calls, "an input that does not beat the winner reads nothing")
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldActual(tx, &cluster.Actual{Index: 14, Entities: []*cluster.Entity{cluster.NewDeleteEntity(rows, []byte("b"))}})
	}))
	require.Equal(t, []string{"13 rows c 2→3"}, reads.calls, "a migrated input reads back through its migration")
}

// A forEach stage's elements of a topic entity point to the entity: a
// refold reads the entity back once and fans it again for each of its
// elements it needs.
func TestFanElementsArePointersToTheirEntity(t *testing.T) {
	log, check := testInputLog, checkFoldState
	testInputLog, checkFoldState = nil, nil // count only the graph's own reads
	defer func() { testInputLog, checkFoldState = log, check }()
	sts := []Stage{{
		Name: "g", From: "orders", ForEach: "$.lines[*]", ElementKey: "$.sku", KeyPath: []string{"$parent.cust"},
		Reduce: "latest", OrderBy: "$.q", OrderByType: "number", TieBy: "$.sku",
		Emit: []Emit{{Field: "sku", From: "$.sku"}, {Field: "order", From: "$parent.id"}},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	reads := &recordingReader{data: map[uint64]string{}}
	g.Log = reads
	store := openTestStore(t, Fingerprint(sts))
	orders := &cluster.Type{ID: "orders", Version: 1}
	fold := func(a *cluster.Actual) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error { return g.FoldActual(tx, a) }))
	}
	upsert := func(index uint64, key, data string) {
		reads.data[index] = data
		fold(&cluster.Actual{Index: index, Entities: []*cluster.Entity{{Type: orders, Key: []byte(key), Data: []byte(data)}}})
	}
	out := func() string {
		var got []byte
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			var err error
			got, err = g.Output(tx, "g", []byte("x"))
			return err
		}))
		return string(got)
	}
	upsert(10, "o1", `{"id":"o1","cust":"x","lines":[{"sku":"a","q":2},{"sku":"b","q":5}]}`)
	upsert(11, "o2", `{"id":"o2","cust":"x","lines":[{"sku":"c","q":3},{"sku":"d","q":4},{"sku":"e","q":1}]}`)
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		n := 0
		err := tx.InputsFor("g", []byte("x"), func(inKey, val []byte) error {
			n++
			_, payload := decodeRetained(val)
			_, ok, err := decodeRetainedPointer(payload)
			require.NoError(t, err)
			require.True(t, ok, "element %q is a copy", inKey)
			return nil
		})
		require.Equal(t, 5, n)
		return err
	}))
	require.Empty(t, reads.calls)
	require.JSONEq(t, `{"sku":"b","order":"o1"}`, out())

	// The winner's entity is deleted: the key refolds from o2's elements,
	// one read of o2.
	fold(&cluster.Actual{Index: 12, Entities: []*cluster.Entity{cluster.NewDeleteEntity(orders, []byte("o1"))}})
	require.Equal(t, []string{"11 orders o2 0→1"}, reads.calls)
	require.JSONEq(t, `{"sku":"d","order":"o2"}`, out())
}

// A forEach stage lagging behind a scrub of an entity it fanned leaves the
// entity's elements out of a full refold until the delete folds, as for
// any topic input.
func TestScrubbedEntityLeavesItsElementsOut(t *testing.T) {
	sts := []Stage{{
		Name: "g", From: "orders", ForEach: "$.lines[*]", ElementKey: "$.sku", KeyPath: []string{"$parent.cust"},
		Reduce: "aggregate", Emit: []Emit{{Field: "n", Expr: "count(*)"}, {Field: "q", Expr: "sum($.q)"}},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := openTestStore(t, Fingerprint(sts))
	var last uint64
	up := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "orders", []byte(key), decodePayload(t, payload))
		}))
		last = suiteLog.next
	}
	out := func() string {
		var got []byte
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			var err error
			got, err = g.Output(tx, "g", []byte("x"))
			return err
		}))
		return string(got)
	}
	full := func() string {
		var want []byte
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			w, _, err := g.refoldOutputBytes(tx, g.byName["g"].def, []byte("x"))
			want = w
			return err
		}))
		return string(want)
	}
	up("o1", `{"cust":"x","lines":[{"sku":"a","q":1},{"sku":"b","q":2}]}`)
	up("o2", `{"cust":"x","lines":[{"sku":"c","q":100},{"sku":"d","q":200}]}`)
	suiteLog.scrub(last) // o2's delete is further on in the log
	require.JSONEq(t, `{"n":2,"q":3}`, full(), "a full refold leaves the scrubbed entity's elements out")
	require.JSONEq(t, `{"n":4,"q":303}`, out(), "the fold state still holds them")
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "orders", []byte("o2"))
	}))
	require.JSONEq(t, `{"n":2,"q":3}`, out(), "once o2's delete folds")
	require.Equal(t, full(), out())
}

type recordingReader struct {
	data  map[uint64]string
	calls []string
}

func (r *recordingReader) ReadEntity(index uint64, topic string, key []byte, from, to int) ([]byte, bool, error) {
	r.calls = append(r.calls, fmt.Sprintf("%d %s %s %d→%d", index, topic, key, from, to))
	d, ok := r.data[index]
	return []byte(d), ok, nil
}

// A stage lagging behind a scrub holds a pointer to an entity the scrub
// erased (a later entry deletes or supersedes it). Until the stage folds
// that entry, a full refold leaves the input out — what a fold over the
// scrubbed log computes — while a fold state still holds its contribution;
// the delete's retraction cannot read the input, so it refolds the key in
// full, and from there the key matches a full refold again.
func TestScrubbedInputIsAbsentUntilItsDeleteFolds(t *testing.T) {
	for name, st := range map[string]Stage{
		"aggregate": {Reduce: "aggregate", Emit: []Emit{{Field: "n", Expr: "count(*)"}, {Field: "s", Expr: "sum($.o)"}, {Field: "mx", Expr: "max($.o)"}}},
		"latest":    {Reduce: "latest", OrderBy: "$.o", OrderByType: "number", TieBy: "$.id", Emit: []Emit{{Field: "o", From: "$.o"}}},
		"liveSet":   {Reduce: "liveSet", DeleteWhen: []WhenClause{{Path: "$.dead", Equals: true}}, Emit: []Emit{{Field: "o", From: "$.o"}}},
	} {
		t.Run(name, func(t *testing.T) {
			st.Name, st.From, st.KeyPath = "g", "rows", []string{"$.k"}
			sts := []Stage{st}
			require.NoError(t, ValidateShapes(sts))
			g := BuildGraph(sts)
			store := openTestStore(t, Fingerprint(sts))
			var last uint64
			up := func(key, payload string) {
				require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
					return g.FoldTopicUpsertNow(tx, "rows", []byte(key), decodePayload(t, payload))
				}))
				last = suiteLog.next
			}
			del := func(key string) {
				require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
					return g.FoldTopicDeleteNow(tx, "rows", []byte(key))
				}))
			}
			out := func() string {
				var got []byte
				require.NoError(t, store.View(func(tx *stagestore.Tx) error {
					var err error
					got, err = g.Output(tx, "g", []byte("x"))
					return err
				}))
				return string(got)
			}
			full := func() string {
				var want []byte
				require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
					w, live, err := g.refoldOutputBytes(tx, g.byName["g"].def, []byte("x"))
					if live {
						want = w
					}
					return err
				}))
				return string(want)
			}
			up("a", `{"k":"x","o":1,"id":"a"}`)
			up("b", `{"k":"x","o":7,"id":"b"}`)
			suiteLog.scrub(last) // b's delete is further on in the log
			before := g.byName["g"].scrubbedInputs.Load()
			up("c", `{"k":"x","o":3,"id":"c"}`)
			up("a", `{"k":"x","o":2,"id":"a"}`)
			require.NotContains(t, full(), "7", "a full refold leaves the scrubbed input out")
			require.Greater(t, g.byName["g"].scrubbedInputs.Load(), before, "the absent input is counted")
			del("b")
			require.Equal(t, full(), out(), "once b's delete folds")
			require.NotContains(t, out(), "7")
		})
	}
}

// The store reference renders pointers by what they read back.
func TestRenderRetainedPointer(t *testing.T) {
	idx := suiteLog.append("rows", []byte("k"), map[string]any{"v": json.Number("1")})
	got := string(renderRetained(encodeRetainedPointer(3, &inputOrigin{index: idx})))
	require.True(t, strings.HasPrefix(got, `pointer(gen 3) {"v":1}`), got)
}
