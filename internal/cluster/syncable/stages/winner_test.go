package stages

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// winnerCorpusStages cover each winner-shaped reduce in each form, alone and
// behind joins (a filtering join, an anti-join, a lookup the emit reads), so
// dimension churn exercises the full-refold marks.
func winnerCorpusStages() map[string][]Stage {
	joins := []Join{
		{Topic: "dims", On: []string{"$.ref"}, As: "d", Where: []WhenClause{{Path: "$.on", Equals: true}}},
		{Topic: "dims", On: []string{"$.alt"}, As: "a", Optional: true},
	}
	anti := []Join{{Topic: "dims", On: []string{"$.ref"}, Absent: true, Where: []WhenClause{{Path: "$.on", Equals: true}}}}
	emit := []Emit{{Field: "x", From: "$.x"}, {Field: "n", From: "$.n"}, {Field: "s", Expr: "coalesce($.s::text, '-')"}}
	joinedEmit := append(append([]Emit(nil), emit...), Emit{Field: "dn", From: "$.d.name"}, Emit{Field: "an", From: "$.a.name"})
	stage := func(st Stage) []Stage {
		st.Name, st.From, st.KeyPath = "g", "rows", []string{"$.k"}
		st.When = append(st.When, WhenClause{Expr: "coalesce($.w, true)"})
		if st.Emit == nil {
			st.Emit = emit
		}
		return []Stage{st}
	}
	latestPath := func(orderType string) Stage {
		return Stage{Reduce: "latest", OrderBy: "$.o", OrderByType: orderType, TieBy: "$.t", TieByType: orderType}
	}
	latestList := Stage{Reduce: "latest", WindowOrderBy: []string{"$.o::text desc nulls last", "$.n::text"}, WindowTieBy: []string{"$.t::text"}}
	liveSet := Stage{Reduce: "liveSet", DeleteWhen: []WhenClause{{Path: "$.f", Equals: "del"}}}
	withJoins := func(st Stage, js []Join, e []Emit) Stage {
		st.Joins, st.Emit = js, e
		return st
	}
	return map[string][]Stage{
		"reshape":              stage(Stage{}),
		"reshape joined":       stage(withJoins(Stage{}, joins, joinedEmit)),
		"latest number":        stage(latestPath("number")),
		"latest text":          stage(latestPath("")),
		"latest number joined": stage(withJoins(latestPath("number"), joins, joinedEmit)),
		"latest list":          stage(latestList),
		"latest list anti":     stage(withJoins(latestList, anti, emit)),
		"liveSet":              stage(liveSet),
		"liveSet joined":       stage(withJoins(liveSet, joins, joinedEmit)),
	}
}

// winnerPayload is a random input: values with nulls, ties, numbers that
// tie numerically but not as text, and delete evidence.
func winnerPayload(rng *rand.Rand) string {
	pick := func(vals ...string) string { return vals[rng.Intn(len(vals))] }
	return fmt.Sprintf(`{"k":%s,"w":%s,"ref":%s,"alt":%s,"o":%s,"t":%s,"n":%s,"x":%s,"s":%s,"f":%s}`,
		pick(`"g0"`, `"g1"`, `"g2"`),
		pick(`true`, `true`, `true`, `false`, `null`),
		pick(`"d0"`, `"d1"`, `"d2"`, `"d3"`, `null`),
		pick(`"d0"`, `"d1"`, `"d4"`, `null`),
		pick(`1`, `2`, `2.0`, `"2"`, `10`, `null`, `"z"`, `-1`),
		pick(`1`, `2`, `2`, `null`, `"b"`, `"a"`),
		pick(`1`, `2`, `null`),
		pick(`"a"`, `"b"`, `null`),
		pick(`"p"`, `1`, `null`),
		pick(`"y"`, `"y"`, `"y"`, `"y"`, `"y"`, `"y"`, `"y"`, `"y"`, `"y"`, `"del"`),
	)
}

// TestWinnerStateMatchesFullRefold replays random appends, overwrites,
// deletes, rekeys, gate changes, epoch sweeps and dimension changes in
// random batches, and after every drain requires each key's stored output
// to equal the refold, as written before winner states existed, over the
// same retained inputs.
func TestWinnerStateMatchesFullRefold(t *testing.T) {
	for name, sts := range winnerCorpusStages() {
		t.Run(name, func(t *testing.T) { runWinnerCorpus(t, sts) })
	}
}

func runWinnerCorpus(t *testing.T, sts []Stage) {
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	st := g.byName["g"].def
	require.True(t, st.winnerReduce())
	oracle := g.oracleReshape
	switch st.Reduce {
	case "latest":
		oracle = g.oracleLatest
	case "liveSet":
		oracle = g.oracleLiveSet
	}
	store := openTestStore(t, Fingerprint(sts))

	rng := rand.New(rand.NewSource(11))
	epoch := uint64(1)
	for batch := range 800 {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			for range 1 + rng.Intn(5) {
				if len(st.Joins) > 0 && rng.Intn(5) == 0 {
					dim := fmt.Sprintf("d%d", rng.Intn(5))
					if rng.Intn(4) == 0 {
						if err := g.FoldTopicDelete(tx, "dims", []byte(dim), dirty); err != nil {
							return err
						}
					} else if err := g.FoldTopicUpsert(tx, "dims", []byte(dim), decodePayload(t, corpusDim(rng)), epoch, dirty); err != nil {
						return err
					}
					continue
				}
				row := fmt.Sprintf("r%02d", rng.Intn(18))
				switch r := rng.Intn(20); {
				case r < 14:
					if err := g.FoldTopicUpsert(tx, "rows", []byte(row), decodePayload(t, winnerPayload(rng)), epoch, dirty); err != nil {
						return err
					}
				case r < 19:
					if err := g.FoldTopicDelete(tx, "rows", []byte(row), dirty); err != nil {
						return err
					}
				default:
					epoch++
					if err := g.SweepEpochs(tx, "rows", epoch-uint64(rng.Intn(2)), dirty); err != nil {
						return err
					}
				}
			}
			return g.Drain(tx, dirty)
		}), "batch %d", batch)

		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			for _, k := range []string{"g0", "g1", "g2"} {
				got, err := tx.GetOut("g", []byte(k))
				require.NoError(t, err)
				want, live, err := oracle(tx, st, []byte(k))
				require.NoError(t, err)
				if !live {
					require.Nil(t, got, "batch %d key %s: output present, the refold says not live", batch, k)
					continue
				}
				require.Equal(t, string(want), string(got), "batch %d key %s", batch, k)
			}
			return nil
		}))
	}
	n := g.byName["g"]
	t.Logf("refolds: %d from fold state, %d in full", n.foldStateRefolds.Load(), n.fullRefolds.Load())
	require.Greater(t, n.foldStateRefolds.Load(), int64(200), "the corpus never exercised the fold state")
}

// TestWinnerStateAppendsReadNoInputs: a key growing by inputs that do not
// beat its winner refolds from its state without reading any input.
func TestWinnerStateAppendsReadNoInputs(t *testing.T) {
	sts := []Stage{{
		Name: "g", From: "rows", KeyPath: []string{"$.k"},
		Reduce: "latest", OrderBy: "$.o", OrderByType: "number", TieBy: "$.id",
		Emit: []Emit{{Field: "o", From: "$.o"}},
	}}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := openTestStore(t, Fingerprint(sts))
	for i := range 200 {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			// Alternately a new leader and a laggard.
			o := i
			if i%2 == 1 {
				o = -i
			}
			return g.FoldTopicUpsertNow(tx, "rows", fmt.Appendf(nil, "e%03d", i), decodePayload(t, fmt.Sprintf(`{"k":"a","o":%d,"id":"e%03d"}`, o, i)))
		}))
	}
	n := g.byName["g"]
	require.Equal(t, int64(1), n.fullRefolds.Load(), "only the key's first input refolds in full")
	require.Equal(t, int64(199), n.foldStateRefolds.Load())
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		got, err := tx.GetOut("g", []byte("a"))
		require.NoError(t, err)
		require.True(t, bytes.Equal(got, []byte(`{"o":198}`)), "got %s", got)
		return nil
	}))
}

// The refolds as written before winner states (eval.go at the parent
// commit), kept verbatim as the corpus's oracle.

func (g *Graph) oracleReshape(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	// Reshape: one output object per key — deterministically the emitted
	// object of the bytewise-LARGEST input identity (real reshape inputs
	// are 1:1 with keys; the tiebreak only decides pathological
	// collisions, deterministically).
	type scoped struct {
		data, parent any
		src          string
	}
	var winner *scoped
	scanErr := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := g.inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent, st.inputSource(inKey)}
		return nil
	})
	if scanErr != nil || winner == nil {
		return nil, false, scanErr
	}
	return g.emitReshape(tx, st, winner.data, winner.parent, winner.src)
}

func (g *Graph) oracleLiveSet(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type scoped struct {
		data, parent any
		src          string
	}
	var winner *scoped
	dead := false
	err := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		if dead {
			return nil
		}
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		src := st.inputSource(inKey)
		if Match(st.DeleteWhen, withSource(data, src)) {
			dead = true
			winner = nil
			return nil
		}
		if ok, err := g.inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent, src}
		return nil
	})
	if err != nil || dead || winner == nil {
		return nil, false, err
	}
	return g.emitReshape(tx, st, winner.data, winner.parent, winner.src)
}

func (g *Graph) oracleLatest(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type ranked struct {
		order, tie  any
		sortKey     []byte
		inKey       []byte
		obj, parent any
		src         string
	}
	var winner *ranked
	err := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := g.inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return err
		}
		cand := &ranked{inKey: append([]byte(nil), inKey...), obj: obj, parent: parent, src: st.inputSource(inKey)}
		if len(st.winOrder) > 0 {
			// The list form: order and tie keys compare as one sort key in
			// the shared value order; the greatest wins, and the input
			// identity breaks an exact tie.
			if cand.sortKey, err = latestSortKey(st, withSource(obj, cand.src), parent); err != nil {
				g.noteLatestOrderError(st, err)
				return nil
			}
			if winner == nil {
				winner = cand
			} else if c := bytes.Compare(cand.sortKey, winner.sortKey); c > 0 || (c == 0 && bytes.Compare(cand.inKey, winner.inKey) > 0) {
				winner = cand
			}
			return nil
		}
		cand.order, _ = ResolvePath(st.OrderBy, obj, parent)
		cand.tie, _ = ResolvePath(st.TieBy, obj, parent)
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
	return g.emitReshape(tx, st, winner.obj, winner.parent, winner.src)
}
