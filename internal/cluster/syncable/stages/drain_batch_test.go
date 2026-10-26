package stages

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// stageOp is one ingress operation, marking the keys it dirties.
type stageOp func(g *Graph, tx *stagestore.Tx, dirty Dirty) error

// referenceOps records the store-format fixture's feed as operations.
func referenceOps(t *testing.T) []stageOp {
	t.Helper()
	var ops []stageOp
	storeReferenceFeed(
		func(topic, key, payload string, gen uint64) {
			obj := decodePayload(t, payload)
			ops = append(ops, func(g *Graph, tx *stagestore.Tx, dirty Dirty) error {
				return g.FoldTopicUpsert(tx, topic, []byte(key), obj, gen, dirty)
			})
		},
		func(topic string, marker uint64) {
			ops = append(ops, func(g *Graph, tx *stagestore.Tx, dirty Dirty) error {
				return g.SweepEpochs(tx, topic, marker, dirty)
			})
		},
		func(topic, key string) {
			ops = append(ops, func(g *Graph, tx *stagestore.Tx, dirty Dirty) error {
				return g.FoldTopicDelete(tx, topic, []byte(key), dirty)
			})
		},
	)
	return ops
}

// replayInRuns folds ops into a fresh store of the reference fixture, each
// run of ops in one transaction drained ONCE, and returns the store dump.
func replayInRuns(t *testing.T, ops []stageOp, runs []int) string {
	t.Helper()
	sts := storeReferenceFixture()
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	dir := t.TempDir()
	store, _, err := stagestore.Open(dir, "fixture", Fingerprint(sts))
	require.NoError(t, err)
	i := 0
	for _, n := range runs {
		run := ops[i : i+n]
		i += n
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			for _, op := range run {
				if err := op(g, tx, dirty); err != nil {
					return err
				}
			}
			return g.Drain(tx, dirty)
		}))
	}
	require.Equal(t, len(ops), i)
	require.NoError(t, store.Close())
	dump, _ := dumpStore(t, g, dir, "fixture")
	return dump
}

// TestDrainOncePerRunMatchesDrainPerOp: folding a run of inputs and draining
// once leaves the store byte-identical to draining after each input — the
// property FoldActuals relies on. Covers the whole stage vocabulary (the
// store-format fixture) under every run cut of a random sample.
func TestDrainOncePerRunMatchesDrainPerOp(t *testing.T) {
	ops := referenceOps(t)
	perOp := make([]int, len(ops))
	for i := range perOp {
		perOp[i] = 1
	}
	want := replayInRuns(t, ops, perOp)
	require.Contains(t, want, "stage:els:acc", "the fixture's aggregate keeps a fold state (its output is the state's rendering)")

	require.Equal(t, want, replayInRuns(t, ops, []int{len(ops)}), "one run for the whole feed")

	rng := rand.New(rand.NewSource(1))
	for range 30 {
		var runs []int
		for left := len(ops); left > 0; {
			n := 1 + rng.Intn(left)
			runs = append(runs, n)
			left -= n
		}
		require.Equal(t, want, replayInRuns(t, ops, runs), "runs %v", runs)
	}
}
