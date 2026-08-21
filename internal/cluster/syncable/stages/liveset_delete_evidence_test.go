package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The field's marquee un-retraction, pinned at the engine level on the
// LIVE (emitting) path: a delete-shaped event arriving at the tail must
// kill its liveSet key and cascade the heal through a downstream
// anti-join — event → reshape hop → liveSet death → dimension retraction
// → the suppressed input heals back in, with the sink delta observed.
func TestLiveSetDeleteSideLiveTopology(t *testing.T) {
	sts := []Stage{
		{
			Name: "events", From: "raw-events", KeyPath: []string{"$.eid"},
			Emit: []Emit{
				{Field: "type", From: "$.type"},
				{Field: "txn", From: "$.txn"},
				{Field: "job", From: "$.job"},
			},
		},
		{
			Name: "txn-live", From: "events", KeyPath: []string{"$.txn"}, Normalize: "lower",
			When:       []WhenClause{{Path: "$.type", Equals: "created"}},
			Reduce:     "liveSet",
			DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
			Emit:       []Emit{{Field: "job", From: "$.job"}},
		},
		{
			Name: "unbilled", From: "visits", KeyPath: []string{"$.vid"},
			Joins: []Join{{From: "txn-live", On: []string{"$.ref"}, Absent: true}},
			Emit:  []Emit{{Field: "vid", From: "$.vid"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	type delta struct {
		stage string
		key   string
		live  bool
	}
	var deltas []delta
	g.OnDelta = func(stage string, outKey []byte, _ any, live bool) error {
		deltas = append(deltas, delta{stage, string(outKey), live})
		return nil
	}

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("unbilled", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// The visit participates while no transaction is live.
	fold("visits", "v1", `{"vid":"V1","ref":"T1"}`)
	require.Equal(t, `{"vid":"V1"}`, get("V1"))

	// The created event arrives LIVE: reshape hop → liveSet key t1 →
	// dimension row → the visit retracts (the proven arrival direction).
	fold("raw-events", "e1", `{"eid":"e1","type":"created","txn":"T1","job":"j9"}`)
	require.Empty(t, get("V1"), "a live transaction suppresses the anti-joined visit")
	require.Contains(t, deltas, delta{"unbilled", "V1", false}, "the retraction delta reaches the sink")

	// The deleted event arrives LIVE — a different event row, same
	// transaction: set difference kills t1 and the visit heals back in.
	// This is the field repro (insert an EventType='deleted' row for a
	// live transaction) at the engine's own seam.
	fold("raw-events", "e2", `{"eid":"e2","type":"deleted","txn":"T1"}`)
	require.Equal(t, `{"vid":"V1"}`, get("V1"),
		"the live-tail delete-shaped event must kill the liveSet key and heal the suppressed input")
	require.Contains(t, deltas, delta{"txn-live", OutKey([]string{"t1"}), false}, "the liveSet death reaches the sink")
	require.Contains(t, deltas, delta{"unbilled", "V1", true}, "the un-retraction delta reaches the sink")
}

// A delete-shaped input whose keyPath cannot resolve (or render into its
// declared comparison space) is non-membership like any input — but for
// a liveSet it is a LOST RETRACTION: the key it meant to kill stays
// live, silently. The engine must count it loudly so a rig answers "did
// my delete event key?" in one StageStats read.
func TestUnkeyedDeleteShapedInputCounted(t *testing.T) {
	sts := []Stage{
		{
			Name: "txn-live", From: "txn-events", KeyPath: []string{"$.txn"},
			When:       []WhenClause{{Path: "$.type", Equals: "created"}},
			Reduce:     "liveSet",
			DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
			Emit:       []Emit{{Field: "job", From: "$.job"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txn-events", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("txn-live", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	fold("e1", `{"txn":"t1","type":"created","job":"j1"}`)
	require.Equal(t, `{"job":"j1"}`, get("t1"))

	// The delete-shaped event lacks the keyed field entirely (the field
	// probe's suspected shape): the key stays live — documented
	// semantics — and the loss is COUNTED.
	fold("e2", `{"type":"deleted"}`)
	require.Equal(t, `{"job":"j1"}`, get("t1"), "an unkeyable delete cannot know which key to kill")
	require.Equal(t, int64(1), g.FlowCounts()["txn-live"].UnkeyedDeletes,
		"a lost retraction must be loud — counted per stage")

	// An ordinary non-matching input does NOT count: the counter is
	// delete-evidence-specific, not shape-variance noise.
	fold("e3", `{"type":"annotated"}`)
	require.Equal(t, int64(1), g.FlowCounts()["txn-live"].UnkeyedDeletes)
}

// The unrenderable-keyType variant of the same loss: the delete-shaped
// event carries the field, but its value cannot render into the declared
// comparison space.
func TestUnkeyedDeleteShapedInputCountedOnTypeMiss(t *testing.T) {
	sts := []Stage{
		{
			Name: "txn-live", From: "txn-events", KeyPath: []string{"$.txn"}, KeyType: []string{"number"},
			When:       []WhenClause{{Path: "$.type", Equals: "created"}},
			Reduce:     "liveSet",
			DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
			Emit:       []Emit{{Field: "job", From: "$.job"}},
		},
	}
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txn-events", []byte(key), decodePayload(t, payload))
		}))
	}

	fold("e1", `{"txn":5,"type":"created","job":"j1"}`)
	fold("e2", `{"txn":"not-a-number","type":"deleted"}`)
	require.Equal(t, int64(1), g.FlowCounts()["txn-live"].UnkeyedDeletes,
		"a delete whose key cannot render into the declared space is the same lost retraction")
}
