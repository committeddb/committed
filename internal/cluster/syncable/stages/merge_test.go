package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func mergeHarness(t *testing.T, sts []Stage) (fold func(topic, key, payload string), del func(topic, key string), get func(stage, key string) string) {
	t.Helper()
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)
	store := stageStoreForTest(t)
	fold = func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	del = func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	get = func(stage, key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	return fold, del, get
}

// Candidate C's shape: quoted − invoiced per job, a FULL-OUTER combine
// of two per-job sums with coalesce arithmetic and left gating — the
// cross-stage value resolution the whole design session was for.
func TestMergeQuotedMinusInvoiced(t *testing.T) {
	sts := []Stage{
		{
			Name: "quoted", From: "quotes", KeyPath: []string{"$.jobId"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amount"}},
		},
		{
			Name: "invoiced", From: "invoices", KeyPath: []string{"$.jobId"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amount"}},
		},
		{
			Name:  "open-by-job",
			Merge: []MergeEntry{{Stage: "quoted"}, {Stage: "invoiced"}},
			When:  []WhenClause{{Path: "$.quoted", NotNull: true}},
			Emit: []Emit{
				{Field: "open", Expr: "coalesce($.quoted.total, 0) - coalesce($.invoiced.total, 0)"},
			},
		},
	}
	fold, del, get := mergeHarness(t, sts)

	// A quote with no invoices: participates (left side present), open =
	// the full quote.
	fold("quotes", "q1", `{"jobId":"j1","amount":100.50}`)
	require.Equal(t, `{"open":100.5}`, get("open-by-job", "j1"), "quoted-only: invoiced side null, coalesce carries")

	// Invoices arrive: the tuple refolds, the difference updates.
	fold("invoices", "i1", `{"jobId":"j1","amount":40.25}`)
	require.Equal(t, `{"open":60.25}`, get("open-by-job", "j1"))

	// An invoice with NO quote: gated out by the notNull (left join).
	fold("invoices", "i2", `{"jobId":"j2","amount":10}`)
	require.Empty(t, get("open-by-job", "j2"), "notNull gates the invoiced-only job out")

	// Its quote arrives late: heals in.
	fold("quotes", "q2", `{"jobId":"j2","amount":10}`)
	require.Equal(t, `{"open":0}`, get("open-by-job", "j2"))

	// The quoted side retracts entirely: the gate closes again.
	del("quotes", "q2")
	require.Empty(t, get("open-by-job", "j2"), "owner-side retraction retracts the merged key")

	// Both sides retract: the key is gone.
	del("quotes", "q1")
	del("invoices", "i1")
	require.Empty(t, get("open-by-job", "j1"))
}

// The union shape: charges from two event forms summed per pair — full
// outer with coalesce addition, no gating. The form that made
// multi-forEach unnecessary.
func TestMergeUnionOfSums(t *testing.T) {
	sts := []Stage{
		{
			Name: "created-charges", From: "created", KeyPath: []string{"$.wa"},
			Reduce: "aggregate", Emit: []Emit{{Field: "sum", Sum: "$.charge"}},
		},
		{
			Name: "added-charges", From: "added", KeyPath: []string{"$.wa"},
			Reduce: "aggregate", Emit: []Emit{{Field: "sum", Sum: "$.charge"}},
		},
		{
			Name:  "charges",
			Merge: []MergeEntry{{Stage: "created-charges", As: "c"}, {Stage: "added-charges", As: "a"}},
			Emit:  []Emit{{Field: "sum", Expr: "coalesce($.c.sum, 0) + coalesce($.a.sum, 0)"}},
		},
	}
	fold, _, get := mergeHarness(t, sts)

	// A pair whose ONLY charges arrived via elements-added — the case a
	// left join from the created side would drop.
	fold("added", "a1", `{"wa":"w9","charge":5.25}`)
	require.Equal(t, `{"sum":5.25}`, get("charges", "w9"), "added-only pairs participate (full outer)")

	fold("created", "c1", `{"wa":"w9","charge":10}`)
	require.Equal(t, `{"sum":15.25}`, get("charges", "w9"))
}

// Candidate A's shape: attribution — items carry only a workarea; the
// job id lives on another topic. Merge them by workarea, emit the job
// id, and the NEXT stage keys by it: the rekey on re-homing flows
// through the existing upstream-delta machinery with zero new code.
func TestMergeAttributionRekey(t *testing.T) {
	sts := []Stage{
		{
			Name: "item-sums", From: "items", KeyPath: []string{"$.wa"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amount"}},
		},
		{
			Name: "wa-jobs", From: "workareas", KeyPath: []string{"$.id"},
			Emit: []Emit{{Field: "jobId", From: "$.jobId"}},
		},
		{
			Name:  "attributed",
			Merge: []MergeEntry{{Stage: "item-sums", As: "items"}, {Stage: "wa-jobs", As: "wa"}},
			When:  []WhenClause{{Path: "$.wa.jobId", NotNull: true}},
			Emit: []Emit{
				{Field: "jobId", From: "$.wa.jobId"},
				{Field: "total", From: "$.items.total"},
			},
		},
		{
			Name: "by-job", From: "attributed", KeyPath: []string{"$.jobId"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.total"}},
		},
	}
	fold, _, get := mergeHarness(t, sts)

	fold("workareas", "w1", `{"id":"w1","jobId":"jobA"}`)
	fold("items", "i1", `{"wa":"w1","amount":25}`)
	require.Equal(t, `{"total":25}`, get("by-job", "jobA"), "items attribute to their job through the merge")

	// The workarea is RE-HOMED to another job: the merge's output
	// changes, the downstream stage re-keys — jobA loses, jobB gains.
	fold("workareas", "w1", `{"id":"w1","jobId":"jobB"}`)
	require.Empty(t, get("by-job", "jobA"), "re-homing retracts the old attribution")
	require.Equal(t, `{"total":25}`, get("by-job", "jobB"), "and re-keys to the new job")
}

// The same-key degenerate: S11's anti-join as a merge — participate
// while the billed side is ABSENT (null arm on the alias path).
func TestMergeSameKeyAnti(t *testing.T) {
	sts := []Stage{
		{
			Name: "pairs", From: "completed", KeyPath: []string{"$.pair"},
			Emit: []Emit{{Field: "pair", From: "$.pair"}},
		},
		{
			Name: "billed", From: "billing", KeyPath: []string{"$.pair"},
			Emit: []Emit{{Field: "pair", From: "$.pair"}},
		},
		{
			Name:  "unbilled",
			Merge: []MergeEntry{{Stage: "pairs"}, {Stage: "billed"}},
			When: []WhenClause{
				{Path: "$.pairs", NotNull: true},
				{Path: "$.billed", Null: true},
			},
			Emit: []Emit{{Field: "pair", From: "$.pairs.pair"}},
		},
	}
	fold, del, get := mergeHarness(t, sts)

	fold("completed", "p1", `{"pair":"P1"}`)
	require.NotEmpty(t, get("unbilled", "P1"), "unbilled while the billed side is absent")

	fold("billing", "b1", `{"pair":"P1"}`)
	require.Empty(t, get("unbilled", "P1"), "billing arrival suppresses")

	del("billing", "b1")
	require.NotEmpty(t, get("unbilled", "P1"), "billing retraction un-suppresses")
}

// Merge admission: the rejected forms, each with its remedy.
func TestMergeValidated(t *testing.T) {
	base := func() []Stage {
		return []Stage{
			{Name: "a", From: "t1", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}}},
			{Name: "b", From: "t2", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}}},
			{Name: "m", Merge: []MergeEntry{{Stage: "a"}, {Stage: "b"}}, Emit: []Emit{{Field: "k", From: "$.a.k"}}},
		}
	}
	require.NoError(t, ValidateShapes(base()))

	cases := []struct {
		name  string
		mut   func([]Stage)
		wants string
	}{
		{"keyPath rejected", func(s []Stage) { s[2].KeyPath = []string{"$.k"} }, "keyPath is not declarable on a merge"},
		{"normalize rejected", func(s []Stage) { s[2].Normalize = "lower" }, "normalize is not declarable on a merge"},
		{"keyType rejected", func(s []Stage) { s[2].KeyType = []string{"number"} }, "keyType is not declarable on a merge"},
		{"reduce rejected", func(s []Stage) { s[2].Reduce = "aggregate" }, "reduce is not applicable to a merge"},
		{"joins rejected", func(s []Stage) {
			s[2].Joins = []Join{{Topic: "d", On: []string{"$.x"}}}
		}, "joins are not applicable to a merge stage"},
		{"one entry", func(s []Stage) { s[2].Merge = s[2].Merge[:1] }, "at least two prior stages"},
		{"duplicate", func(s []Stage) { s[2].Merge[1].Stage = "a" }, `stage "a" merged twice`},
		{"unknown", func(s []Stage) { s[2].Merge[1].Stage = "nope" }, "names no declared stage"},
		{"from and merge", func(s []Stage) { s[2].From = "a" }, "exactly one of from or merge"},
		{"alias unsafe", func(s []Stage) { s[2].Merge[1].As = "has-dash" }, "not addressable by a jsonpath dot segment"},
		{"alias dup", func(s []Stage) { s[2].Merge[1].As = "a" }, `a second merged side aliased "a"`},
		{"normalize disagree", func(s []Stage) { s[0].Normalize = "lower" }, "disagree on normalize"},
		{"keyType disagree", func(s []Stage) { s[1].KeyType = []string{"number"} }, "disagree on keyType"},
		{"arity disagree", func(s []Stage) {
			s[1].KeyPath = []string{"$.k", "$.k2"}
		}, "disagree on key arity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sts := base()
			tc.mut(sts)
			err := ValidateShapes(sts)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wants)
		})
	}

	// A merge of a merge resolves its key space through the chain.
	chain := append(base(), Stage{
		Name: "c", From: "t3", KeyPath: []string{"$.k"},
		Emit: []Emit{{Field: "k", From: "$.k"}},
	}, Stage{
		Name: "m2", Merge: []MergeEntry{{Stage: "m"}, {Stage: "c"}},
		Emit: []Emit{{Field: "k", From: "$.m.k"}},
	})
	require.NoError(t, ValidateShapes(chain), "merge-of-merge inherits through the chain")
}

// GOLDEN CONTRACT — fingerprint stability across binary upgrades. This
// pins the fingerprint of a representative stage set that uses NONE of
// the newer vocabulary. If this test fails, the binary you are building
// will spuriously RESET the stage store of every unchanged config on
// upgrade — a silent multi-minute (hours at scale) re-derivation with
// live-tail latency degraded throughout (field-measured). Vocabulary
// ADDITIONS must keep this stable (declared-content marshal: omitempty
// + WhenClause's explicit arms). Only a DELIBERATE semantic change may
// re-pin it, and that change ships with an upgrade-notes callout.
func TestFingerprintGoldenContract(t *testing.T) {
	sts := []Stage{
		{
			Name: "live", From: "txns", KeyPath: []string{"$.id"},
			When: []WhenClause{{Path: "$.status", Equals: "active"}},
			Emit: []Emit{{Field: "job", From: "$.jobId"}, {Field: "amt", From: "$.amount"}},
		},
		{
			Name: "by-job", From: "live", KeyPath: []string{"$.job"},
			Reduce: "aggregate",
			Joins:  []Join{{Topic: "projects", On: []string{"$.job"}, Absent: true}},
			Emit:   []Emit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}},
		},
	}
	const golden = "4b5151015db670971db43e9e9acbafa46ddacd98e1f74805a99c68930666914c"
	require.Equal(t, golden, Fingerprint(sts),
		"fingerprint of an unchanged config drifted — unchanged configs would reset their stores on upgrade (see comment)")

	// And the stability mechanism itself: declaring a NEW vocabulary
	// field changes the fingerprint; leaving it undeclared does not.
	withNew := make([]Stage, len(sts))
	copy(withNew, sts)
	withNew[1].KeyType = []string{"number"}
	require.NotEqual(t, golden, Fingerprint(withNew), "declared new vocabulary must change the fingerprint")
}
