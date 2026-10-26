package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// unionHarness folds entities from several topics into a stage graph.
type unionHarness struct {
	t     *testing.T
	g     *Graph
	store *stagestore.Store
}

func newUnionHarness(t *testing.T, sts ...Stage) *unionHarness {
	t.Helper()
	cfg := stageConfig(sts...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	return &unionHarness{t, buildStageGraph(cfg.Stages), stageStoreForTest(t)}
}

func (h *unionHarness) fold(topic, key, payload string, gen uint64) {
	h.t.Helper()
	require.NoError(h.t, h.store.Update(func(tx *stagestore.Tx) error {
		dirty := Dirty{}
		if err := h.g.FoldTopicUpsert(tx, topic, []byte(key), decodePayload(h.t, payload), gen, dirty); err != nil {
			return err
		}
		return h.g.Drain(tx, dirty)
	}))
}

func (h *unionHarness) upsert(topic, key, payload string) {
	h.t.Helper()
	h.fold(topic, key, payload, 0)
}

func (h *unionHarness) del(topic, key string) {
	h.t.Helper()
	require.NoError(h.t, h.store.Update(func(tx *stagestore.Tx) error {
		return h.g.FoldTopicDeleteNow(tx, topic, []byte(key))
	}))
}

func (h *unionHarness) sweep(topic string, marker uint64) {
	h.t.Helper()
	require.NoError(h.t, h.store.Update(func(tx *stagestore.Tx) error {
		dirty := Dirty{}
		if err := h.g.SweepEpochs(tx, topic, marker, dirty); err != nil {
			return err
		}
		return h.g.Drain(tx, dirty)
	}))
}

func (h *unionHarness) out(stage, key string) string {
	h.t.Helper()
	var got string
	require.NoError(h.t, h.store.View(func(tx *stagestore.Tx) error {
		v, err := h.g.Output(tx, stage, []byte(key))
		got = string(v)
		return err
	}))
	return got
}

// The union's identity is (source, key): equal keys from two topics are two
// rows of the fold, and each topic's tombstone retracts only its own row.
func TestUnionAggregateKeepsCollidingKeysApart(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "rev", FromList: []string{"invoices-a", "invoices-b"}, KeyPath: []string{"$.cust"}, Reduce: "aggregate",
		Emit: []Emit{
			{Field: "n", Expr: "count(*)"},
			{Field: "avg", Expr: "round(avg($.amount), 2)"},
			{Field: "from_a", Expr: "count(*) filter (where source() = 'invoices-a')"},
			{Field: "srcs", Expr: "array_agg(source() order by source(), $.amount)"},
		},
	})
	h.upsert("invoices-a", "i1", `{"cust":"c1","amount":10}`)
	h.upsert("invoices-b", "i1", `{"cust":"c1","amount":20}`)
	require.Equal(t, `{"avg":15,"from_a":1,"n":2,"srcs":["invoices-a","invoices-b"]}`, h.out("rev", "c1"),
		"the same key i1 in both systems is two rows")

	h.del("invoices-a", "i1")
	require.Equal(t, `{"avg":20,"from_a":0,"n":1,"srcs":["invoices-b"]}`, h.out("rev", "c1"),
		"invoices-a's tombstone retracts only its own i1")

	// A rekey moves one source's row and leaves the other's.
	h.upsert("invoices-a", "i1", `{"cust":"c1","amount":4}`)
	h.upsert("invoices-b", "i1", `{"cust":"c2","amount":20}`)
	require.Equal(t, `{"avg":4,"from_a":1,"n":1,"srcs":["invoices-a"]}`, h.out("rev", "c1"))
	require.Equal(t, `{"avg":20,"from_a":0,"n":1,"srcs":["invoices-b"]}`, h.out("rev", "c2"))

	fc := h.g.FlowCounts()["rev"]
	require.Equal(t, map[string]int64{"invoices-a": 2, "invoices-b": 2}, fc.InputsBySource)
	require.EqualValues(t, 4, fc.Inputs)
}

// source() reads the input wherever input paths are valid at fold time
// (when, keyPath) and at refold (a reshape's emit).
func TestUnionSourceAtFoldAndRefold(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "events", FromList: []string{"placed", "shipped", "noise"}, KeyPath: []string{"$.order", "source()"},
		When: []WhenClause{{Expr: "source() <> 'noise'"}},
		Emit: []Emit{{Field: "kind", Expr: "upper(source())"}, {Field: "order", From: "$.order"}},
	})
	h.upsert("placed", "e1", `{"order":"o1"}`)
	h.upsert("shipped", "e1", `{"order":"o1"}`)
	h.upsert("noise", "e1", `{"order":"o1"}`)
	require.Equal(t, `{"kind":"PLACED","order":"o1"}`, h.out("events", OutKey([]string{"o1", "placed"})))
	require.Equal(t, `{"kind":"SHIPPED","order":"o1"}`, h.out("events", OutKey([]string{"o1", "shipped"})))
	require.Empty(t, h.out("events", OutKey([]string{"o1", "noise"})), "the when filtered by source()")
}

// A union mixes topics and prior stages; a prior stage's retraction
// retracts its own row of the union, and a single-source stage's source()
// is its from.
func TestUnionOfTopicAndStage(t *testing.T) {
	h := newUnionHarness(t,
		Stage{
			Name: "legacy", From: "legacy-orders", KeyPath: []string{"$.id"},
			Emit: []Emit{{Field: "cust", From: "$.customer"}, {Field: "src", Expr: "source()"}},
		},
		Stage{
			Name: "orders", FromList: []string{"legacy", "orders-v2"}, KeyPath: []string{"$.cust"}, Reduce: "aggregate",
			Emit: []Emit{{Field: "n", Expr: "count(*)"}, {Field: "srcs", Expr: "string_agg(source(), ',' order by source())"}},
		},
	)
	h.upsert("legacy-orders", "o1", `{"id":"o1","customer":"c1"}`)
	h.upsert("orders-v2", "o1", `{"cust":"c1"}`)
	require.Equal(t, `{"cust":"c1","src":"legacy-orders"}`, h.out("legacy", "o1"))
	require.Equal(t, `{"n":2,"srcs":"legacy,orders-v2"}`, h.out("orders", "c1"))

	h.del("legacy-orders", "o1")
	require.Equal(t, `{"n":1,"srcs":"orders-v2"}`, h.out("orders", "c1"), "the stage's retraction reached its own row only")
	h.del("orders-v2", "o1")
	require.Empty(t, h.out("orders", "c1"))
}

// A refresh boundary on one topic sweeps that topic's stale inputs only —
// another source's inputs carry their own topic's epochs.
func TestUnionSweepIsPerSource(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "n", FromList: []string{"a", "b"}, KeyPath: []string{"$.k"}, Reduce: "aggregate",
		Emit: []Emit{{Field: "srcs", Expr: "string_agg(source(), ',' order by source())"}},
	})
	h.fold("a", "x", `{"k":"k1"}`, 1)
	h.fold("b", "x", `{"k":"k1"}`, 1)
	require.Equal(t, `{"srcs":"a,b"}`, h.out("n", "k1"))
	h.sweep("a", 2) // a's re-snapshot did not re-assert x
	require.Equal(t, `{"srcs":"b"}`, h.out("n", "k1"), "b's epoch-1 input is not a's to sweep")
}

// liveSet across topics: the create and delete events live in different
// topics, classified by source().
func TestUnionLiveSetAcrossTopics(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "accounts", FromList: []string{"account-opened", "account-closed"}, KeyPath: []string{"$.account"},
		Reduce: "liveSet", DeleteWhen: []WhenClause{{Expr: "source() = 'account-closed'"}},
		Emit: []Emit{{Field: "owner", From: "$.owner"}},
	})
	h.upsert("account-opened", "e1", `{"account":"a1","owner":"ada"}`)
	require.Equal(t, `{"owner":"ada"}`, h.out("accounts", "a1"))
	h.upsert("account-closed", "e1", `{"account":"a1"}`) // the same event key in the other topic
	require.Empty(t, h.out("accounts", "a1"), "closed")
	h.del("account-closed", "e1")
	require.Equal(t, `{"owner":"ada"}`, h.out("accounts", "a1"), "retracting the close re-opens the account")
}

// A fan stage over a union: element identities are framed by the
// (source, key) parent identity, and source() reads in the element scope.
func TestUnionFanStage(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "lines", FromList: []string{"orders-a", "orders-b"}, KeyPath: []string{"$.sku"}, ForEach: "$.lines[*]", ElementKey: "$.sku",
		Reduce: "aggregate",
		Emit:   []Emit{{Field: "qty", Expr: "sum($.qty)"}, {Field: "from_b", Expr: "sum($.qty) filter (where source() = 'orders-b')"}},
	})
	h.upsert("orders-a", "o1", `{"lines":[{"sku":"s1","qty":2}]}`)
	h.upsert("orders-b", "o1", `{"lines":[{"sku":"s1","qty":5}]}`)
	require.Equal(t, `{"from_b":5,"qty":7}`, h.out("lines", "s1"))
	h.del("orders-b", "o1")
	require.Equal(t, `{"from_b":null,"qty":2}`, h.out("lines", "s1"))
}

// A window over a union keys each row by (source, input key) — a two-part
// composite a table decodes — and source() can break order ties.
func TestUnionWindowRowsKeyBySourceAndKey(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "timeline", FromList: []string{"placed", "shipped"}, Reduce: "window",
		PartitionBy: []string{"$.order"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"source()"},
		Emit: []Emit{{Field: "kind", Expr: "source()"}, {Field: "n", Expr: "row_number()"}, {Field: "prev", Expr: "lag(source())"}},
	})
	h.upsert("placed", "e1", `{"order":"o1","at":"2026-01-01T10:00:00Z"}`)
	h.upsert("shipped", "e1", `{"order":"o1","at":"2026-01-01T10:00:00Z"}`)
	placed := OutKey([]string{"placed", "e1"})
	shipped := OutKey([]string{"shipped", "e1"})
	require.Equal(t, `{"kind":"placed","n":1,"prev":null}`, h.out("timeline", placed))
	require.Equal(t, `{"kind":"shipped","n":2,"prev":"placed"}`, h.out("timeline", shipped), "tied at, broken by source()")
	parts, err := cluster.DecodeCompositeKey(shipped, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"shipped", "e1"}, parts)

	h.del("placed", "e1")
	require.Empty(t, h.out("timeline", placed))
	require.Equal(t, `{"kind":"shipped","n":1,"prev":null}`, h.out("timeline", shipped))
}

// latest's list form: the greatest (order, tie) sort key wins in the value
// order; desc makes the earliest win; a null order is greatest unless
// declared nulls first (PostgreSQL's DISTINCT ON … ORDER BY … DESC).
func TestLatestListForm(t *testing.T) {
	latest := func(order ...string) Stage {
		return Stage{
			Name: "l", FromList: []string{"logins", "orders"}, KeyPath: []string{"$.user"}, Reduce: "latest",
			WindowOrderBy: order, WindowTieBy: []string{"source()"},
			Emit: []Emit{{Field: "kind", Expr: "source()"}, {Field: "at", From: "$.at"}},
		}
	}
	h := newUnionHarness(t, latest("$.at::timestamptz"))
	h.upsert("logins", "e1", `{"user":"u1","at":"2026-01-02T00:00:00Z"}`)
	h.upsert("orders", "e1", `{"user":"u1","at":"2026-01-01T23:00:00-05:00"}`) // 04:00Z on Jan 2: later
	require.Equal(t, `{"at":"2026-01-01T23:00:00-05:00","kind":"orders"}`, h.out("l", "u1"), "timestamps compare as instants, not text")
	h.upsert("logins", "e2", `{"user":"u1","at":"2026-01-02T04:00:00Z"}`)
	require.Equal(t, `{"at":"2026-01-01T23:00:00-05:00","kind":"orders"}`, h.out("l", "u1"),
		"an exact order tie breaks by the tie key: orders > logins")
	h.upsert("logins", "e3", `{"user":"u1","at":null}`)
	require.Equal(t, `{"at":null,"kind":"logins"}`, h.out("l", "u1"), "a null order is greatest (nulls last)")

	h = newUnionHarness(t, latest("$.at::timestamptz desc"))
	h.upsert("logins", "e1", `{"user":"u1","at":"2026-01-02T00:00:00Z"}`)
	h.upsert("orders", "e1", `{"user":"u1","at":"2026-01-01T00:00:00Z"}`)
	require.Equal(t, `{"at":"2026-01-01T00:00:00Z","kind":"orders"}`, h.out("l", "u1"), "desc: the earliest wins")
	h.upsert("logins", "e3", `{"user":"u1","at":null}`)
	require.Equal(t, `{"at":"2026-01-01T00:00:00Z","kind":"orders"}`, h.out("l", "u1"), "desc puts nulls first: a null never wins")

	h = newUnionHarness(t, latest("$.at::timestamptz nulls first"))
	h.upsert("logins", "e1", `{"user":"u1","at":"2026-01-02T00:00:00Z"}`)
	h.upsert("logins", "e3", `{"user":"u1","at":null}`)
	require.Equal(t, `{"at":"2026-01-02T00:00:00Z","kind":"logins"}`, h.out("l", "u1"))

	// An order key that fails on an input keeps that input out of the
	// argmax rather than failing the key.
	h.upsert("orders", "bad", `{"user":"u1","at":"not a time"}`)
	require.Equal(t, `{"at":"2026-01-02T00:00:00Z","kind":"logins"}`, h.out("l", "u1"))
}

// Equal sort keys (order and tie both equal) fall to the input identity:
// deterministic whatever the arrival order.
func TestLatestListFormIdentityFloor(t *testing.T) {
	st := Stage{
		Name: "l", From: "events", KeyPath: []string{"$.user"}, Reduce: "latest",
		WindowOrderBy: []string{"$.at"}, WindowTieBy: []string{"$.seq"},
		Emit: []Emit{{Field: "id", From: "$.id"}},
	}
	for _, order := range [][]string{{"a", "b"}, {"b", "a"}} {
		h := newUnionHarness(t, st)
		for _, k := range order {
			h.upsert("events", k, `{"user":"u1","at":1,"seq":1,"id":"`+k+`"}`)
		}
		require.Equal(t, `{"id":"b"}`, h.out("l", "u1"))
	}
}

func TestUnionAdmission(t *testing.T) {
	agg := func(mut func(*Stage)) error {
		st := Stage{
			Name: "u", FromList: []string{"a", "b"}, KeyPath: []string{"$.k"}, Reduce: "aggregate",
			Emit: []Emit{{Field: "n", Expr: "count(*)"}},
		}
		mut(&st)
		return validateProjectionStageShapes(stageConfig(st))
	}
	require.NoError(t, agg(func(*Stage) {}))
	require.ErrorContains(t, agg(func(s *Stage) { s.FromList = []string{"a", "a"} }), `lists "a" twice`)
	require.ErrorContains(t, agg(func(s *Stage) { s.FromList = []string{"a"} }), "two or more sources")
	require.ErrorContains(t, agg(func(s *Stage) { s.FromList = []string{"a", ""} }), "empty source name")
	require.ErrorContains(t, agg(func(s *Stage) { s.From = "c" }), "not both")
	require.ErrorContains(t, agg(func(s *Stage) { s.FromList = []string{"a", "u"} }), "at or after this one")
	require.ErrorContains(t, agg(func(s *Stage) { s.Emit[0].Expr = "count(*) || source()" }), "source() is outside an aggregate")
	require.ErrorContains(t, agg(func(s *Stage) { s.Having = "source() = 'a'" }), "source() is outside an aggregate")
	require.ErrorContains(t, agg(func(s *Stage) {
		s.Joins = []Join{{Topic: "dims", On: []string{"$.d"}, Where: []WhenClause{{Expr: "source() = 'a'"}}}}
	}), "a join's where filters the joined row")

	// A later stage listed in the union.
	err := validateProjectionStageShapes(stageConfig(
		Stage{Name: "u", FromList: []string{"a", "later"}, KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}}},
		Stage{Name: "later", From: "t", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}}},
	))
	require.ErrorContains(t, err, "at or after this one")

	// A merge has no from, so no source().
	err = validateProjectionStageShapes(stageConfig(
		Stage{Name: "x", From: "t", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "v", From: "$.v"}}},
		Stage{Name: "y", From: "t2", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "v", From: "$.v"}}},
		Stage{Name: "m", Merge: []MergeEntry{{Stage: "x"}, {Stage: "y"}}, Emit: []Emit{{Field: "s", Expr: "source()"}}},
	))
	require.ErrorContains(t, err, "a merge has no from")

	// The unemitted-field guard over a union of stages: a path no source
	// emits is rejected; a path one source emits is fine.
	two := func(path string) error {
		return validateProjectionStageShapes(stageConfig(
			Stage{Name: "x", From: "t", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}, {Field: "a", From: "$.a"}}},
			Stage{Name: "y", From: "t2", KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "k", From: "$.k"}, {Field: "b", From: "$.b"}}},
			Stage{Name: "u", FromList: []string{"x", "y"}, KeyPath: []string{"$.k"}, Emit: []Emit{{Field: "v", Expr: path}}},
		))
	}
	require.NoError(t, two("coalesce($.a, $.b)"))
	require.ErrorContains(t, two("$.c"), `none of the union's stages "x", "y" emits`)

	// source() is a zero-argument function.
	_, err = Compile("source($.a)")
	require.ErrorContains(t, err, "takes no arguments")
	// A bare `source` path still resolves as the jsonpath library's form.
	_, isExpr, err := ClassifyKeyPath("source")
	require.NoError(t, err)
	require.False(t, isExpr)
}

func TestLatestListFormAdmission(t *testing.T) {
	latest := func(mut func(*Stage)) error {
		st := Stage{
			Name: "l", From: "t", KeyPath: []string{"$.k"}, Reduce: "latest",
			WindowOrderBy: []string{"$.at::timestamptz desc"}, WindowTieBy: []string{"$.id"},
			Emit: []Emit{{Field: "v", From: "$.v"}},
		}
		mut(&st)
		return validateProjectionStageShapes(stageConfig(st))
	}
	require.NoError(t, latest(func(*Stage) {}))
	require.ErrorContains(t, latest(func(s *Stage) { s.WindowTieBy = nil }), "needs tieBy")
	require.ErrorContains(t, latest(func(s *Stage) { s.OrderByType = KeyTypeNumber }), "cast instead")
	require.ErrorContains(t, latest(func(s *Stage) { s.TieBy = "$.id" }), "both single paths or both order-key lists")
	require.ErrorContains(t, latest(func(s *Stage) { s.WindowOrderBy = []string{"$.at sideways"} }), "orderBy")
	require.ErrorContains(t, latest(func(s *Stage) { s.PartitionBy = []string{"$.p"} }), "partitionBy declares a window")

	for src, bare := range map[string]bool{
		"$.at": true, "tenant_id": true, `$["a b"]`: true, "$['a b']": false,
		"$.at desc": false, "$.at::timestamptz": false, "$.at nulls first": false, "source()": false, "$.at sideways": false,
	} {
		require.Equal(t, bare, IsBareOrderPath(src), src)
	}
}
