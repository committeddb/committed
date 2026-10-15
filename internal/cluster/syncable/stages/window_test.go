package stages

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func windowStage(emit []Emit, tieBy ...string) Stage {
	return Stage{
		Name: "w", From: "rows", Reduce: "window",
		PartitionBy: []string{"$.p"}, WindowOrderBy: []string{"$.o"}, WindowTieBy: tieBy,
		Emit: emit,
	}
}

// windowHarness folds a topic into a window stage and reads every row's
// output.
type windowHarness struct {
	t     *testing.T
	g     *Graph
	store *stagestore.Store
	keys  map[string]bool
}

func newWindowHarness(t *testing.T, stages ...Stage) *windowHarness {
	t.Helper()
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	return &windowHarness{t, buildStageGraph(cfg.Stages), stageStoreForTest(t), map[string]bool{}}
}

type windowOp struct {
	key     string
	payload string // "" = delete
}

// apply folds several ops as ONE batch (one Drain).
func (h *windowHarness) apply(ops ...windowOp) error {
	return h.store.Update(func(tx *stagestore.Tx) error {
		dirty := Dirty{}
		for _, op := range ops {
			h.keys[op.key] = true
			var err error
			if op.payload == "" {
				err = h.g.FoldTopicDelete(tx, "rows", []byte(op.key), dirty)
			} else {
				err = h.g.FoldTopicUpsert(tx, "rows", []byte(op.key), decodePayload(h.t, op.payload), 0, dirty)
			}
			if err != nil {
				return err
			}
		}
		return h.g.Drain(tx, dirty)
	})
}

func (h *windowHarness) upsert(key, payload string) {
	h.t.Helper()
	require.NoError(h.t, h.apply(windowOp{key, payload}))
}

func (h *windowHarness) del(key string) {
	h.t.Helper()
	require.NoError(h.t, h.apply(windowOp{key: key}))
}

// outputs reads stage's output for every key the harness has touched.
func (h *windowHarness) outputs(stage string) map[string]string {
	got := map[string]string{}
	require.NoError(h.t, h.store.View(func(tx *stagestore.Tx) error {
		for k := range h.keys {
			v, err := tx.GetOut(stage, []byte(k))
			if err != nil {
				return err
			}
			if v != nil {
				got[k] = string(v)
			}
		}
		return nil
	}))
	return got
}

func (h *windowHarness) out(key string) string { return h.outputs("w")[key] }

// PostgreSQL 16's own output for the same rows:
//
//	SELECT id, row_number() OVER w, rank() OVER w, dense_rank() OVER w,
//	       lag(v) OVER w, lead(v, 1, -1) OVER w, first_value(v) OVER w,
//	       last_value(v) OVER w, sum(v) OVER w, count(*) OVER w
//	FROM t WINDOW w AS (PARTITION BY p ORDER BY o, id);
//
// (PG with the tie key in its ORDER BY, so peers are on (o, id) there; the
// peer cases are pinned separately below.)
func TestWindowFunctionsMatchPostgres(t *testing.T) {
	h := newWindowHarness(t, windowStage([]Emit{
		{Field: "rn", Expr: "row_number()"},
		{Field: "lag", Expr: "lag($.v)"},
		{Field: "lead", Expr: "lead($.v, 1, -1)"},
		{Field: "first", Expr: "first_value($.v)"},
		{Field: "sum", Expr: "sum($.v)"},
		{Field: "n", Expr: "count(*)"},
	}, "$.id"))
	h.upsert("a", `{"p":"x","o":3,"id":1,"v":30}`)
	h.upsert("b", `{"p":"x","o":1,"id":2,"v":10}`)
	h.upsert("c", `{"p":"x","o":2,"id":3,"v":20}`)
	h.upsert("d", `{"p":"y","o":1,"id":4,"v":5}`)
	require.Equal(t, map[string]string{
		"b": `{"first":10,"lag":null,"lead":20,"n":1,"rn":1,"sum":10}`,
		"c": `{"first":10,"lag":10,"lead":30,"n":2,"rn":2,"sum":30}`,
		"a": `{"first":10,"lag":20,"lead":-1,"n":3,"rn":3,"sum":60}`,
		"d": `{"first":5,"lag":null,"lead":-1,"n":1,"rn":1,"sum":5}`,
	}, h.outputs("w"))
}

// Peers: rows with equal orderBy share rank and the default frame (RANGE
// … CURRENT ROW includes every peer), exactly as PG computes
//
//	rank() / dense_rank() / sum(v) / last_value(v) OVER (ORDER BY o)
//
// over (o, v) = (1,1), (2,10), (2,20), (3,100): ranks 1,2,2,4; dense
// 1,2,2,3; running sums 1,31,31,131. last_value is the LAST peer, which
// PG leaves to arbitrary order and committed decides by tieBy.
func TestWindowPeers(t *testing.T) {
	h := newWindowHarness(t, windowStage([]Emit{
		{Field: "rank", Expr: "rank()"},
		{Field: "dense", Expr: "dense_rank()"},
		{Field: "sum", Expr: "sum($.v)"},
		{Field: "last", Expr: "last_value($.v)"},
		{Field: "max", Expr: "max($.v)"},
		{Field: "avg", Expr: "round(avg($.v), 2)"},
	}, "$.id"))
	h.upsert("1", `{"p":"x","o":1,"id":1,"v":1}`)
	h.upsert("2", `{"p":"x","o":2,"id":2,"v":10}`)
	h.upsert("3", `{"p":"x","o":2,"id":3,"v":20}`)
	h.upsert("4", `{"p":"x","o":3,"id":4,"v":100}`)
	require.Equal(t, map[string]string{
		"1": `{"avg":1,"dense":1,"last":1,"max":1,"rank":1,"sum":1}`,
		"2": `{"avg":10.33,"dense":2,"last":20,"max":20,"rank":2,"sum":31}`,
		"3": `{"avg":10.33,"dense":2,"last":20,"max":20,"rank":2,"sum":31}`,
		"4": `{"avg":32.75,"dense":3,"last":100,"max":100,"rank":4,"sum":131}`,
	}, h.outputs("w"))

	// A peer joining the group changes every peer's frame, and the next
	// group's rank.
	h.upsert("5", `{"p":"x","o":2,"id":0,"v":5}`)
	require.JSONEq(t, `{"avg":9,"dense":2,"last":20,"max":20,"rank":2,"sum":36}`, h.out("2"))
	require.JSONEq(t, `{"avg":27.2,"dense":3,"last":100,"max":100,"rank":5,"sum":136}`, h.out("4"))
}

// Nulls sort last ascending and first descending (PG's defaults), and
// nulls first/last overrides; desc reverses the business order.
func TestWindowOrderDirectionAndNulls(t *testing.T) {
	for _, tc := range []struct {
		order string
		want  []string // keys in row_number order
	}{
		{"$.o", []string{"b", "c", "a", "n"}},
		{"$.o desc", []string{"n", "a", "c", "b"}},
		{"$.o nulls first", []string{"n", "b", "c", "a"}},
		{"$.o desc nulls last", []string{"a", "c", "b", "n"}},
	} {
		st := windowStage([]Emit{{Field: "rn", Expr: "row_number()"}}, "$.id")
		st.WindowOrderBy = []string{tc.order}
		h := newWindowHarness(t, st)
		h.upsert("a", `{"p":"x","o":3,"id":1}`)
		h.upsert("b", `{"p":"x","o":1,"id":2}`)
		h.upsert("c", `{"p":"x","o":2,"id":3}`)
		h.upsert("n", `{"p":"x","o":null,"id":4}`)
		for i, k := range tc.want {
			require.JSONEq(t, fmt.Sprintf(`{"rn":%d}`, i+1), h.out(k), "%s: %s", tc.order, k)
		}
	}
}

// Time in status — the acceptance shape: lead's timestamp minus this one,
// epoch seconds; the open (latest) status has null.
func TestWindowTimeInStatus(t *testing.T) {
	st := windowStage([]Emit{
		{Field: "status", From: "$.status"},
		{Field: "secs", Expr: "extract(epoch from (lead($.at::timestamptz) - $.at::timestamptz))"},
		{Field: "prev", Expr: "lag($.status)"},
	}, "$.seq")
	st.PartitionBy = []string{"$.job"}
	st.WindowOrderBy = []string{"$.at::timestamptz"}
	h := newWindowHarness(t, st)
	h.upsert("e1", `{"job":"j1","seq":1,"status":"scheduled","at":"2024-01-01T10:00:00Z"}`)
	h.upsert("e2", `{"job":"j1","seq":2,"status":"running","at":"2024-01-01T10:05:30Z"}`)
	require.JSONEq(t, `{"prev":null,"secs":330,"status":"scheduled"}`, h.out("e1"))
	require.JSONEq(t, `{"prev":"scheduled","secs":null,"status":"running"}`, h.out("e2"))
	// A late event between them re-splits the span.
	h.upsert("e3", `{"job":"j1","seq":3,"status":"paused","at":"2024-01-01T10:01:00Z"}`)
	require.JSONEq(t, `{"prev":null,"secs":60,"status":"scheduled"}`, h.out("e1"))
	require.JSONEq(t, `{"prev":"scheduled","secs":270,"status":"paused"}`, h.out("e3"))
	require.JSONEq(t, `{"prev":"paused","secs":null,"status":"running"}`, h.out("e2"))
	// Deleting it heals the original spans; the row retracts.
	h.del("e3")
	require.JSONEq(t, `{"prev":null,"secs":330,"status":"scheduled"}`, h.out("e1"))
	require.Equal(t, "", h.out("e3"))
}

// An append in business order recomputes the new row and the row whose
// lead now reaches it — not the partition.
func TestWindowInOrderAppendRecomputesConstantRows(t *testing.T) {
	h := newWindowHarness(t, windowStage([]Emit{
		{Field: "rn", Expr: "row_number()"},
		{Field: "bal", Expr: "sum($.v)"},
		{Field: "next", Expr: "lead($.v)"},
		{Field: "prev", Expr: "lag($.v, 2)"},
	}, "$.id"))
	for i := 1; i <= 50; i++ {
		h.upsert(fmt.Sprintf("r%03d", i), fmt.Sprintf(`{"p":"x","o":%d,"id":%d,"v":%d}`, i, i, i))
	}
	before := h.g.FlowCounts()["w"].WindowRowsRecomputed
	h.upsert("r051", `{"p":"x","o":51,"id":51,"v":51}`)
	require.LessOrEqual(t, h.g.FlowCounts()["w"].WindowRowsRecomputed-before, int64(2), "an in-order append recomputes O(1) rows")
	require.JSONEq(t, `{"bal":1326,"next":null,"prev":49,"rn":51}`, h.out("r051"))
	require.JSONEq(t, `{"bal":1275,"next":51,"prev":48,"rn":50}`, h.out("r050"))

	// An out-of-order insert recomputes the suffix after it — and stops:
	// an update that changes nothing positional stops right after the
	// rows whose lag/lead reach it.
	before = h.g.FlowCounts()["w"].WindowRowsRecomputed
	h.upsert("r025", `{"p":"x","o":25,"id":25,"v":25,"note":"edited"}`)
	require.LessOrEqual(t, h.g.FlowCounts()["w"].WindowRowsRecomputed-before, int64(5), "a payload-only change stops once state converges")
	require.Equal(t, int64(51), h.g.FlowCounts()["w"].WindowLargestPartition)
}

// A row moving between partitions recomputes both, and a downstream
// filter on row_number retracts the row that slides out (top-N per row).
func TestWindowMoveAndTopN(t *testing.T) {
	w := windowStage([]Emit{{Field: "p", From: "$.p"}, {Field: "id", From: "$.id"}, {Field: "n", Expr: "row_number()"}}, "$.id")
	w.WindowOrderBy = []string{"$.score desc"}
	top := Stage{
		Name: "top", From: "w", KeyPath: []string{"$.id"}, When: []WhenClause{{Expr: "$.n <= 2"}},
		Emit: []Emit{{Field: "p", From: "$.p"}, {Field: "n", From: "$.n"}},
	}
	h := newWindowHarness(t, w, top)
	h.upsert("a", `{"p":"x","id":"a","score":10}`)
	h.upsert("b", `{"p":"x","id":"b","score":20}`)
	h.upsert("c", `{"p":"y","id":"c","score":5}`)
	require.JSONEq(t, `{"n":2,"p":"x"}`, h.outputs("top")["a"])
	h.upsert("d", `{"p":"x","id":"d","score":30}`)
	require.Equal(t, "", h.outputs("top")["a"], "a slid to third: it retracts from the top 2")
	require.JSONEq(t, `{"n":2,"p":"x"}`, h.outputs("top")["b"])
	// Move d to y: x re-ranks (a back in), y gains a first row.
	h.upsert("d", `{"p":"y","id":"d","score":30}`)
	require.JSONEq(t, `{"n":2,"p":"x"}`, h.outputs("top")["a"])
	require.JSONEq(t, `{"id":"d","n":1,"p":"y"}`, h.out("d"))
	require.JSONEq(t, `{"id":"c","n":2,"p":"y"}`, h.out("c"))
}

// The when filters inputs BEFORE windowing (SQL's WHERE), and an input
// whose order cannot evaluate is no row at all.
func TestWindowWhenAndUnorderable(t *testing.T) {
	st := windowStage([]Emit{{Field: "n", Expr: "row_number()"}}, "$.id")
	st.When = []WhenClause{{Expr: "$.ok"}}
	h := newWindowHarness(t, st)
	h.upsert("a", `{"p":"x","o":1,"id":1,"ok":true}`)
	h.upsert("b", `{"p":"x","o":2,"id":2,"ok":true}`)
	h.upsert("a", `{"p":"x","o":1,"id":1,"ok":false}`)
	require.Equal(t, "", h.out("a"))
	require.JSONEq(t, `{"n":1}`, h.out("b"))
	h.upsert("c", `{"p":"x","o":[1],"id":3,"ok":true}`)
	require.Equal(t, "", h.out("c"), "an array has no order: not a row")
}

// A required join gates membership at refold: a dimension arriving moves
// positions, so the partition recomputes whole.
func TestWindowJoinMembership(t *testing.T) {
	st := windowStage([]Emit{{Field: "n", Expr: "row_number()"}, {Field: "name", Expr: "$.acct.name"}}, "$.id")
	st.Joins = []Join{{Topic: "accts", On: []string{"$.acct_id"}, As: "acct"}}
	h := newWindowHarness(t, st)
	h.upsert("a", `{"p":"x","o":1,"id":1,"acct_id":"A"}`)
	h.upsert("b", `{"p":"x","o":2,"id":2,"acct_id":"B"}`)
	require.Empty(t, h.outputs("w"))
	require.NoError(t, h.store.Update(func(tx *stagestore.Tx) error {
		return h.g.FoldTopicUpsertNow(tx, "accts", []byte("B"), decodePayload(t, `{"name":"bee"}`))
	}))
	require.JSONEq(t, `{"n":1,"name":"bee"}`, h.out("b"))
	require.NoError(t, h.store.Update(func(tx *stagestore.Tx) error {
		return h.g.FoldTopicUpsertNow(tx, "accts", []byte("A"), decodePayload(t, `{"name":"ay"}`))
	}))
	require.JSONEq(t, `{"n":1,"name":"ay"}`, h.out("a"))
	require.JSONEq(t, `{"n":2,"name":"bee"}`, h.out("b"))
	require.NoError(t, h.store.Update(func(tx *stagestore.Tx) error {
		return h.g.FoldTopicDeleteNow(tx, "accts", []byte("A"))
	}))
	require.Equal(t, "", h.out("a"))
	require.JSONEq(t, `{"n":1,"name":"bee"}`, h.out("b"))
}

// The incremental recompute must equal a from-scratch build after every
// batch: random inserts, payload edits, order moves, partition moves, and
// deletes, several per batch, across partitions — with lag/lead offsets
// and peers in play, so every start/stop rule is exercised.
func TestWindowIncrementalEqualsRebuild(t *testing.T) {
	full := []Emit{
		{Field: "rn", Expr: "row_number()"},
		{Field: "rank", Expr: "rank()"},
		{Field: "dense", Expr: "dense_rank()"},
		{Field: "lag2", Expr: "lag($.v, 2, 0)"},
		{Field: "lead1", Expr: "lead($.v)"},
		{Field: "lead3", Expr: "lead($.v, 3)"},
		{Field: "first", Expr: "first_value($.v)"},
		{Field: "last", Expr: "last_value($.v)"},
		{Field: "sum", Expr: "sum($.v) filter (where $.v > 2)"},
		{Field: "cnt", Expr: "count(*)"},
		{Field: "min", Expr: "min($.v)"},
		{Field: "avg", Expr: "round(avg($.v), 3)"},
	}
	// Positional-only state (row number, ranks) makes coincidentally equal
	// states across partitions and positions likely — the early stop must
	// not trust them.
	positional := []Emit{{Field: "rn", Expr: "row_number()"}, {Field: "rank", Expr: "rank()"}, {Field: "v", From: "$.v"}}
	lagOnly := []Emit{{Field: "rn", Expr: "row_number()"}, {Field: "lag", Expr: "lag($.v)"}}
	for name, emits := range map[string][]Emit{"full": full, "positional": positional, "lagOnly": lagOnly} {
		t.Run(name, func(t *testing.T) { checkWindowIncrementalEqualsRebuild(t, emits) })
	}
}

func checkWindowIncrementalEqualsRebuild(t *testing.T, emits []Emit) {
	stage := func() Stage {
		return windowStage(emits, "$.t")
	}
	r := rand.New(rand.NewPCG(7, 11))
	inc := newWindowHarness(t, stage())
	live := map[string]string{}
	for step := 0; step < 300; step++ {
		var ops []windowOp
		for range 1 + r.IntN(3) {
			key := fmt.Sprintf("k%02d", r.IntN(40))
			if _, ok := live[key]; ok && r.IntN(4) == 0 {
				delete(live, key)
				ops = append(ops, windowOp{key: key})
				continue
			}
			// Few distinct o values, so peer groups are common; t ties too,
			// leaving the input key as the final tiebreak.
			p := fmt.Sprintf(`{"p":"p%d","o":%d,"t":%d,"v":%d}`, r.IntN(3), r.IntN(8), r.IntN(3), r.IntN(10))
			live[key] = p
			ops = append(ops, windowOp{key, p})
		}
		require.NoError(t, inc.apply(ops...))

		fresh := newWindowHarness(t, stage())
		keys := make([]string, 0, len(live))
		for k := range live {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		all := make([]windowOp, 0, len(keys))
		for _, k := range keys {
			all = append(all, windowOp{k, live[k]})
		}
		require.NoError(t, fresh.apply(all...))
		for k := range inc.keys {
			fresh.keys[k] = true
		}
		require.Equal(t, fresh.outputs("w"), inc.outputs("w"), "step %d", step)
	}
}

func TestWindowAdmission(t *testing.T) {
	ok := func(st Stage) error { return validateProjectionStageShapes(stageConfig(st)) }
	good := windowStage([]Emit{{Field: "n", Expr: "row_number()"}, {Field: "x", From: "$.x"}}, "$.id")
	require.NoError(t, ok(good))

	noTie := windowStage([]Emit{{Field: "r", Expr: "rank()"}, {Field: "s", Expr: "sum($.v)"}})
	require.NoError(t, ok(noTie), "rank and running aggregates treat ties as peers")
	noPartition := good
	noPartition.PartitionBy = nil
	require.NoError(t, ok(noPartition), "no partitionBy = one partition")

	for name, tc := range map[string]struct {
		mut  func(*Stage)
		want string
	}{
		"tie-sensitive without tieBy": {func(s *Stage) { s.WindowTieBy = nil }, "declare tieBy"},
		"no orderBy":                  {func(s *Stage) { s.WindowOrderBy = nil }, "needs orderBy"},
		"keyPath":                     {func(s *Stage) { s.KeyPath = []string{"$.k"} }, "keyPath is not used on a window stage"},
		"forEach":                     {func(s *Stage) { s.ForEach = "$.xs[*]"; s.ElementKey = "$.id" }, "fan in a prior stage"},
		"keyType":                     {func(s *Stage) { s.KeyType = []string{"text"} }, "not declarable on a window stage"},
		"bad order key":               {func(s *Stage) { s.WindowOrderBy = []string{"$.o sideways"} }, "unexpected trailing"},
		"division in order":           {func(s *Stage) { s.WindowOrderBy = []string{"$.a / $.b"} }, "round"},
		"over with a window":          {func(s *Stage) { s.Emit[0].Expr = "sum($.x) over (order by $.x)" }, "takes only a frame"},
		"frame on row_number":         {func(s *Stage) { s.Emit[0].Expr = "row_number() over (rows 1 preceding)" }, "does not use a frame"},
		"nested":                      {func(s *Stage) { s.Emit[0].Expr = "lag(row_number())" }, "nested"},
		"distinct":                    {func(s *Stage) { s.Emit[0].Expr = "count(distinct $.x)" }, "distinct is not supported"},
		"array_agg":                   {func(s *Stage) { s.Emit[0].Expr = "array_agg($.x)" }, "collapse with an aggregate stage"},
		"offset expression":           {func(s *Stage) { s.Emit[0].Expr = "lag($.x, $.n)" }, "integer literal"},
		"filter on ranking":           {func(s *Stage) { s.Emit[0].Expr = "rank() filter (where $.x)" }, "aggregate window functions only"},
		"avg unrounded":               {func(s *Stage) { s.Emit[0].Expr = "avg($.x)" }, "round"},
		"latest's orderBy":            {func(s *Stage) { s.OrderBy = "$.o" }, "expression lists"},
		"window fields elsewhere":     {func(s *Stage) { s.Reduce = ""; s.KeyPath = []string{"$.k"} }, "only for reduce = \"window\""},
	} {
		st := windowStage([]Emit{{Field: "n", Expr: "row_number()"}}, "$.id")
		tc.mut(&st)
		require.ErrorContains(t, ok(st), tc.want, name)
	}

	// Window functions outside a window stage name their home.
	_, err := Compile("row_number()")
	require.ErrorContains(t, err, "window stage")
	agg := aggStage([]Emit{{Field: "v", Expr: "lag($.x)"}}, "")
	require.ErrorContains(t, validateProjectionStageShapes(stageConfig(agg...)), "window stage")
}

// A stage-fed window keys its rows by the upstream's keys, so its key
// space is the upstream's; a topic-fed window's is the producer's.
func TestWindowKeySpace(t *testing.T) {
	up := Stage{Name: "up", From: "rows", KeyPath: []string{"$.a", "$.b"}, KeyType: []string{"number"}, Emit: []Emit{{Field: "a", From: "$.a"}}}
	w := windowStage([]Emit{{Field: "n", Expr: "rank()"}})
	w.From, w.PartitionBy, w.WindowOrderBy = "up", nil, []string{"$.a"}
	arity, kt, _ := ResolvedKeySpace([]Stage{up, w}, 1)
	require.Equal(t, 2, arity)
	require.Equal(t, []string{"number"}, kt)
	arity, _, _ = ResolvedKeySpace([]Stage{windowStage(nil)}, 0)
	require.Equal(t, KeyArityProducer, arity)
}
