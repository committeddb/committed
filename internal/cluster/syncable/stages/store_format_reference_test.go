package stages

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// TestStoreFormatReference pins the stage store's bytes — every bucket, key,
// and value a fixture pipeline leaves behind — against a reference chosen by
// the format number the store itself records. The config fingerprint
// resets a store when the DECLARATION changes; this reference covers what the
// engine chooses on its own (key framing, fan-element identity, retained
// input shape, synthetic stage names, bucket names, key-part rendering, the
// collect/min/max order, canonical output JSON). A change to any of those
// under an unchanged fingerprint would leave existing stores holding state
// folded by the old rule while new events fold by the new one — a silent
// divergence from a cold replay that nothing resets.
//
// So the rule is structural: the bytes and the format number move
// together. A changed dump under the current number fails here. Bumping
// stagestore.formatVersion makes this test look for a reference that does not
// exist yet, which is generated deliberately:
//
//	UPDATE_STORE_REFERENCE=1 go test ./internal/cluster/syncable/stages -run TestStoreFormatReference
//
// and committed alongside the bump. The bump is what resets every existing
// store on upgrade (stagestore/format_reset_test.go), so the two rules never
// mix. Coverage is structural too: TestStoreReferenceFixtureCoversTheVocabulary
// fails when the fixture leaves any vocabulary field unset.
func TestStoreFormatReference(t *testing.T) {
	sts := storeReferenceFixture()
	require.NoError(t, ValidateShapes(sts))
	g := BuildGraph(sts)

	dir := t.TempDir()
	store, _, err := stagestore.Open(dir, "fixture", Fingerprint(sts))
	require.NoError(t, err)
	// The production entry points: an upsert stamped with its ingest refresh
	// epoch (the generation the retained input carries), a refresh-boundary
	// sweep, a tombstone — each followed by the drain the projection runs
	// per Actual.
	fold := func(topic, key, payload string, gen uint64) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			if err := g.FoldTopicUpsert(tx, topic, []byte(key), decodePayload(t, payload), gen, dirty); err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	sweep := func(topic string, marker uint64) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			dirty := Dirty{}
			if err := g.SweepEpochs(tx, topic, marker, dirty); err != nil {
				return err
			}
			return g.Drain(tx, dirty)
		}))
	}
	del := func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	storeReferenceFeed(fold, sweep, del)
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error { return tx.SetFrontier(14) }))
	require.NoError(t, store.Close())

	dump, format := dumpStore(t, dir, "fixture")
	referencePath := filepath.Join("testdata", fmt.Sprintf("store_format_%d.reference", format))
	if os.Getenv("UPDATE_STORE_REFERENCE") == "1" {
		require.NoError(t, os.WriteFile(referencePath, []byte(dump), 0o644))
		t.Logf("wrote %s", referencePath)
	}
	want, err := os.ReadFile(referencePath)
	require.NoError(t, err, "no reference for store format %d: the format number was bumped — generate its reference deliberately (UPDATE_STORE_REFERENCE=1) and commit it with the bump", format)
	require.Equal(t, string(want), dump,
		"the stage store's bytes changed under format %d. Existing stores would silently mix two rules; bump stagestore.formatVersion (resetting them on upgrade) and regenerate the reference for the new number", format)
}

// storeReferenceFixture is the pipeline TestStoreFormatReference folds. It sets
// every field of the stage vocabulary somewhere (enforced by
// TestStoreReferenceFixtureCoversTheVocabulary), so a new construct cannot be
// added to the grammar without being added here, where its store bytes get
// pinned.
func storeReferenceFixture() []Stage {
	return []Stage{
		// Reshape: when-expr, computed emit, a NUMBER-typed key (canonical
		// rendering: "5.0000" and 5 are one key).
		{
			Name: "props", From: "proposals", KeyPath: []string{"$.id"}, KeyType: []string{KeyTypeNumber},
			When: []WhenClause{{Expr: "coalesce($.amount, 0) > 0"}},
			Emit: []Emit{{Field: "pid", From: "$.projectId"}, {Field: "amount", Expr: "$.amount + 1"}},
		},
		// Multi-arm fan into an aggregate: arm-namespaced element identity,
		// the retained {e,p} wrapper, lowercase key normalization, every
		// fold arm (sum, count+where, distinct collect over MIXED families,
		// min/max over text).
		{
			Name: "els", From: "txn-events", KeyPath: []string{"$.wa"}, Normalize: NormalizeLower,
			Fan: []FanArm{
				{ForEach: "$.elements[*]", When: []WhenClause{{Path: "$.type", Equals: "created"}}},
				{ForEach: "$.added[*]", When: []WhenClause{{Path: "$.type", Equals: "elements-added"}}},
			},
			ElementKey: "$.id", Reduce: "aggregate",
			Emit: []Emit{
				{Field: "total", Sum: "$.amount"},
				{Field: "big", Count: true, Where: []WhenClause{{Expr: "$.amount > 5"}}},
				{Field: "small", Count: true, Where: []WhenClause{{Path: "$.amount", LessThan: 6}}},
				{Field: "vals", Collect: "$.v", Distinct: true},
				{Field: "first", Min: "$.tag"},
				{Field: "last", Max: "$.tag"},
			},
		},
		// Argmax with a numeric tiebreak.
		{
			Name: "latest-status", From: "status-events", KeyPath: []string{"$.job"},
			Reduce: "latest", OrderBy: "$.ts", OrderByType: KeyTypeNumber, TieBy: "$.seq", TieByType: KeyTypeNumber,
			Emit: []Emit{{Field: "status", From: "$.status"}},
		},
		// A field-addressed stage join (synthetic `props[$.pid]` stage) and a
		// topic join (dim/rev buckets).
		{
			Name: "cand", From: "projects", KeyPath: []string{"$.id"}, Normalize: NormalizeLower,
			When: []WhenClause{{Path: "$.status", NotEquals: "archived"}},
			Joins: []Join{
				{From: "props", On: []string{"$.id"}, Field: "$.pid", As: "lp", Optional: true},
				{
					Topic: "owners", On: []string{"$.ownerId"}, As: "own", Optional: true,
					Normalize: NormalizeLower, OnType: []string{KeyTypeText},
					Where: []WhenClause{{Path: "$.name", NotNull: true}},
				},
				{Topic: "blocks", On: []string{"$.id"}, Absent: true, Normalize: NormalizeLower},
			},
			Emit: []Emit{{Field: "amount", Expr: "coalesce($.lp.amount, 0)"}, {Field: "owner", From: "$.own.name"}},
		},
		// Single-path forEach (raw element identity, no arm lead) with
		// element-level scalar when arms.
		{
			Name: "lines", From: "orders", KeyPath: []string{"$.order"}, ForEach: "$.lines[*]", ElementKey: "$.sku",
			When:   []WhenClause{{Path: "$.qty", GreaterThan: 0}, {Path: "$.note", Null: true}},
			Reduce: "aggregate",
			Emit:   []Emit{{Field: "qty", Sum: "$.qty"}},
		},
		// liveSet: created-minus-deleted membership with a deleteWhen classifier.
		{
			Name: "txn-live", From: "txn-events", KeyPath: []string{"$.txn"},
			Reduce:     "liveSet",
			DeleteWhen: []WhenClause{{Path: "$.type", Equals: "deleted"}},
			Emit:       []Emit{{Field: "kind", From: "$.type"}},
		},
		// A merge of a stage side and a topic side (synthetic lift stage
		// `work-areas[$.Id]`).
		{
			Name: "job",
			Merge: []MergeEntry{
				{Stage: "cand", As: "c"},
				{Topic: "work-areas", KeyPath: []string{"$.Id"}, KeyType: []string{KeyTypeText}, Normalize: NormalizeLower, As: "wa"},
			},
			Emit: []Emit{{Field: "amount", From: "$.c.amount"}, {Field: "area", From: "$.wa.name"}},
		},
		// Aggregate expressions and having (SQL's select list and HAVING)
		// over a computed key (GROUP BY <expression>): key rendering of a
		// time value, avg's exact quotient under round, FILTER, a distinct
		// count, a constant column, and a month the having holds back.
		{
			Name: "monthly", From: "payments", KeyPath: []string{"date_trunc('month', $.at::timestamptz)"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "avg", Expr: "round(avg($.amount), 2)"},
				{Field: "big", Expr: "count(*) filter (where $.amount > 5)"},
				{Field: "payers", Expr: "count(distinct lower($.payer))"},
				{Field: "source", Expr: "'ledger'"},
			},
			Having: "sum($.amount) > 0",
		},
		// Ordered aggregates: array_agg's order-by with a descending key and
		// a null element, string_agg's distinct value order, and the
		// value-order tiebreak — engine-chosen orders the store pins.
		{
			Name: "payer-list", From: "payments", KeyPath: []string{"'all'"},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "by_amount", Expr: "array_agg(lower($.payer) order by $.amount desc)"},
				{Field: "names", Expr: "string_agg(distinct $.payer, ', ' order by $.payer)"},
				{Field: "notes", Expr: "array_agg($.note)"},
			},
		},
		// Window: one row per input in business order — the ord/pos/row
		// buckets, the order-preserving sort key (a time value descending,
		// a numeric tie), a computed partition, each row's running state,
		// and peers (equal orderBy) sharing the running sum.
		{
			Name: "ledger-rows", From: "ledger", Reduce: "window",
			PartitionBy: []string{"lower($.acct)"}, WindowOrderBy: []string{"$.at::timestamptz desc"}, WindowTieBy: []string{"$.seq"},
			Emit: []Emit{
				{Field: "n", Expr: "row_number()"},
				{Field: "bal", Expr: "sum($.amount)"},
				{Field: "prev", Expr: "lag($.amount, 1, 0)"},
				{Field: "first", Expr: "first_value($.seq)"},
				{Field: "seq", From: "$.seq"},
			},
		},
		// Window frames: a ROWS moving sum and a RANGE (interval) count —
		// framed calls keep no prefix accumulators in the row state.
		{
			Name: "ledger-frames", From: "ledger", Reduce: "window",
			PartitionBy: []string{"lower($.acct)"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"$.seq"},
			Emit: []Emit{
				{Field: "ma2", Expr: "sum($.amount) over (rows between 1 preceding and current row)"},
				{Field: "day_n", Expr: "count(*) over (range between interval '1 day' preceding and current row)"},
				{Field: "final", Expr: "last_value($.seq) over (rows between unbounded preceding and unbounded following)"},
			},
		},
		// UNION ALL of a topic and a prior stage: the (source, key) input
		// identity, source() in when and in aggregate arguments.
		{
			Name: "activity", FromList: []string{"refunds", "props"}, KeyPath: []string{"coalesce($.acct, $.pid)"},
			When:   []WhenClause{{Expr: "source() = 'props' or $.amount > 0"}},
			Reduce: "aggregate",
			Emit: []Emit{
				{Field: "n", Expr: "count(*)"},
				{Field: "refunded", Expr: "sum($.amount) filter (where source() = 'refunds')"},
				{Field: "srcs", Expr: "string_agg(distinct source(), ',' order by source())"},
			},
		},
		// latest's list form over a union: a descending cast order key (the
		// earliest wins) tied by source().
		{
			Name: "first-activity", FromList: []string{"refunds", "refund-notes"}, KeyPath: []string{"lower($.acct)"},
			Reduce: "latest", WindowOrderBy: []string{"$.at::date desc"}, WindowTieBy: []string{"source()"},
			Emit: []Emit{{Field: "kind", Expr: "source()"}, {Field: "at", From: "$.at"}},
		},
		// An optional fan join on a prior stage's field: the pairs' framed
		// identities, the mdim/mpos/parent buckets, the null pair.
		{
			Name: "member-props", From: "proj-members", KeyPath: []string{"$.user", "coalesce($.pp.amount, 0)"},
			Joins: []Join{{From: "props", Field: "$.pid", On: []string{"$.project"}, As: "pp", Optional: true, Fan: true}},
			Emit:  []Emit{{Field: "amount", From: "$.pp.amount"}},
		},
		// An as-of join on a topic's field: the version index (a date
		// order key, a tie, the producer key), each version's position, and
		// the dependents indexed by their bound.
		{
			Name: "fx-usd", From: "fx-payments", KeyPath: []string{"$.id"},
			Joins: []Join{{
				Topic: "fx-rates", Field: "$.ccy", On: []string{"$.ccy"}, Normalize: NormalizeLower,
				OrderBy: []string{"$.day::date"}, TieBy: []string{"$.seq"}, AsOf: "$.paid::date", As: "fx", Optional: true,
			}},
			Emit: []Emit{{Field: "usd", Expr: "round($.amount * $.fx.rate, 2)"}},
		},
	}
}

// storeReferenceFeed drives the fixture: every topic, a filtered-out input, a
// key that canonicalizes, both fan arms, mixed collect families, an argmax
// tie, real refresh epochs (generation 0 = a direct write, never swept;
// 1 and 2 = ingest epochs) with one refresh-boundary sweep, and one
// tombstone.
func storeReferenceFeed(fold func(topic, key, payload string, gen uint64), sweep func(topic string, marker uint64), del func(topic, key string)) {
	fold("proposals", "p1", `{"id":"5.0000","projectId":"J1","amount":10}`, 1)
	fold("proposals", "p2", `{"id":7,"projectId":"J2","amount":0}`, 1) // filtered by the when-expr
	fold("proposals", "p3", `{"id":8,"projectId":"J2","amount":3}`, 1)
	fold("txn-events", "e1", `{"type":"created","txn":"t1","elements":[{"id":"a","wa":"W1","amount":10,"tag":"m","v":10},{"id":"b","wa":"w1","amount":5,"tag":"b","v":"b"}]}`, 1)
	fold("txn-events", "e2", `{"type":"elements-added","txn":"t1","added":[{"id":"a","wa":"w1","amount":7,"tag":"z","v":2},{"id":"c","wa":"w1","amount":1,"tag":"a","v":"B"}]}`, 1)
	fold("txn-events", "e3", `{"type":"created","txn":"t2","elements":[{"id":"d","wa":"w1","amount":6,"tag":"k","v":true},{"id":"e","wa":"w1","amount":6,"tag":"k","v":2.5},{"id":"f","wa":"w1","amount":6,"tag":"k","v":false},{"id":"g","wa":"w1","amount":6,"tag":"k","v":"a"},{"id":"h","wa":"w1","amount":6,"tag":"k","v":""},{"id":"i","wa":"w1","amount":6,"tag":"k","v":2}]}`, 2)
	fold("txn-events", "e4", `{"type":"deleted","txn":"t2"}`, 2) // liveSet retraction of t2
	fold("status-events", "s1", `{"job":"j1","ts":20260102,"seq":1,"status":"open"}`, 0)
	fold("status-events", "s2", `{"job":"j1","ts":20260102,"seq":2,"status":"done"}`, 0)
	fold("status-events", "s3", `{"job":"j1","ts":20260101,"seq":9,"status":"stale"}`, 0)
	fold("owners", "o1", `{"id":"o1","name":"Ada"}`, 1)
	fold("owners", "o3", `{"id":"o3","name":null}`, 2)  // fails the join's where
	fold("owners", "o4", `{"id":"o4","name":"Bea"}`, 1) // captured under epoch 1: swept below
	fold("projects", "j1", `{"id":"J1","ownerId":"O1","status":"open"}`, 1)
	fold("projects", "j2", `{"id":"J2","ownerId":"o2","status":"open"}`, 1)
	fold("projects", "j3", `{"id":"J3","ownerId":"o3","status":"archived"}`, 1) // filtered by the stage when
	fold("projects", "j4", `{"id":"J4","ownerId":"o4","status":"open"}`, 1)
	fold("blocks", "j2", `{"blocked":true}`, 0) // the absent join is entity-keyed and normalized: it drops J2
	fold("work-areas", "w1", `{"Id":"J1","name":"east"}`, 0)
	fold("orders", "r1", `{"lines":[{"order":"r1","sku":"s1","qty":2,"note":null},{"order":"r1","sku":"s2","qty":0,"note":null},{"order":"r1","sku":"s3","qty":4}]}`, 1)
	// A refresh boundary at epoch 2 on owners: o1 and o4 (epoch 1) were not
	// re-asserted by the re-snapshot, so they retract and their dependents
	// refold; o3 (epoch 2) survives.
	sweep("owners", 2)
	del("proposals", "p3")
	fold("payments", "q1", `{"at":"2026-01-05T10:00:00Z","amount":10,"payer":"Ada"}`, 0)
	fold("payments", "q2", `{"at":"2026-01-20","amount":3,"payer":"ada"}`, 0)
	fold("payments", "q3", `{"at":"2026-01-31 20:00:00-05:00","amount":-4,"payer":"Bea"}`, 0) // Feb 1 UTC: Feb's sum is -4, held back
	fold("ledger", "l1", `{"acct":"A","at":"2026-01-02","seq":1,"amount":5}`, 0)
	fold("ledger", "l2", `{"acct":"a","at":"2026-01-01","seq":2,"amount":-2.5}`, 0)
	fold("ledger", "l3", `{"acct":"B","at":"2026-01-03","seq":3,"amount":1}`, 0)
	fold("ledger", "l4", `{"acct":"a","at":"2026-01-02","seq":4,"amount":2}`, 0) // a peer of l1
	fold("ledger", "l3", `{"acct":"a","at":"2026-01-03","seq":3,"amount":1}`, 0) // moves partitions
	del("ledger", "l2")
	fold("refunds", "r1", `{"acct":"J1","amount":2,"at":"2026-01-03"}`, 1)
	fold("refunds", "r2", `{"acct":"J9","amount":0,"at":"2026-01-01"}`, 1) // filtered out of activity by its when
	fold("refund-notes", "r1", `{"acct":"j1","at":"2026-01-02"}`, 1)       // refunds' key r1 too: a separate row
	fold("refunds", "r3", `{"acct":"j1","amount":1,"at":"2026-01-02"}`, 2) // ties refund-notes r1; source() breaks it
	fold("refunds", "r4", `{"acct":"J1","amount":5,"at":"2026-01-04"}`, 2) // joins props' J1 row in activity
	sweep("refunds", 2)                                                    // r1, r2 retract; refund-notes r1 is another source's
	fold("refund-notes", "n2", `{"acct":"j1","at":"2026-01-01"}`, 0)
	del("refund-notes", "n2")                                      // a tombstone retracts its own source's row only
	fold("proj-members", "pm1", `{"user":"u1","project":"J1"}`, 1) // pairs with props' J1 row
	fold("proj-members", "pm2", `{"user":"u2","project":"J7"}`, 1) // no match: the null pair
	fold("fx-rates", "r1", `{"ccy":"EUR","day":"2026-01-01","seq":1,"rate":1.1}`, 1)
	fold("fx-rates", "r2", `{"ccy":"eur","day":"2026-01-10","seq":2,"rate":1.2}`, 2)
	fold("fx-payments", "x1", `{"id":"x1","ccy":"EUR","amount":100,"paid":"2026-01-05"}`, 0)
	fold("fx-payments", "x2", `{"id":"x2","ccy":"EUR","amount":100,"paid":"2025-12-31"}`, 0) // before any version
	sweep("fx-rates", 2)                                                                     // r1 retracts: x1 falls to nothing
	fold("fx-rates", "r0", `{"ccy":"EUR","day":"2025-12-01","seq":0,"rate":1.05}`, 2)        // backfill: x1 and x2 resolve to it
}

// TestStoreReferenceFixtureCoversTheVocabulary makes the reference's coverage
// structural: every exported, config-tagged field of Stage, FanArm,
// MergeEntry, Join, Emit, and WhenClause must be set (non-zero) somewhere in
// storeReferenceFixture. A new vocabulary word therefore fails here until the
// fixture exercises it, which is the moment its store bytes get pinned.
func TestStoreReferenceFixtureCoversTheVocabulary(t *testing.T) {
	seen := map[string]bool{}
	var walk func(v reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		case reflect.Struct:
			tp := v.Type()
			for i := 0; i < tp.NumField(); i++ {
				f := tp.Field(i)
				if !f.IsExported() {
					continue
				}
				fv := v.Field(i)
				if !fv.IsZero() {
					seen[tp.Name()+"."+f.Name] = true
				}
				walk(fv)
			}
		}
	}
	walk(reflect.ValueOf(storeReferenceFixture()))

	var missing []string
	for _, tp := range []reflect.Type{
		reflect.TypeOf(Stage{}), reflect.TypeOf(FanArm{}), reflect.TypeOf(MergeEntry{}),
		reflect.TypeOf(Join{}), reflect.TypeOf(Emit{}), reflect.TypeOf(WhenClause{}),
	} {
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			if !f.IsExported() || (f.Tag.Get("mapstructure") == "" && f.Tag.Get("json") == "") {
				continue
			}
			if !seen[tp.Name()+"."+f.Name] {
				missing = append(missing, tp.Name()+"."+f.Name)
			}
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "vocabulary fields the store-reference fixture never sets — add a stage that uses each, so its store bytes are pinned")
}

// dumpStore renders every bucket, key, and value of a closed store file in
// bucket order — bucket names and keys Go-quoted (they carry framing and
// namespace bytes), values raw when printable and quoted otherwise — so the
// reference is plain text and a diff names the entry that moved. It also
// returns the format number the store recorded in its meta bucket.
func dumpStore(t *testing.T, dir, name string) (string, uint64) {
	t.Helper()
	var sb strings.Builder
	var format uint64
	require.NoError(t, stagestore.Inspect(dir, name, func(bucket, k, v []byte) error {
		if string(bucket) == "meta" && string(k) == "format" {
			format = binary.BigEndian.Uint64(v)
		}
		if strings.HasSuffix(string(bucket), ":acc") {
			v = renderAggState(t, v)
		}
		if strings.HasSuffix(string(bucket), ":in") {
			v = renderRetained(v)
		}
		fmt.Fprintf(&sb, "%s\t%s\t%s\n", strconv.Quote(string(bucket)), strconv.Quote(string(k)), renderValue(v))
		return nil
	}))
	return sb.String(), format
}

// renderAggState renders a fold state in the JSON form, so the reference pins
// what a state holds rather than how it is encoded: both encodings decode to
// the same state (decodeAggState), and the encoding has its own pin
// (TestAggStateBinaryLayout). A winner state (winner.go) renders as its
// fields.
func renderAggState(t *testing.T, v []byte) []byte {
	t.Helper()
	if len(v) > 0 && v[0] == '{' {
		return v
	}
	if len(v) > 0 && v[0] == winnerStateV1 {
		ws, err := decodeWinnerState(v)
		require.NoError(t, err)
		var winner any
		if ws.inKey != nil {
			winner = string(ws.inKey)
		}
		out, err := json.Marshal(map[string]any{"inputs": ws.inputs, "dead": ws.dead, "winner": winner, "rank": ws.rank})
		require.NoError(t, err)
		return out
	}
	as, err := parseAggState(v)
	require.NoError(t, err)
	out, err := encodeAggStateJSON(as)
	require.NoError(t, err)
	return out
}

func renderValue(v []byte) string {
	if v == nil {
		return "-"
	}
	if utf8.Valid(v) && strings.IndexFunc(string(v), func(r rune) bool { return !unicode.IsPrint(r) }) < 0 {
		return string(v)
	}
	return strconv.Quote(string(v))
}
