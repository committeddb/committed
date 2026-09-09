package stages

import (
	"encoding/binary"
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
	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// TestStoreFormatGolden pins the stage store's bytes — every bucket, key,
// and value a fixture pipeline leaves behind — against a golden chosen by
// the format number the store itself records. The config fingerprint
// resets a store when the DECLARATION changes; this golden covers what the
// engine chooses on its own (key framing, fan-element identity, retained
// input shape, synthetic stage names, bucket names, key-part rendering, the
// collect/min/max order, canonical output JSON). A change to any of those
// under an unchanged fingerprint would leave existing stores holding state
// folded by the old rule while new events fold by the new one — a silent
// divergence from a cold replay that nothing resets.
//
// So the rule is structural: the bytes and the format number move
// together. A changed dump under the current number fails here. Bumping
// stagestore.formatVersion makes this test look for a golden that does not
// exist yet, which is generated deliberately:
//
//	UPDATE_STORE_GOLDEN=1 go test ./internal/cluster/syncable/stages -run TestStoreFormatGolden
//
// and committed alongside the bump. The bump is what resets every existing
// store on upgrade (stagestore/format_reset_test.go), so the two rules never
// mix. Coverage is structural too: TestStoreGoldenFixtureCoversTheVocabulary
// fails when the fixture leaves any vocabulary field unset.
func TestStoreFormatGolden(t *testing.T) {
	sts := storeGoldenFixture()
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
	storeGoldenFeed(fold, sweep, del)
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error { return tx.SetFrontier(14) }))
	require.NoError(t, store.Close())

	dump, format := dumpStore(t, stagestore.FilePath(dir, "fixture"))
	goldenPath := filepath.Join("testdata", fmt.Sprintf("store_format_%d.golden", format))
	if os.Getenv("UPDATE_STORE_GOLDEN") == "1" {
		require.NoError(t, os.WriteFile(goldenPath, []byte(dump), 0o644))
		t.Logf("wrote %s", goldenPath)
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "no golden for store format %d: the format number was bumped — generate its golden deliberately (UPDATE_STORE_GOLDEN=1) and commit it with the bump", format)
	require.Equal(t, string(want), dump,
		"the stage store's bytes changed under format %d. Existing stores would silently mix two rules; bump stagestore.formatVersion (resetting them on upgrade) and regenerate the golden for the new number", format)
}

// storeGoldenFixture is the pipeline TestStoreFormatGolden folds. It sets
// every field of the stage vocabulary somewhere (enforced by
// TestStoreGoldenFixtureCoversTheVocabulary), so a new construct cannot be
// added to the grammar without being added here, where its store bytes get
// pinned.
func storeGoldenFixture() []Stage {
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
	}
}

// storeGoldenFeed drives the fixture: every topic, a filtered-out input, a
// key that canonicalizes, both fan arms, mixed collect families, an argmax
// tie, real refresh epochs (generation 0 = a direct write, never swept;
// 1 and 2 = ingest epochs) with one refresh-boundary sweep, and one
// tombstone.
func storeGoldenFeed(fold func(topic, key, payload string, gen uint64), sweep func(topic string, marker uint64), del func(topic, key string)) {
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
}

// TestStoreGoldenFixtureCoversTheVocabulary makes the golden's coverage
// structural: every exported, config-tagged field of Stage, FanArm,
// MergeEntry, Join, Emit, and WhenClause must be set (non-zero) somewhere in
// storeGoldenFixture. A new vocabulary word therefore fails here until the
// fixture exercises it, which is the moment its store bytes get pinned.
func TestStoreGoldenFixtureCoversTheVocabulary(t *testing.T) {
	seen := map[string]bool{}
	var walk func(v reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Ptr, reflect.Interface:
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
	walk(reflect.ValueOf(storeGoldenFixture()))

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
	require.Empty(t, missing, "vocabulary fields the store-golden fixture never sets — add a stage that uses each, so its store bytes are pinned")
}

// dumpStore renders every bucket, key, and value of a closed store file in
// bucket order — bucket names and keys Go-quoted (they carry framing and
// namespace bytes), values raw when printable and quoted otherwise — so the
// golden is plain text and a diff names the entry that moved. It also
// returns the format number the store recorded in its meta bucket.
func dumpStore(t *testing.T, path string) (string, uint64) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var sb strings.Builder
	var format uint64
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(k, v []byte) error {
				if string(name) == "meta" && string(k) == "format" {
					format = binary.BigEndian.Uint64(v)
				}
				fmt.Fprintf(&sb, "%s\t%s\t%s\n", strconv.Quote(string(name)), strconv.Quote(string(k)), renderValue(v))
				return nil
			})
		})
	}))
	return sb.String(), format
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
