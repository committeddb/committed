package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// Classification never changes a key that resolves today: a $ path and
// the jsonpath library's bare forms stay paths; an entry that compiles to
// more than a path is an expression; an entry that never resolved (the
// library cannot parse it, or parses a function call it cannot run) is a
// loud error unless it is a valid expression.
func TestClassifyKeyPath(t *testing.T) {
	for kp, wantExpr := range map[string]bool{
		"$.id":           false,
		"$.items[0].sku": false,
		"$parent.id":     false,
		"tenant_id":      false, // the jsonpath library resolves bare names
		"a.b":            false,
		"$.a == 1":       false, // the library's own syntax: unchanged
		"lower($.email)": true,
		"date_trunc('month', $.paid_at::timestamptz)": true,
		"$.a - 1":        true, // never parsed as a jsonpath, so never resolved
		"$.qty::numeric": true,
		"'all'":          true, // a constant key: one group
	} {
		_, isExpr, err := ClassifyKeyPath(kp)
		require.NoError(t, err, kp)
		require.Equal(t, wantExpr, isExpr, kp)
	}
	for _, kp := range []string{"lower(email)", "$.[bad", "date_trunc('fortnight', $.t::timestamptz)"} {
		_, _, err := ClassifyKeyPath(kp)
		require.Error(t, err, kp)
	}
}

func TestKeyExpressionAdmission(t *testing.T) {
	mk := func(kp ...string) error {
		return ValidateShapes([]Stage{{
			Name: "s", From: "t", KeyPath: kp,
			Joins: []Join{{Topic: "projects", On: []string{"$.pid"}, As: "project"}},
			Emit:  []Emit{{Field: "v", From: "$.v"}},
		}})
	}
	require.NoError(t, mk("lower(trim($.email))"))
	require.NoError(t, mk("$.customer", "date_trunc('month', $.paid_at::timestamptz)"))
	require.ErrorContains(t, mk("date_trunc('fortnight', $.t::timestamptz)"), "neither a path nor a valid expression")
	require.ErrorContains(t, mk("round($.a / 3, 0) + $.b / 3"), "round(...) or trunc(...)", "a key is stored: no bare division")
	require.ErrorContains(t, mk("$.items[*].sku"), "multi-valued", "paths keep their old checks")
	require.ErrorContains(t, mk("upper($.project.code)"), "REFOLD", "a key is fold-time: no alias reads, even inside an expression")

	// A stage-fed stage's key expression may only read what the upstream emits.
	err := ValidateShapes([]Stage{
		{Name: "a", From: "t", KeyPath: []string{"$.id"}, Emit: []Emit{{Field: "email", From: "$.email"}}},
		{Name: "b", From: "a", KeyPath: []string{"lower($.mail)"}, Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}}},
	})
	require.ErrorContains(t, err, "mail")

	// A topic merge side's keyPath takes an expression too (it is a stage's key).
	err = ValidateShapes([]Stage{
		{Name: "a", From: "t", KeyPath: []string{"lower($.email)"}, Emit: []Emit{{Field: "x", From: "$.x"}}},
		{Name: "m", Merge: []MergeEntry{{Stage: "a"}, {Topic: "u", KeyPath: []string{"lower($.mail)"}, As: "u"}}, Emit: []Emit{{Field: "y", From: "$.u.y"}}},
	})
	require.NoError(t, err)
	err = ValidateShapes([]Stage{
		{Name: "a", From: "t", KeyPath: []string{"$.id"}, Emit: []Emit{{Field: "x", From: "$.x"}}},
		{Name: "m", Merge: []MergeEntry{{Stage: "a"}, {Topic: "u", KeyPath: []string{"lower(mail)"}, As: "u"}}, Emit: []Emit{{Field: "y", From: "$.u.y"}}},
	})
	require.ErrorContains(t, err, "neither a path nor a valid expression")
}

// GROUP BY date_trunc: inputs fold under their computed key, a changed
// value rekeys (the old key refolds without it), a tombstone retracts by
// the retained key (no payload needed), and an input whose expression is
// null or fails is a non-member.
func TestKeyExpressionFolds(t *testing.T) {
	stages := []Stage{{
		Name: "by-month", From: "payments",
		KeyPath: []string{"$.customer", "date_trunc('month', $.paid_at::timestamptz)"},
		Reduce:  "aggregate",
		Emit:    []Emit{{Field: "total", Sum: "$.amount"}, {Field: "n", Count: true}},
	}}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	upsert := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "payments", []byte(key), decodePayload(t, payload))
		}))
	}
	out := func(parts ...string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("by-month", []byte(OutKey(parts)))
			got = string(v)
			return err
		}))
		return got
	}
	jan, feb := "2024-01-01 00:00:00+00:00", "2024-02-01 00:00:00+00:00"

	upsert("p1", `{"customer":"c1","paid_at":"2024-01-05T10:00:00Z","amount":10}`)
	upsert("p2", `{"customer":"c1","paid_at":"2024-01-31 20:00:00-05:00","amount":5}`) // Feb 1 01:00 UTC
	upsert("p3", `{"customer":"c1","paid_at":"2024-01-20","amount":2.5}`)
	require.Equal(t, `{"n":2,"total":12.5}`, out("c1", jan))
	require.Equal(t, `{"n":1,"total":5}`, out("c1", feb), "the offset decides the month: Jan 31 -05:00 is Feb 1 UTC")

	// Rekey: p3 moves to February.
	upsert("p3", `{"customer":"c1","paid_at":"2024-02-02","amount":2.5}`)
	require.Equal(t, `{"n":1,"total":10}`, out("c1", jan))
	require.Equal(t, `{"n":2,"total":7.5}`, out("c1", feb))

	// A tombstone carries no payload; the retained key retracts it.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "payments", []byte("p1"))
	}))
	require.Empty(t, out("c1", jan), "January emptied → its key retracts")

	// A null or unparsable key value is non-membership — and an input
	// that stops resolving retracts from its old key.
	upsert("p4", `{"customer":"c1","amount":100}`)
	upsert("p2", `{"customer":"c1","paid_at":"not a date","amount":5}`)
	require.Equal(t, `{"n":1,"total":2.5}`, out("c1", feb), "p2's data error retracts it; p4 never joins")
}

// keyType and normalize render the expression's materialized value, as
// they render a path's.
func TestKeyExpressionComposesWithKeyTypeAndNormalize(t *testing.T) {
	stages := []Stage{{
		Name: "by-email", From: "signups", KeyPath: []string{"trim($.email)", "$.qty::numeric"},
		KeyType: []string{KeyTypeText, KeyTypeNumber}, Normalize: NormalizeLower,
		Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}},
	}}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	for k, p := range map[string]string{
		"s1": `{"email":"  Ada@X.com ","qty":"5.000"}`,
		"s2": `{"email":"ada@x.com","qty":5}`,
	} {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "signups", []byte(k), decodePayload(t, p))
		}))
	}
	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut("by-email", []byte(OutKey([]string{"ada@x.com", "5"})))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"n":2}`, got, "trimmed, lowercased, and canonical-numbered keys collapse")

	key, ok, err := ProbeKey(cfg.Stages, 0, []string{"ADA@X.COM", "5.0"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, OutKey([]string{"ada@x.com", "5"}), key, "a probe renders the computed key's value like any key")
}

// A later stage can join a computed-key stage: its on value addresses the
// rendered key.
func TestJoinIntoKeyExpressionStage(t *testing.T) {
	stages := []Stage{
		{
			Name: "monthly", From: "payments", KeyPath: []string{"date_trunc('month', $.paid_at::timestamptz)"},
			Reduce: "aggregate", Emit: []Emit{{Field: "total", Sum: "$.amount"}},
		},
		{
			Name: "budgets", From: "budget-topic", KeyPath: []string{"$.id"},
			Joins: []Join{{From: "monthly", On: []string{"$.month"}, As: "spent"}},
			Emit:  []Emit{{Field: "left", Expr: "$.budget - coalesce($.spent.total, 0)"}},
		},
	}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	fold("payments", "p1", `{"paid_at":"2024-03-09T12:00:00Z","amount":30}`)
	fold("budget-topic", "b1", `{"id":"b1","month":"2024-03-01 00:00:00+00:00","budget":100}`)
	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut("budgets", []byte("b1"))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"left":70}`, got)
}

// A fan without elementKey identifies elements by its key — computed keys
// included.
func TestFanIdentityUsesKeyExpression(t *testing.T) {
	stages := []Stage{{
		Name: "lines", From: "orders", ForEach: "$.items[*]", KeyPath: []string{"upper($.sku)"},
		Emit: []Emit{{Field: "sku", Expr: "upper($.sku)"}, {Field: "order", From: "$parent.id"}},
	}}
	cfg := stageConfig(stages...)
	require.NoError(t, validateProjectionStageShapes(cfg))
	g := buildStageGraph(cfg.Stages)
	store := stageStoreForTest(t)
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicUpsertNow(tx, "orders", []byte("o1"), decodePayload(t, `{"id":"o1","items":[{"sku":"ab"},{"sku":"cd"}]}`))
	}))
	var got string
	require.NoError(t, store.View(func(tx *stagestore.Tx) error {
		v, err := tx.GetOut("lines", []byte("CD"))
		got = string(v)
		return err
	}))
	require.Equal(t, `{"order":"o1","sku":"CD"}`, got)
}
