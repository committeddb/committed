package stages

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// userPermsStages is the fan ticket's acceptance model: memberships ×
// group permissions → one (user, permission) row, counting the groups
// that grant it.
func userPermsStages() []Stage {
	return []Stage{
		{
			Name: "group-perms", From: "grants", KeyPath: []string{"$.group", "$.perm"},
			Emit: []Emit{{Field: "group", From: "$.group"}, {Field: "permission", From: "$.perm"}},
		},
		{
			Name: "user-perms", From: "memberships", KeyPath: []string{"$.user", "$.p.permission"}, Reduce: "aggregate",
			Joins: []Join{{From: "group-perms", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true}},
			Emit: []Emit{
				{Field: "n", Expr: "count(*)"},
				{Field: "groups", Expr: "string_agg($.group, ',' order by $.group)"},
			},
		},
	}
}

func TestFanJoinUserPermissions(t *testing.T) {
	h := newUnionHarness(t, userPermsStages()...)
	up := func(user, perm string) string { return h.out("user-perms", OutKey([]string{user, perm})) }

	h.upsert("memberships", "m1", `{"user":"u1","group":"g1"}`)
	h.upsert("memberships", "m2", `{"user":"u2","group":"g1"}`)
	h.upsert("memberships", "m3", `{"user":"u1","group":"g2"}`)
	require.Empty(t, up("u1", "read"), "no grants yet: an inner fan has no pairs")

	h.upsert("grants", "x1", `{"group":"g1","perm":"read"}`)
	require.Equal(t, `{"groups":"g1","n":1}`, up("u1", "read"), "a grant reaches every member")
	require.Equal(t, `{"groups":"g1","n":1}`, up("u2", "read"))

	h.upsert("grants", "x2", `{"group":"g2","perm":"read"}`)
	require.Equal(t, `{"groups":"g1,g2","n":2}`, up("u1", "read"), "two groups grant it: two pairs, one row")

	h.del("grants", "x1")
	require.Equal(t, `{"groups":"g2","n":1}`, up("u1", "read"), "a revoke retracts its pairs only")
	require.Empty(t, up("u2", "read"))

	h.upsert("grants", "x3", `{"group":"g1","perm":"write"}`)
	h.del("memberships", "m2")
	require.Empty(t, up("u2", "write"), "a member leaving takes its pairs")
	require.Equal(t, `{"groups":"g1","n":1}`, up("u1", "write"))

	h.upsert("memberships", "m1", `{"user":"u1","group":"g2"}`) // u1 moves from g1 to g2
	require.Empty(t, up("u1", "write"), "the input's reference moved: its old pairs retract")
	require.Equal(t, `{"groups":"g2,g2","n":2}`, up("u1", "read"), "m1 and m3 both reach g2's read")
}

// An optional fan join over a TOPIC's field is SQL's LEFT JOIN: an input
// with no match is one pair with a null alias, standing in exactly while
// no row matches.
func TestFanJoinOptionalTopic(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "product-cats", From: "products", KeyPath: []string{"$.id", "coalesce($.cat.category, 'none')"},
		Joins: []Join{{Topic: "assignments", Field: "$.product", On: []string{"$.id"}, As: "cat", Optional: true, Fan: true}},
		Emit:  []Emit{{Field: "category", From: "$.cat.category"}, {Field: "name", From: "$.name"}},
	})
	row := func(product, cat string) string { return h.out("product-cats", OutKey([]string{product, cat})) }

	h.upsert("products", "p1", `{"id":"p1","name":"kite"}`)
	require.Equal(t, `{"category":null,"name":"kite"}`, row("p1", "none"))

	h.upsert("assignments", "a1", `{"product":"p1","category":"toys"}`)
	require.Equal(t, `{"category":"toys","name":"kite"}`, row("p1", "toys"))
	require.Empty(t, row("p1", "none"), "the null pair stands in only while nothing matches")

	h.upsert("assignments", "a2", `{"product":"p1","category":"outdoor"}`)
	require.NotEmpty(t, row("p1", "outdoor"))

	h.upsert("products", "p1", `{"id":"p1","name":"big kite"}`)
	require.Equal(t, `{"category":"toys","name":"big kite"}`, row("p1", "toys"), "an input change re-fans its pairs")

	h.del("assignments", "a1")
	h.upsert("assignments", "a2", `{"product":"p2","category":"outdoor"}`) // moves to another product
	require.Empty(t, row("p1", "toys"))
	require.Empty(t, row("p1", "outdoor"))
	require.Equal(t, `{"category":null,"name":"big kite"}`, row("p1", "none"), "the last match left: the null pair returns")

	h.upsert("products", "p2", `{"id":"p2","name":"ball"}`)
	require.Equal(t, `{"category":"outdoor","name":"ball"}`, row("p2", "outdoor"), "an input arriving after its matches")

	fc := h.g.FlowCounts()["product-cats"]
	require.Positive(t, fc.Fanned)
	require.EqualValues(t, 1, fc.Joins[0].Misses, "p1's first fan found nothing")
}

// The join's where filters the matched rows, and the stage's when sees
// the pair — the matched row included.
func TestFanJoinWhereAndWhen(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "active-perms", From: "memberships", KeyPath: []string{"$.user", "$.p.perm"},
		Joins: []Join{{
			Topic: "grants", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true,
			Where: []WhenClause{{Path: "$.revoked", Null: true}},
		}},
		When: []WhenClause{{Expr: "$.p.perm <> 'admin'"}},
		Emit: []Emit{{Field: "via", From: "$.group"}},
	})
	h.upsert("memberships", "m1", `{"user":"u1","group":"g1"}`)
	h.upsert("grants", "x1", `{"group":"g1","perm":"read","revoked":null}`)
	h.upsert("grants", "x2", `{"group":"g1","perm":"admin","revoked":null}`)
	h.upsert("grants", "x3", `{"group":"g1","perm":"write","revoked":"2026-01-01"}`)
	require.Equal(t, `{"via":"g1"}`, h.out("active-perms", OutKey([]string{"u1", "read"})))
	require.Empty(t, h.out("active-perms", OutKey([]string{"u1", "admin"})), "the when filtered the pair")
	require.Empty(t, h.out("active-perms", OutKey([]string{"u1", "write"})), "the join's where filtered the row")

	h.upsert("grants", "x3", `{"group":"g1","perm":"write","revoked":null}`)
	require.NotEmpty(t, h.out("active-perms", OutKey([]string{"u1", "write"})), "a row starting to match adds its pair")
	h.upsert("grants", "x1", `{"group":"g1","perm":"read","revoked":"2026-02-01"}`)
	require.Empty(t, h.out("active-perms", OutKey([]string{"u1", "read"})), "a row stopping to match retracts its pair")
}

// A dimension change touches one pair per dependent input, not every pair
// of every dependent: the cost of a grant is the group's member count.
func TestFanJoinDimensionChangeCost(t *testing.T) {
	h := newUnionHarness(t, userPermsStages()...)
	for i := range 40 {
		h.upsert("grants", fmt.Sprintf("x%d", i), fmt.Sprintf(`{"group":"g1","perm":"p%d"}`, i))
	}
	for i := range 3 {
		h.upsert("memberships", fmt.Sprintf("m%d", i), fmt.Sprintf(`{"user":"u%d","group":"g1"}`, i))
	}
	before := h.g.FlowCounts()["user-perms"].Fanned
	h.upsert("grants", "new", `{"group":"g1","perm":"extra"}`)
	require.EqualValues(t, 3, h.g.FlowCounts()["user-perms"].Fanned-before, "one pair per member")
	require.Equal(t, `{"groups":"g1","n":1}`, h.out("user-perms", OutKey([]string{"u2", "extra"})))
}

// Refresh markers: a topic fan join's stale rows and a fan stage's stale
// inputs each retract with their pairs.
func TestFanJoinEpochSweep(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "pairs", From: "memberships", KeyPath: []string{"$.user", "$.p.perm"},
		Joins: []Join{{Topic: "grants", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true}},
		Emit:  []Emit{{Field: "g", From: "$.group"}},
	})
	key := func(u, p string) string { return OutKey([]string{u, p}) }
	h.fold("memberships", "m1", `{"user":"u1","group":"g1"}`, 1)
	h.fold("memberships", "m2", `{"user":"u2","group":"g1"}`, 1)
	h.fold("grants", "x1", `{"group":"g1","perm":"read"}`, 1)
	h.fold("grants", "x2", `{"group":"g1","perm":"write"}`, 2)
	h.sweep("grants", 2)
	require.Empty(t, h.out("pairs", key("u1", "read")), "x1 was not re-asserted")
	require.NotEmpty(t, h.out("pairs", key("u1", "write")))

	h.fold("memberships", "m1", `{"user":"u1","group":"g1"}`, 2)
	h.sweep("memberships", 2)
	require.NotEmpty(t, h.out("pairs", key("u1", "write")))
	require.Empty(t, h.out("pairs", key("u2", "write")), "m2 was not re-asserted: its pairs retract")

	h.fold("grants", "x3", `{"group":"g1","perm":"admin"}`, 2)
	require.Empty(t, h.out("pairs", key("u2", "admin")), "a swept input is never re-fanned")
}

// A fan join over a union stage's inputs: identities are (source, key)
// pairs, and source() reads in the pair.
func TestFanJoinOverUnion(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "pairs", FromList: []string{"members-a", "members-b"}, KeyPath: []string{"$.user", "$.p.perm", "source()"},
		Joins: []Join{{Topic: "grants", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true}},
		Emit:  []Emit{{Field: "src", Expr: "source()"}},
	})
	h.upsert("grants", "x1", `{"group":"g1","perm":"read"}`)
	h.upsert("members-a", "m1", `{"user":"u1","group":"g1"}`)
	h.upsert("members-b", "m1", `{"user":"u1","group":"g1"}`)
	require.Equal(t, `{"src":"members-a"}`, h.out("pairs", OutKey([]string{"u1", "read", "members-a"})))
	require.Equal(t, `{"src":"members-b"}`, h.out("pairs", OutKey([]string{"u1", "read", "members-b"})))
	h.del("members-a", "m1")
	require.Empty(t, h.out("pairs", OutKey([]string{"u1", "read", "members-a"})))
	h.upsert("grants", "x1", `{"group":"g1","perm":"write"}`)
	require.Equal(t, `{"src":"members-b"}`, h.out("pairs", OutKey([]string{"u1", "write", "members-b"})))
	require.Empty(t, h.out("pairs", OutKey([]string{"u1", "read", "members-b"})))
}

func TestFanJoinAdmission(t *testing.T) {
	base := func(mut func(*Stage)) error {
		st := Stage{
			Name: "s", From: "memberships", KeyPath: []string{"$.user", "$.p.perm"},
			Joins: []Join{{Topic: "grants", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true}},
			Emit:  []Emit{{Field: "g", From: "$.group"}},
		}
		mut(&st)
		return validateProjectionStageShapes(stageConfig(st))
	}
	require.NoError(t, base(func(*Stage) {}))
	require.ErrorContains(t, base(func(s *Stage) { s.Joins[0].As = "" }), "name them with as")
	require.ErrorContains(t, base(func(s *Stage) { s.Joins[0].Field = "" }), "fan needs field")
	require.ErrorContains(t, base(func(s *Stage) { s.Joins[0].Absent = true }), "anti-join has no row to fan")
	require.ErrorContains(t, base(func(s *Stage) { s.ForEach = "$.items[*]"; s.ElementKey = "$.id" }), "one fan per fold unit")
	require.ErrorContains(t, base(func(s *Stage) {
		s.Reduce, s.DeleteWhen = "liveSet", []WhenClause{{Path: "$.gone", NotNull: true}}
	}), "fan on a liveSet")
	require.ErrorContains(t, base(func(s *Stage) {
		s.Joins = append(s.Joins, Join{Topic: "roles", Field: "$.group", On: []string{"$.group"}, As: "r", Fan: true})
	}), "a second fan join")
	require.ErrorContains(t, base(func(s *Stage) { s.Joins[0].Fan = false }), "field addresses a STAGE's emitted field",
		"a topic field join still needs fan")
	// A non-fan join's alias stays refold-only; the fan alias is fold-time.
	require.ErrorContains(t, base(func(s *Stage) {
		s.Joins = append(s.Joins, Join{Topic: "users", On: []string{"$.user"}, As: "u", Optional: true})
		s.KeyPath = []string{"$.u.name"}
	}), "addresses join alias")

	err := validateProjectionStageShapes(stageConfig(Stage{
		Name: "w", From: "memberships", Reduce: "window", WindowOrderBy: []string{"$.at"},
		Joins: []Join{{Topic: "grants", Field: "$.group", On: []string{"$.group"}, As: "p", Fan: true}},
		Emit:  []Emit{{Field: "n", Expr: "row_number()"}},
	}))
	require.ErrorContains(t, err, "fan on a window stage")
}
