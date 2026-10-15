//go:build docker || integration

package dialects_test

import (
	"testing"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// Fan joins against their oracle: each acceptance read model runs as a
// projection while the same writes land in raw Postgres tables, and after
// every step the projection must equal PostgreSQL's own join over them.

// 1. User permissions: memberships × group grants (a many-to-many join on
// group), one row per (user, permission) with the granting groups.
func TestPostgreSQLIntegration_ProjFanJoinUserPermissions(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"user_id", "permission"},
		Columns: []sql.ProjectionColumn{
			{Name: "user_id", SQLType: "VARCHAR(32)"},
			{Name: "permission", SQLType: "VARCHAR(32)"},
			{Name: "n", SQLType: "INT"},
			{Name: "groups", SQLType: "TEXT"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "group-perms", From: "grants", KeyPath: []string{"$.id"},
				Emit: []sql.StageEmit{{Field: "group_id", From: "$.group_id"}, {Field: "permission", From: "$.permission"}},
			},
			{
				Name: "user-perms", From: "memberships", KeyPath: []string{"$.user_id", "$.perm.permission"}, Reduce: "aggregate",
				Joins: []sql.StageJoin{{From: "group-perms", Field: "$.group_id", On: []string{"$.group_id"}, As: "perm", Fan: true}},
				Emit: []sql.StageEmit{
					{Field: "n", Expr: "count(*)"},
					{Field: "groups", Expr: "string_agg($.group_id, ',' order by $.group_id)"},
				},
			},
		},
		Sources: []sql.ProjectionSource{{FromStage: "user-perms", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "n", From: "$.n"}, {Column: "groups", From: "$.groups"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"memberships", "grants"},
		`id TEXT PRIMARY KEY, user_id TEXT, group_id TEXT, permission TEXT`, []string{"id", "user_id", "group_id", "permission"}, nil)
	got := `SELECT user_id, permission, n::text, groups FROM %q ORDER BY user_id, permission`
	want := `SELECT m.user_id, g.permission, count(*)::text, string_agg(m.group_id, ',' ORDER BY m.group_id)
		FROM {memberships} m JOIN {grants} g ON g.group_id = m.group_id
		GROUP BY m.user_id, g.permission ORDER BY 1, 2`

	member := func(id, user, group string) map[string]any {
		return map[string]any{"id": id, "user_id": user, "group_id": group}
	}
	grant := func(id, group, perm string) map[string]any {
		return map[string]any{"id": id, "group_id": group, "permission": perm}
	}
	o.upsert("memberships", member("m1", "u1", "g1"))
	o.upsert("memberships", member("m2", "u2", "g1"))
	o.upsert("memberships", member("m3", "u1", "g2"))
	o.upsert("grants", grant("x1", "g1", "read"))
	o.assertMatches("a grant reaches every member of the group", got, want)

	o.upsert("grants", grant("x2", "g2", "read"))
	o.upsert("grants", grant("x3", "g1", "write"))
	o.assertMatches("a permission granted by two groups", got, want)

	o.del("grants", "x1")
	o.assertMatches("a revoke", got, want)

	o.upsert("memberships", member("m4", "u3", "g2"))
	o.assertMatches("a user joins a group", got, want)

	o.del("memberships", "m4")
	o.upsert("memberships", member("m1", "u1", "g2"))
	o.assertMatches("a user leaves a group, another moves", got, want)

	o.upsert("grants", grant("x3", "g2", "write"))
	o.assertMatches("a grant moves to another group", got, want)
}

// 2. Optional fan: products × category assignments as LEFT JOIN rows; an
// uncategorized product keeps one row with a null category.
func TestPostgreSQLIntegration_ProjFanJoinOptional(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"product_id", "category_key"},
		Columns: []sql.ProjectionColumn{
			{Name: "product_id", SQLType: "VARCHAR(32)"},
			{Name: "category_key", SQLType: "VARCHAR(32)"},
			{Name: "name", SQLType: "VARCHAR(32)"},
			{Name: "category", SQLType: "VARCHAR(32)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "product-cats", From: "products", KeyPath: []string{"$.id", "coalesce($.cat.category, 'none')"},
			Joins: []sql.StageJoin{{Topic: "assignments", Field: "$.product_id", On: []string{"$.id"}, As: "cat", Optional: true, Fan: true}},
			Emit:  []sql.StageEmit{{Field: "name", From: "$.name"}, {Field: "category", From: "$.cat.category"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "product-cats", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "name", From: "$.name"}, {Column: "category", From: "$.category"},
		}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"products", "assignments"},
		`id TEXT PRIMARY KEY, name TEXT, product_id TEXT, category TEXT`, []string{"id", "name", "product_id", "category"}, nil)
	got := `SELECT product_id, category_key, name, category FROM %q ORDER BY product_id, category_key`
	want := `SELECT p.id, coalesce(a.category, 'none'), p.name, a.category
		FROM {products} p LEFT JOIN {assignments} a ON a.product_id = p.id
		ORDER BY 1, 2`

	product := func(id, name string) map[string]any { return map[string]any{"id": id, "name": name} }
	assign := func(id, product, cat string) map[string]any {
		return map[string]any{"id": id, "product_id": product, "category": cat}
	}
	o.upsert("products", product("p1", "kite"))
	o.upsert("products", product("p2", "ball"))
	o.assertMatches("uncategorized products keep a null row", got, want)

	o.upsert("assignments", assign("a1", "p1", "toys"))
	o.upsert("assignments", assign("a2", "p1", "outdoor"))
	o.upsert("assignments", assign("a3", "p3", "games")) // a product that has not arrived
	o.assertMatches("assignments arrive", got, want)

	o.upsert("products", product("p3", "dice"))
	o.upsert("products", product("p1", "big kite"))
	o.assertMatches("a product arrives after its assignment; a rename", got, want)

	o.del("assignments", "a1")
	o.upsert("assignments", assign("a2", "p2", "outdoor"))
	o.assertMatches("p1 loses its last category", got, want)

	o.del("products", "p2")
	o.assertMatches("a product delete", got, want)
}
