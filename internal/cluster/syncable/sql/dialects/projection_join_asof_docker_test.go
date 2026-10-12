//go:build docker || integration

package dialects_test

import (
	"testing"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// Ordered and as-of joins against their oracle: PostgreSQL has no ASOF
// JOIN, so each acceptance read model is checked against the LATERAL
// equivalent — (… WHERE v.key = i.key AND v.t <= i.t ORDER BY v.t DESC,
// tie DESC, id DESC LIMIT 1) — over raw tables after every step.

// 1. Order pricing: each order at the price in effect at its own time,
// through a backfilled version, a same-time correction, a version delete,
// an order whose time changes, and an order before any version.
func TestPostgreSQLIntegration_ProjAsOfOrderPricing(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"id"},
		Columns:    []sql.ProjectionColumn{{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "price", SQLType: "NUMERIC(10,2)"}},
		Stages: []sql.ProjectionStage{{
			Name: "priced", From: "orders", KeyPath: []string{"$.id"},
			Joins: []sql.StageJoin{{
				Topic: "price-changes", Field: "$.product_id", On: []string{"$.product_id"},
				OrderBy: []string{"$.effective_from::timestamptz"}, TieBy: []string{"$.change_id"},
				AsOf: "$.ordered_at::timestamptz", As: "price", Optional: true,
			}},
			Emit: []sql.StageEmit{{Field: "price", From: "$.price.price"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "priced", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "price", From: "$.price"}}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"orders", "price-changes"},
		`id TEXT PRIMARY KEY, product_id TEXT, ordered_at TIMESTAMPTZ, effective_from TIMESTAMPTZ, change_id INT, price NUMERIC`,
		[]string{"id", "product_id", "ordered_at", "effective_from", "change_id", "price"},
		map[string]string{"ordered_at": "::timestamptz", "effective_from": "::timestamptz", "price": "::numeric"})
	got := `SELECT id, price::numeric(10,2)::text FROM %q ORDER BY id`
	want := `SELECT o.id, p.price::numeric(10,2)::text FROM {orders} o LEFT JOIN LATERAL (
			SELECT * FROM {price-changes} p WHERE p.product_id = o.product_id AND p.effective_from <= o.ordered_at
			ORDER BY p.effective_from DESC, p.change_id DESC, p.id COLLATE "C" DESC LIMIT 1) p ON true
		ORDER BY o.id`

	version := func(id, product, from string, change int, price float64) map[string]any {
		return map[string]any{"id": id, "product_id": product, "effective_from": from, "change_id": change, "price": price}
	}
	ord := func(id, product, at string) map[string]any {
		return map[string]any{"id": id, "product_id": product, "ordered_at": at}
	}
	o.upsert("price-changes", version("c1", "p1", "2026-01-01T00:00:00Z", 1, 10))
	o.upsert("price-changes", version("c2", "p1", "2026-02-01T00:00:00Z", 2, 12.5))
	o.upsert("price-changes", version("c3", "p2", "2026-01-15T00:00:00Z", 3, 99))
	o.upsert("orders", ord("o1", "p1", "2026-01-15T00:00:00Z"))
	o.upsert("orders", ord("o2", "p1", "2026-02-01T00:00:00Z"))
	o.upsert("orders", ord("o3", "p1", "2025-12-15T00:00:00Z"))
	o.upsert("orders", ord("o4", "p2", "2026-03-01T00:00:00Z"))
	o.assertMatches("in-order versions; an order before any version is null", got, want)

	o.upsert("price-changes", version("c0", "p1", "2025-12-01T00:00:00Z", 0, 9))
	o.assertMatches("a backfilled older version", got, want)

	o.upsert("price-changes", version("c1b", "p1", "2026-01-01T00:00:00Z", 4, 11))
	o.assertMatches("a corrected version at the same time (higher tieBy wins)", got, want)

	o.del("price-changes", "c2")
	o.assertMatches("a version delete hands its range back", got, want)

	o.upsert("orders", ord("o1", "p1", "2025-12-20T00:00:00Z"))
	o.upsert("price-changes", version("c1b", "p1", "2026-01-20T00:00:00Z", 4, 11))
	o.assertMatches("an order's time changes; a version moves", got, want)
}

// 2. FX conversion: payments × daily rates per currency as of the payment
// date, an inner join (a payment before any rate has no row).
func TestPostgreSQLIntegration_ProjAsOfFXConversion(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"id"},
		Columns:    []sql.ProjectionColumn{{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "amount_usd", SQLType: "NUMERIC(18,2)"}},
		Stages: []sql.ProjectionStage{{
			Name: "usd", From: "payments", KeyPath: []string{"$.id"},
			Joins: []sql.StageJoin{{
				Topic: "fx-rates", Field: "$.currency", On: []string{"$.currency"},
				OrderBy: []string{"$.day::date"}, TieBy: []string{"$.id"}, AsOf: "$.paid_on::date", As: "fx",
			}},
			Emit: []sql.StageEmit{{Field: "amount_usd", Expr: "round($.amount * $.fx.rate, 2)"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "usd", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "amount_usd", From: "$.amount_usd"}}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"payments", "fx-rates"},
		`id TEXT PRIMARY KEY, currency TEXT, amount NUMERIC, paid_on DATE, day DATE, rate NUMERIC`,
		[]string{"id", "currency", "amount", "paid_on", "day", "rate"},
		map[string]string{"paid_on": "::date", "day": "::date", "amount": "::numeric", "rate": "::numeric"})
	got := `SELECT id, amount_usd::text FROM %q ORDER BY id`
	want := `SELECT p.id, round(p.amount * r.rate, 2)::text FROM {payments} p JOIN LATERAL (
			SELECT * FROM {fx-rates} r WHERE r.currency = p.currency AND r.day <= p.paid_on
			ORDER BY r.day DESC, r.id COLLATE "C" DESC LIMIT 1) r ON true
		ORDER BY p.id`

	rate := func(id, ccy, day string, r float64) map[string]any {
		return map[string]any{"id": id, "currency": ccy, "day": day, "rate": r}
	}
	pay := func(id, ccy string, amount float64, on string) map[string]any {
		return map[string]any{"id": id, "currency": ccy, "amount": amount, "paid_on": on}
	}
	o.upsert("fx-rates", rate("r1", "EUR", "2026-01-01", 1.1))
	o.upsert("fx-rates", rate("r2", "EUR", "2026-01-03", 1.125))
	o.upsert("fx-rates", rate("r3", "GBP", "2026-01-02", 1.27))
	o.upsert("payments", pay("x1", "EUR", 100, "2026-01-02"))
	o.upsert("payments", pay("x2", "EUR", 19.99, "2026-01-03"))
	o.upsert("payments", pay("x3", "GBP", 50, "2026-01-01")) // before GBP's first rate
	o.upsert("payments", pay("x4", "GBP", 50, "2026-01-05"))
	o.assertMatches("payments at the day's rate", got, want)

	o.upsert("fx-rates", rate("r0", "GBP", "2025-12-31", 1.25))
	o.upsert("fx-rates", rate("r2", "EUR", "2026-01-02", 1.125))
	o.assertMatches("a backfilled rate; a rate's day corrected", got, want)

	o.del("fx-rates", "r1")
	o.upsert("payments", pay("x1", "GBP", 100, "2026-01-02"))
	o.assertMatches("a rate delete; a payment changes currency", got, want)
}

// 3. Latest by business order (no asOf): each ticket's current assignee by
// assignment time, not by key bytes.
func TestPostgreSQLIntegration_ProjOrderedJoinCurrentAssignee(t *testing.T) {
	cfg := &sql.ProjectionConfig{
		Table:      uniqueTable(t),
		PrimaryKey: []string{"id"},
		Columns:    []sql.ProjectionColumn{{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "assignee", SQLType: "VARCHAR(32)"}},
		Stages: []sql.ProjectionStage{{
			Name: "current", From: "tickets", KeyPath: []string{"$.id"},
			Joins: []sql.StageJoin{{
				Topic: "assignments", Field: "$.ticket_id", On: []string{"$.id"},
				OrderBy: []string{"$.assigned_at::timestamptz"}, TieBy: []string{"$.seq"}, As: "a", Optional: true,
			}},
			Emit: []sql.StageEmit{{Field: "assignee", From: "$.a.assignee"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "current", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "assignee", From: "$.assignee"}}}}}},
	}
	o := newUnionOracle(t, cfg, []string{"tickets", "assignments"},
		`id TEXT PRIMARY KEY, ticket_id TEXT, assignee TEXT, assigned_at TIMESTAMPTZ, seq INT`,
		[]string{"id", "ticket_id", "assignee", "assigned_at", "seq"}, map[string]string{"assigned_at": "::timestamptz"})
	got := `SELECT id, assignee FROM %q ORDER BY id`
	want := `SELECT t.id, a.assignee FROM {tickets} t LEFT JOIN LATERAL (
			SELECT * FROM {assignments} a WHERE a.ticket_id = t.id
			ORDER BY a.assigned_at DESC, a.seq DESC, a.id COLLATE "C" DESC LIMIT 1) a ON true
		ORDER BY t.id`

	assign := func(id, ticket, who string, at any, seq int) map[string]any {
		return map[string]any{"id": id, "ticket_id": ticket, "assignee": who, "assigned_at": at, "seq": seq}
	}
	o.upsert("tickets", map[string]any{"id": "t1"})
	o.upsert("tickets", map[string]any{"id": "t2"})
	o.upsert("assignments", assign("zz", "t1", "ada", "2026-01-02T00:00:00Z", 1))
	o.upsert("assignments", assign("aa", "t1", "bea", "2026-01-03T00:00:00Z", 2))
	o.upsert("assignments", assign("mm", "t2", "cy", "2026-01-01T00:00:00Z", 3))
	o.assertMatches("the latest assignment by time, not by key", got, want)

	o.upsert("assignments", assign("bb", "t1", "di", "2026-01-03T00:00:00Z", 5))
	o.assertMatches("a tie on time, broken by seq", got, want)

	o.upsert("assignments", assign("nn", "t2", "ed", nil, 6))
	o.assertMatches("a null time is greatest, as DESC sorts nulls first", got, want)

	o.del("assignments", "bb")
	o.del("assignments", "nn")
	o.assertMatches("deletes hand the ticket back", got, want)
}
