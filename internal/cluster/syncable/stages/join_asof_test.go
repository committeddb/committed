package stages

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

func pricedStage(mut func(*Join)) Stage {
	j := Join{
		Topic: "price-changes", Field: "$.product", On: []string{"$.product"},
		OrderBy: []string{"$.from::timestamptz"}, TieBy: []string{"$.change"},
		AsOf: "$.at::timestamptz", As: "price", Optional: true,
	}
	if mut != nil {
		mut(&j)
	}
	return Stage{
		Name: "priced", From: "orders", KeyPath: []string{"$.id"},
		Joins: []Join{j},
		Emit:  []Emit{{Field: "price", From: "$.price.amount"}},
	}
}

func (h *unionHarness) reresolved(stage string) int64 {
	return h.g.FlowCounts()[stage].Joins[0].Reresolved
}

// The order-pricing acceptance model: each order prices at the version in
// effect at its own time, and every version change re-resolves exactly the
// orders in its range.
func TestAsOfJoinPricing(t *testing.T) {
	h := newUnionHarness(t, pricedStage(nil))
	price := func(order string) string { return h.out("priced", order) }
	version := func(change, from string, amount int) {
		h.upsert("price-changes", change, fmt.Sprintf(`{"product":"p1","change":%q,"from":%q,"amount":%d}`, change, from, amount))
	}
	version("c1", "2026-01-01T00:00:00Z", 10)
	version("c2", "2026-02-01T00:00:00Z", 12)
	h.upsert("orders", "o1", `{"id":"o1","product":"p1","at":"2026-01-15T00:00:00Z"}`)
	h.upsert("orders", "o2", `{"id":"o2","product":"p1","at":"2026-02-15T00:00:00Z"}`)
	h.upsert("orders", "o3", `{"id":"o3","product":"p1","at":"2025-12-15T00:00:00Z"}`)
	h.upsert("orders", "o4", `{"id":"o4","product":"p1","at":"2026-02-01T00:00:00Z"}`)
	require.Equal(t, `{"price":10}`, price("o1"))
	require.Equal(t, `{"price":12}`, price("o2"))
	require.Equal(t, `{"price":null}`, price("o3"), "before any version: optional scopes null")
	require.Equal(t, `{"price":12}`, price("o4"), "the bound is inclusive")

	before := h.reresolved("priced")
	version("c0", "2025-12-01T00:00:00Z", 9)
	require.Equal(t, `{"price":9}`, price("o3"), "a backfilled older version")
	require.EqualValues(t, 1, h.reresolved("priced")-before, "re-resolves only its range [Dec 1, Jan 1)")

	before = h.reresolved("priced")
	version("c1b", "2026-01-01T00:00:00Z", 11)
	require.Equal(t, `{"price":11}`, price("o1"), "a correction at the same time: the higher tieBy wins")
	require.EqualValues(t, 1, h.reresolved("priced")-before, "only o1 is in [Jan 1, Feb 1)")

	h.del("price-changes", "c2")
	require.Equal(t, `{"price":11}`, price("o2"), "a deleted version's range falls to its predecessor")
	require.Equal(t, `{"price":11}`, price("o4"))

	version("c1b", "2026-01-20T00:00:00Z", 11) // the correction moves later
	require.Equal(t, `{"price":10}`, price("o1"), "moved past o1: o1 falls back to c1")
	require.Equal(t, `{"price":11}`, price("o2"))

	h.upsert("orders", "o1", `{"id":"o1","product":"p1","at":"2026-03-01T00:00:00Z"}`)
	require.Equal(t, `{"price":11}`, price("o1"), "an order's own time changes")

	h.upsert("orders", "o5", `{"id":"o5","product":"p1","at":null}`)
	require.Equal(t, `{"price":null}`, price("o5"), "a null bound is never eligible")
}

// Inner, absent, and where compose with asOf: an inner join drops inputs
// with no version in effect, an anti-join keeps exactly those, and the
// join's where filters the candidate versions before ordering.
func TestAsOfJoinModes(t *testing.T) {
	inner := pricedStage(func(j *Join) { j.Optional = false })
	inner.Name = "inner"
	absent := pricedStage(func(j *Join) { j.Optional, j.As, j.Absent = false, "", true })
	absent.Name, absent.Emit = "unpriced", []Emit{{Field: "id", From: "$.id"}}
	filtered := pricedStage(func(j *Join) { j.Where = []WhenClause{{Path: "$.approved", Equals: true}} })
	filtered.Name = "approved"
	h := newUnionHarness(t, inner, absent, filtered)

	h.upsert("price-changes", "c1", `{"product":"p1","change":"c1","from":"2026-01-01T00:00:00Z","amount":10,"approved":true}`)
	h.upsert("price-changes", "c2", `{"product":"p1","change":"c2","from":"2026-02-01T00:00:00Z","amount":12,"approved":false}`)
	h.upsert("orders", "early", `{"id":"early","product":"p1","at":"2025-06-01T00:00:00Z"}`)
	h.upsert("orders", "late", `{"id":"late","product":"p1","at":"2026-03-01T00:00:00Z"}`)

	require.Empty(t, h.out("inner", "early"))
	require.Equal(t, `{"price":12}`, h.out("inner", "late"))
	require.Equal(t, `{"id":"early"}`, h.out("unpriced", "early"))
	require.Empty(t, h.out("unpriced", "late"))
	require.Equal(t, `{"price":10}`, h.out("approved", "late"), "the unapproved later version is skipped")

	h.upsert("price-changes", "c2", `{"product":"p1","change":"c2","from":"2026-02-01T00:00:00Z","amount":12,"approved":true}`)
	require.Equal(t, `{"price":12}`, h.out("approved", "late"), "a version starting to pass where takes its range")
	h.upsert("price-changes", "c0", `{"product":"p1","change":"c0","from":"2025-01-01T00:00:00Z","amount":5,"approved":true}`)
	require.Empty(t, h.out("unpriced", "early"), "a backfilled version puts early in effect")
	require.Equal(t, `{"price":5}`, h.out("inner", "early"))
}

// A version's where-status matters past the next version: inputs beyond a
// version failing where resolve to the one before it.
func TestAsOfJoinRangeSkipsFilteredVersions(t *testing.T) {
	h := newUnionHarness(t, pricedStage(func(j *Join) { j.Where = []WhenClause{{Path: "$.approved", Equals: true}} }))
	h.upsert("price-changes", "c1", `{"product":"p1","change":"c1","from":"2026-01-01T00:00:00Z","amount":10,"approved":true}`)
	h.upsert("price-changes", "c2", `{"product":"p1","change":"c2","from":"2026-02-01T00:00:00Z","amount":12,"approved":false}`)
	h.upsert("orders", "late", `{"id":"late","product":"p1","at":"2026-03-01T00:00:00Z"}`)
	require.Equal(t, `{"price":10}`, h.out("priced", "late"))
	h.upsert("price-changes", "c1", `{"product":"p1","change":"c1","from":"2026-01-01T00:00:00Z","amount":11,"approved":true}`)
	require.Equal(t, `{"price":11}`, h.out("priced", "late"), "c1's range runs past the filtered c2")
}

// Without asOf, an ordered join is "the latest by business order": the
// current assignee by assignment time, not by key bytes.
func TestOrderedJoinLatestByBusinessOrder(t *testing.T) {
	h := newUnionHarness(t, Stage{
		Name: "tickets", From: "ticket-events", KeyPath: []string{"$.id"},
		Joins: []Join{{
			Topic: "assignments", Field: "$.ticket", On: []string{"$.id"},
			OrderBy: []string{"$.at::timestamptz"}, TieBy: []string{"$.seq"}, As: "a", Optional: true,
		}},
		Emit: []Emit{{Field: "assignee", From: "$.a.user"}},
	})
	h.upsert("ticket-events", "t1", `{"id":"t1"}`)
	h.upsert("assignments", "zz", `{"ticket":"t1","user":"ada","at":"2026-01-02T00:00:00Z","seq":1}`)
	h.upsert("assignments", "aa", `{"ticket":"t1","user":"bea","at":"2026-01-03T00:00:00Z","seq":2}`)
	require.Equal(t, `{"assignee":"bea"}`, h.out("tickets", "t1"), "the later assignment, though its key sorts first")
	h.del("assignments", "aa")
	require.Equal(t, `{"assignee":"ada"}`, h.out("tickets", "t1"))
}

// Versions produced by a prior stage (field on a stage join), and a refresh
// marker sweeping a topic's stale versions.
func TestAsOfJoinStageVersionsAndSweep(t *testing.T) {
	h := newUnionHarness(t,
		Stage{
			Name: "rates", From: "fx-raw", KeyPath: []string{"$.id"},
			Emit: []Emit{{Field: "currency", From: "$.ccy"}, {Field: "day", From: "$.day"}, {Field: "rate", From: "$.rate"}, {Field: "id", From: "$.id"}},
		},
		Stage{
			Name: "usd", From: "payments", KeyPath: []string{"$.id"},
			Joins: []Join{{
				From: "rates", Field: "$.currency", On: []string{"$.currency"},
				OrderBy: []string{"$.day::date"}, TieBy: []string{"$.id"}, AsOf: "$.paid::date", As: "fx",
			}},
			Emit: []Emit{{Field: "usd", Expr: "round($.amount * $.fx.rate, 2)"}},
		},
	)
	h.upsert("fx-raw", "r1", `{"id":"r1","ccy":"EUR","day":"2026-01-01","rate":1.1}`)
	h.upsert("payments", "x", `{"id":"x","currency":"EUR","amount":100,"paid":"2026-01-05"}`)
	require.Equal(t, `{"usd":110}`, h.out("usd", "x"))
	h.upsert("fx-raw", "r2", `{"id":"r2","ccy":"EUR","day":"2026-01-04","rate":1.2}`)
	require.Equal(t, `{"usd":120}`, h.out("usd", "x"))
	h.del("fx-raw", "r2")
	require.Equal(t, `{"usd":110}`, h.out("usd", "x"))

	s := newUnionHarness(t, pricedStage(nil))
	s.fold("price-changes", "c1", `{"product":"p1","change":"c1","from":"2026-01-01T00:00:00Z","amount":10}`, 1)
	s.fold("price-changes", "c2", `{"product":"p1","change":"c2","from":"2026-01-10T00:00:00Z","amount":12}`, 2)
	s.upsert("orders", "o1", `{"id":"o1","product":"p1","at":"2026-01-15T00:00:00Z"}`)
	s.upsert("orders", "o2", `{"id":"o2","product":"p1","at":"2026-01-05T00:00:00Z"}`)
	require.Equal(t, `{"price":10}`, s.out("priced", "o2"))
	s.sweep("price-changes", 2)
	require.Equal(t, `{"price":null}`, s.out("priced", "o2"), "c1 was not re-asserted")
	require.Equal(t, `{"price":12}`, s.out("priced", "o1"))
}

// Comparing a bound and versions of different kinds is a data error, not a
// silent cross-family order.
func TestAsOfJoinFamilyMismatchIsADataError(t *testing.T) {
	st := pricedStage(func(j *Join) { j.OrderBy, j.AsOf = []string{"$.from"}, "$.at" })
	h := newUnionHarness(t, st)
	h.upsert("price-changes", "c1", `{"product":"p1","change":"c1","from":5,"amount":10}`)
	err := h.store.Update(func(tx *stagestore.Tx) error {
		return h.g.FoldTopicUpsertNow(tx, "orders", []byte("o1"), decodePayload(t, `{"id":"o1","product":"p1","at":"2026-01-01"}`))
	})
	require.ErrorContains(t, err, "different kinds")
	require.ErrorIs(t, err, cluster.ErrPermanent, "a data error dead-letters, it does not wedge")
}

func TestOrderedJoinAdmission(t *testing.T) {
	adm := func(mut func(*Join)) error { return validateProjectionStageShapes(stageConfig(pricedStage(mut))) }
	require.NoError(t, adm(nil))
	require.ErrorContains(t, adm(func(j *Join) { j.TieBy = nil }), "needs tieBy")
	require.ErrorContains(t, adm(func(j *Join) { j.Field = "" }), "declare field")
	require.ErrorContains(t, adm(func(j *Join) { j.OrderBy = nil }), "declare orderBy")
	require.ErrorContains(t, adm(func(j *Join) { j.OrderBy, j.AsOf = nil, "" }), "declare orderBy")
	require.ErrorContains(t, adm(func(j *Join) { j.OrderBy = []string{"$.from::timestamptz", "$.x"} }), "ONE orderBy key")
	require.ErrorContains(t, adm(func(j *Join) { j.OrderBy = []string{"$.from::timestamptz desc"} }), "drop desc")
	require.ErrorContains(t, adm(func(j *Join) { j.OrderBy = []string{"$.from::timestamptz nulls first"} }), "drop nulls first")
	require.ErrorContains(t, adm(func(j *Join) { j.AsOf = "$.at::numeric" }), "asOf is a number but orderBy is a date/time")
	require.ErrorContains(t, adm(func(j *Join) { j.Fan = true }), "drop one")
	require.ErrorContains(t, adm(func(j *Join) { j.AsOf = "$.price.at" }), `addresses join alias "price"`)
	require.ErrorContains(t, adm(func(j *Join) { j.AsOf = "source()" }), "not supported there")
	require.NoError(t, adm(func(j *Join) { j.AsOf = "" }), "orderBy alone: the latest version")
}

// Randomized against a brute-force resolver: version inserts, moves,
// corrections, deletes, and where flips, and order changes, with ties on
// orderBy (and on tieBy, falling to the producer key) — after every step
// every order resolves as SQL's LATERAL … WHERE t <= at ORDER BY t DESC,
// change DESC, key DESC LIMIT 1 would.
func TestAsOfJoinRandomizedAgainstBruteForce(t *testing.T) {
	type version struct {
		product        string
		t              *int
		change         string
		amount         int
		approved, live bool
	}
	type order struct {
		product string
		at      *int
		live    bool
	}
	for seed := uint64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		h := newUnionHarness(t, pricedStage(func(j *Join) {
			j.OrderBy, j.TieBy, j.AsOf = []string{"$.t::numeric"}, []string{"$.change"}, "$.at::numeric"
			j.Where = []WhenClause{{Path: "$.approved", Equals: true}}
		}))
		versions := map[string]*version{}
		orders := map[string]*order{}
		intp := func(max int) *int {
			if rng.IntN(10) == 0 {
				return nil
			}
			v := rng.IntN(max)
			return &v
		}
		js := func(p *int) string {
			if p == nil {
				return "null"
			}
			return fmt.Sprint(*p)
		}
		for step := range 120 {
			products := []string{"p1", "p2"}
			if rng.IntN(3) > 0 {
				key := fmt.Sprintf("v%d", rng.IntN(12))
				if v, ok := versions[key]; ok && v.live && rng.IntN(4) == 0 {
					v.live = false
					h.del("price-changes", key)
				} else {
					v := &version{product: products[rng.IntN(2)], t: intp(20), change: fmt.Sprint(rng.IntN(3)), amount: step, approved: rng.IntN(4) > 0, live: true}
					versions[key] = v
					h.upsert("price-changes", key, fmt.Sprintf(`{"product":%q,"t":%s,"change":%q,"amount":%d,"approved":%v}`,
						v.product, js(v.t), v.change, v.amount, v.approved))
				}
			} else {
				key := fmt.Sprintf("o%d", rng.IntN(8))
				o := &order{product: products[rng.IntN(2)], at: intp(22), live: true}
				orders[key] = o
				h.upsert("orders", key, fmt.Sprintf(`{"id":%q,"product":%q,"at":%s}`, key, o.product, js(o.at)))
			}
			for ok, o := range orders {
				var best *version
				bestKey := ""
				for vk, v := range versions {
					if !v.live || !v.approved || v.product != o.product || v.t == nil || o.at == nil || *v.t > *o.at {
						continue
					}
					if best == nil || *v.t > *best.t || (*v.t == *best.t && (v.change > best.change || (v.change == best.change && vk > bestKey))) {
						best, bestKey = v, vk
					}
				}
				want := `{"price":null}`
				if best != nil {
					want = fmt.Sprintf(`{"price":%d}`, best.amount)
				}
				require.Equal(t, want, h.out("priced", ok), "seed %d step %d order %s", seed, step, ok)
			}
		}
	}
}
