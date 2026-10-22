package stages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// planned and unplanned count the graphs this package's tests built
// through their plan and as handed.
var planned, unplanned atomic.Int64

// TestMain routes every graph this package's tests build through its
// plan: BuildGraph runs the lowered stages, which must compile back to
// the same plan. Stages that do not admit (hand-built ones some tests
// feed BuildGraph directly) run as handed.
func TestMain(m *testing.M) {
	throughPlan = func(sts []Stage) []Stage {
		p, err := CompilePlan(sts)
		if err != nil {
			unplanned.Add(1)
			return sts
		}
		lowered, err := p.Stages()
		if err != nil {
			panic(fmt.Sprintf("plan does not lower: %v\nplan: %s", err, p.Encode()))
		}
		back, err := CompilePlan(lowered)
		if err != nil {
			panic(fmt.Sprintf("lowered stages do not compile: %v\nplan: %s", err, p.Encode()))
		}
		if !bytes.Equal(back.Encode(), p.Encode()) {
			panic(fmt.Sprintf("lowered stages compile to a different plan:\n  plan: %s\n  back: %s", p.Encode(), back.Encode()))
		}
		planned.Add(1)
		return lowered
	}
	code := m.Run()
	fmt.Fprintf(os.Stderr, "graphs built through their plan: %d, as handed: %d\n", planned.Load(), unplanned.Load())
	os.Exit(code)
}

func mustPlan(t *testing.T, sts []Stage) *Plan {
	t.Helper()
	p, err := CompilePlan(sts)
	if err != nil {
		t.Fatalf("compile plan: %v", err)
	}
	return p
}

// orders is a two-stage config the spelling tests vary: a latest stage
// joined by field from an aggregate stage that filters on two clauses.
func orders(edit func(latest, totals *Stage)) []Stage {
	sts := []Stage{
		{
			Name: "latest", From: "orders", KeyPath: []string{"$.id"}, Reduce: "latest",
			OrderBy: "$.at", TieBy: "$.seq",
			Emit: []Emit{{Field: "customer", From: "$.customer"}, {Field: "amount", Expr: "$.amount::int"}},
		},
		{
			Name: "totals", From: "customers", KeyPath: []string{"$.id"}, Reduce: "aggregate",
			When:  []WhenClause{{Path: "$.status", Equals: "active"}, {Path: "$.region", NotNull: true}},
			Joins: []Join{{From: "latest", On: []string{"$.id"}, Field: "$.customer", As: "o", Optional: true}},
			Emit:  []Emit{{Field: "orders", Expr: "count(*)"}, {Field: "biggest", Expr: "max($.o.amount) filter (where $.o.amount between 1 and 100)"}},
		},
	}
	if edit != nil {
		edit(&sts[0], &sts[1])
	}
	return sts
}

// TestPlan_SpellingsCompileToOnePlan pins the canonicalizations: what the
// declared stages leave implicit, or say two ways, the plan says once.
func TestPlan_SpellingsCompileToOnePlan(t *testing.T) {
	base := mustPlan(t, orders(nil)).Fingerprint()
	for name, edit := range map[string]func(latest, totals *Stage){
		"keyType text declared":           func(l, _ *Stage) { l.KeyType = []string{"text"} },
		"orderByType text declared":       func(l, _ *Stage) { l.OrderByType, l.TieByType = "text", "text" },
		"field join onType text declared": func(_, tot *Stage) { tot.Joins[0].OnType = []string{"text"} },
		"cast spelled out":                func(l, _ *Stage) { l.Emit[1].Expr = "CAST( $.amount AS INTEGER )" },
		"when clauses reordered":          func(_, tot *Stage) { tot.When[0], tot.When[1] = tot.When[1], tot.When[0] },
		"between spelled out": func(_, tot *Stage) {
			tot.Emit[1].Expr = "MAX($.o.amount) FILTER (WHERE 1 <= $.o.amount AND $.o.amount <= 100)"
		},
	} {
		if got := mustPlan(t, orders(edit)).Fingerprint(); got != base {
			t.Errorf("%s: fingerprint changed", name)
		}
	}
}

// TestPlan_DistinctMeaningsStayDistinct pins what canonicalization must
// not merge: forms not proven to mean the same.
func TestPlan_DistinctMeaningsStayDistinct(t *testing.T) {
	base := mustPlan(t, orders(nil)).Fingerprint()
	for name, edit := range map[string]func(latest, totals *Stage){
		"latest's order-key list": func(l, _ *Stage) {
			l.OrderBy, l.TieBy, l.WindowOrderBy, l.WindowTieBy = "", "", []string{"$.at"}, []string{"$.seq"}
		},
		"a path spelled with brackets": func(l, _ *Stage) { l.Emit[0].From = "$['customer']" },
		"a numeric orderByType":        func(l, _ *Stage) { l.OrderByType = "number" },
	} {
		if got := mustPlan(t, orders(edit)).Fingerprint(); got == base {
			t.Errorf("%s: fingerprint unchanged", name)
		}
	}
	intLit := mustPlan(t, orders(func(_, tot *Stage) { tot.When[0] = WhenClause{Path: "$.status", Equals: int64(5)} })).Fingerprint()
	floatLit := mustPlan(t, orders(func(_, tot *Stage) { tot.When[0] = WhenClause{Path: "$.status", Equals: float64(5)} })).Fingerprint()
	if intLit == floatLit {
		t.Error("equals = 5 and equals = 5.0 share a fingerprint")
	}
	arm := mustPlan(t, orders(func(_, tot *Stage) { tot.Emit[0] = Emit{Field: "orders", Count: true} })).Fingerprint()
	if arm == base {
		t.Error("the count arm and count(*) share a fingerprint")
	}
}

// TestPlan_StatesWhatTheStagesLeaveImplicit checks the engine-written
// blocks and the resolved defaults appear in the plan.
func TestPlan_StatesWhatTheStagesLeaveImplicit(t *testing.T) {
	p := mustPlan(t, orders(nil))
	if len(p.Blocks) != 3 {
		t.Fatalf("want latest, the re-key block, and totals; got %s", p.Encode())
	}
	rekey := p.Blocks[1]
	if rekey.Name != "latest[$.customer]" || rekey.Synthetic != SyntheticRekey || rekey.Key.Parts[0] != "$.customer" {
		t.Errorf("re-key block: %+v", rekey)
	}
	join := p.Blocks[2].Joins[0]
	if join.Block != "latest[$.customer]" || join.Field != "" || len(join.OnTypes) != 1 || join.OnTypes[0] != KeyTypeText {
		t.Errorf("the field join targets its re-key block in its key space: %+v", join)
	}
	if got := p.Blocks[0].Columns[1].Expr; got != "cast($.amount as integer)" {
		t.Errorf("expression: %q", got)
	}
	if got := p.Blocks[0].Reduce; got.OrderType != KeyTypeText || got.TieType != KeyTypeText {
		t.Errorf("latest's order types: %+v", got)
	}
}

// TestPlan_LeavesTheDeclaredStagesAlone: compiling admits a copy.
func TestPlan_LeavesTheDeclaredStagesAlone(t *testing.T) {
	sts := orders(nil)
	mustPlan(t, sts)
	if sts[1].When[0].compiled != nil || sts[1].Emit[0].compiled != nil || sts[1].aggSlots != nil {
		t.Error("CompilePlan compiled into the caller's stages")
	}
}

// TestPlanReference pins the plan's encoding of the store reference
// fixture, which sets every field of the stage vocabulary — so no
// construct's encoding can change unnoticed. The plan's fingerprint is
// what a store's state is derived under, and a changed encoding of an
// unchanged config would reset every store compiled from it, so the bytes
// and PlanVersion move together: a changed encoding fails here until
// PlanVersion is bumped, and the new version's reference is generated
// deliberately and committed with the bump:
//
//	UPDATE_PLAN_REFERENCE=1 go test ./internal/cluster/syncable/stages -run TestPlanReference
func TestPlanReference(t *testing.T) {
	p := mustPlan(t, storeReferenceFixture())
	var got bytes.Buffer
	require.NoError(t, json.Indent(&got, p.Encode(), "", "  "))
	got.WriteString("\n")
	referencePath := filepath.Join("testdata", fmt.Sprintf("plan_%d.reference", PlanVersion))
	if os.Getenv("UPDATE_PLAN_REFERENCE") == "1" {
		require.NoError(t, os.WriteFile(referencePath, got.Bytes(), 0o644))
		t.Logf("wrote %s", referencePath)
	}
	want, err := os.ReadFile(referencePath)
	require.NoError(t, err, "no reference for plan version %d: the version was bumped — generate its reference deliberately (UPDATE_PLAN_REFERENCE=1) and commit it with the bump", PlanVersion)
	require.Equal(t, string(want), got.String(),
		"the plan's encoding changed under version %d: unchanged configs would get new fingerprints. Bump PlanVersion and generate the new version's reference", PlanVersion)
}
