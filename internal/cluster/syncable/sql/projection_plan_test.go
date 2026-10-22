package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// tablePlanFixtures are the configs whose plans TestTablePlanReference
// pins. Together they set every field of the table-source vocabulary
// (TestTablePlanFixturesCoverTheVocabulary), so no construct's encoding
// can change unnoticed.
func tablePlanFixtures() map[string]*ProjectionConfig {
	return map[string]*ProjectionConfig{
		// Rules with every set arm (lookup enrichment from a dimension), a
		// lookup dimension, and an aggregate with an enriched element and
		// filtered scalars.
		"card": {
			Table: "card", PrimaryKey: []string{"id"},
			Columns: []ProjectionColumn{
				{Name: "id", SQLType: "varchar(64)"},
				{Name: "title", SQLType: "VARCHAR(64)"},
				{Name: "flag", SQLType: "BOOLEAN"},
				{Name: "note", SQLType: "VARCHAR(64)"},
				{Name: "kind", SQLType: "VARCHAR(64)"},
				{Name: "cust", SQLType: "VARCHAR(64)"},
				{Name: "cust_name", SQLType: "VARCHAR(64)"},
				{Name: "items", SQLType: "JSONB"},
				{Name: "n", SQLType: "BIGINT"},
				{Name: "top", SQLType: "VARCHAR(64)"},
			},
			Sources: []ProjectionSource{
				{
					Topic: "movies", KeyPath: []string{"$.id"}, OnDelete: onDeleteRow, Normalize: "lower",
					When: []WhenClause{{Path: "$.kind", Equals: "movie"}, {Path: "$.year", GreaterThan: int64(1900)}},
					Rules: []ProjectionRule{{
						When: []WhenClause{{Path: "$.title", NotNull: true}},
						Set: []ProjectionSet{
							{Column: "title", From: "$.title"},
							{Column: "flag", Value: true},
							{Column: "note", Null: true},
							{Column: "kind", Expr: "upper($.k)"},
							{Column: "cust", From: "$.cust"},
							{Column: "cust_name", Lookup: "dim", On: "cust", Select: "name"},
						},
					}},
				},
				{
					Topic: "dims", KeyPath: []string{"$.id"},
					Lookup: &ProjectionLookup{Name: "dim", Fields: []ProjectionElementField{{Field: "name", From: "$.name"}}},
				},
				{
					Topic: "lines", KeyPath: []string{"$.movie"},
					Aggregate: &ProjectionAggregate{
						Column: "items", ElementKey: "$.n", ElementKeyType: elementKeyTypeNumber,
						Element: []ProjectionElementField{
							{Field: "sku", From: "$.sku"},
							{Field: "sku_name", Lookup: "dim", On: "sku", Select: "name"},
						},
						Scalars: []ProjectionScalar{
							{Column: "n", Fn: scalarFnCount, Where: []ScalarWhere{{Field: "sku", Equals: "x"}, {Field: "sku", Null: true}}},
							{Column: "top", Fn: scalarFnMax, Of: "sku", OfType: elementKeyTypeNumber},
						},
					},
				},
			},
		},
		// A row owner, a stage-fed decorator, and a fanned source.
		"owned": {
			Table: "t", PrimaryKey: []string{"id"},
			Columns: []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "w", SQLType: "VARCHAR(64)"}, {Name: "v", SQLType: "VARCHAR(64)"}},
			Stages: []ProjectionStage{{
				Name: "st", From: "tp", KeyPath: []string{"$.id"},
				Emit: []StageEmit{{Field: "v", From: "$.v"}},
			}},
			Sources: []ProjectionSource{
				{
					Topic: "x", KeyPath: []string{"$.id"}, RowOwner: true,
					Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "w", From: "$.w"}}}},
				},
				{
					FromStage: "st",
					Rules:     []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}},
				},
			},
		},
		"fanned": {
			Table: "lines", PrimaryKey: []string{"id"},
			Columns: []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "order_id", SQLType: "VARCHAR(64)"}},
			Sources: []ProjectionSource{{
				Topic: "orders", ForEach: "$.lines[*]", KeyPath: []string{"$.id"},
				Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "order_id", From: "$parent.id"}}}},
			}},
		},
	}
}

func mustProjectionPlan(t *testing.T, c *ProjectionConfig) *ProjectionPlan {
	t.Helper()
	require.NoError(t, validated(t, c))
	p, err := c.compilePlan()
	require.NoError(t, err)
	return p
}

// TestTablePlanReference pins the projection plan's encoding of the
// fixtures, as stages.TestPlanReference pins the stages': a changed
// encoding fails here until tablePlanVersion is bumped, and the new
// version's reference is generated deliberately and committed with the
// bump:
//
//	UPDATE_PLAN_REFERENCE=1 go test ./internal/cluster/syncable/sql -run TestTablePlanReference
func TestTablePlanReference(t *testing.T) {
	fixtures := tablePlanFixtures()
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)
	var got bytes.Buffer
	for _, name := range names {
		fmt.Fprintf(&got, "# %s\n", name)
		require.NoError(t, json.Indent(&got, mustProjectionPlan(t, fixtures[name]).Encode(), "", "  "))
		got.WriteString("\n")
	}
	referencePath := filepath.Join("testdata", fmt.Sprintf("table_plan_%d.reference", tablePlanVersion))
	if os.Getenv("UPDATE_PLAN_REFERENCE") == "1" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(referencePath, got.Bytes(), 0o644))
		t.Logf("wrote %s", referencePath)
	}
	want, err := os.ReadFile(referencePath)
	require.NoError(t, err, "no reference for table plan version %d: the version was bumped — generate its reference deliberately (UPDATE_PLAN_REFERENCE=1) and commit it with the bump", tablePlanVersion)
	require.Equal(t, string(want), got.String(), "the projection plan's encoding changed under table plan version %d: bump tablePlanVersion and generate the new version's reference", tablePlanVersion)
}

func TestTablePlanFixturesCoverTheVocabulary(t *testing.T) {
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
				if f.IsExported() && !v.Field(i).IsZero() {
					seen[tp.Name()+"."+f.Name] = true
				}
				if f.IsExported() {
					walk(v.Field(i))
				}
			}
		}
	}
	for _, c := range tablePlanFixtures() {
		walk(reflect.ValueOf(c.Sources))
		walk(reflect.ValueOf(c.Columns))
	}
	var missing []string
	for _, tp := range []reflect.Type{
		reflect.TypeOf(ProjectionSource{}), reflect.TypeOf(ProjectionRule{}), reflect.TypeOf(ProjectionSet{}),
		reflect.TypeOf(ProjectionAggregate{}), reflect.TypeOf(ProjectionScalar{}), reflect.TypeOf(ScalarWhere{}),
		reflect.TypeOf(ProjectionElementField{}), reflect.TypeOf(ProjectionLookup{}), reflect.TypeOf(ProjectionColumn{}),
	} {
		for i := 0; i < tp.NumField(); i++ {
			if f := tp.Field(i); f.IsExported() && !seen[tp.Name()+"."+f.Name] {
				missing = append(missing, tp.Name()+"."+f.Name)
			}
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "table-source vocabulary fields no plan fixture sets — add a fixture that uses each, so its encoding is pinned")
}

// TestTablePlan_SpellingsCompileToOnePlan pins the table side's
// canonicalizations.
func TestTablePlan_SpellingsCompileToOnePlan(t *testing.T) {
	base := mustProjectionPlan(t, tablePlanFixtures()["card"]).Fingerprint()
	for name, edit := range map[string]func(c *ProjectionConfig){
		"keyPath left to its default":  func(c *ProjectionConfig) { c.Sources[0].KeyPath = nil },
		"onDelete left to its default": func(c *ProjectionConfig) { c.Sources[0].OnDelete = "" },
		"column type case":             func(c *ProjectionConfig) { c.Columns[1].SQLType = " varchar(64)" },
		"expression spelling":          func(c *ProjectionConfig) { c.Sources[0].Rules[0].Set[3].Expr = "UPPER( $.k )" },
		"when clauses reordered": func(c *ProjectionConfig) {
			w := c.Sources[0].When
			w[0], w[1] = w[1], w[0]
		},
	} {
		c := tablePlanFixtures()["card"]
		edit(c)
		require.Equal(t, base, mustProjectionPlan(t, c).Fingerprint(), name)
	}

	// The single-source shorthand is the one-source form.
	cols := []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "v", SQLType: "VARCHAR(64)"}}
	rules := []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}}
	shorthand := &ProjectionConfig{Table: "t", PrimaryKey: []string{"id"}, Columns: cols, Topic: "x", Rules: rules}
	sources := &ProjectionConfig{Table: "t", PrimaryKey: []string{"id"}, Columns: cols, Sources: []ProjectionSource{{Topic: "x", Rules: rules}}}
	require.Equal(t, mustProjectionPlan(t, sources).Fingerprint(), mustProjectionPlan(t, shorthand).Fingerprint(), "shorthand")
}

// TestTablePlan_DistinctMeaningsStayDistinct pins what the table side's
// canonicalization must not merge.
func TestTablePlan_DistinctMeaningsStayDistinct(t *testing.T) {
	base := mustProjectionPlan(t, tablePlanFixtures()["card"]).Fingerprint()
	for name, edit := range map[string]func(c *ProjectionConfig){
		"onDelete clear":       func(c *ProjectionConfig) { c.Sources[0].OnDelete = onDeleteClear },
		"a float literal":      func(c *ProjectionConfig) { c.Sources[0].When[0].Equals = 1.0 },
		"an int literal value": func(c *ProjectionConfig) { c.Sources[0].Rules[0].Set[1].Value = int64(1) },
		"numeric element order": func(c *ProjectionConfig) {
			c.Sources[2].Aggregate.ElementKeyType = elementKeyTypeText
		},
	} {
		c := tablePlanFixtures()["card"]
		edit(c)
		require.NotEqual(t, base, mustProjectionPlan(t, c).Fingerprint(), name)
	}
}
