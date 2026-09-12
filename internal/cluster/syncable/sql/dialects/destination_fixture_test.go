package dialects_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// sinkFixture is the set of sinks the destination reference folds into one
// database: two history tables (keyless and keyed), a single-source
// projection, and two multi-source projections that between them use every
// construct the SQL sink family renders — rules with every set arm,
// aggregates with element lookups and scalars, spine enrichment, forEach,
// internal stages, row ownership, key normalization. The vocabulary
// coverage test below fails naming any config field the fixture never sets,
// so a new construct cannot be added without its rendering getting pinned.
type sinkFixture struct {
	hist, keyed, values *sql.Config
	single              *sql.ProjectionConfig
	movies, jobs, items *sql.ProjectionConfig
	tableNames          []string
}

func destinationReferenceFixture(prefix, jsonType, floatType string) sinkFixture {
	hist := &sql.Config{
		Topic: "audit", Table: prefix + "_hist",
		Mappings: []sql.Mapping{
			{JsonPath: "$.actor", Column: "actor", SQLType: "VARCHAR(32)"},
			{JsonPath: "$.action", Column: "action", SQLType: "VARCHAR(32)"},
			{JsonPath: "$.amount", Column: "amount", SQLType: "DECIMAL(12,2)"},
		},
		Indexes: []sql.Index{{IndexName: "actor_idx", ColumnNames: "actor"}},
	}
	keyed := &sql.Config{
		Topic: "account", Table: prefix + "_keyed", PrimaryKey: []string{"id"}, KeyColumn: "id",
		Mappings: []sql.Mapping{
			{JsonPath: "$.id", Column: "id", SQLType: "VARCHAR(16)"},
			{JsonPath: "$.owner", Column: "owner", SQLType: "VARCHAR(32)"},
			{JsonPath: "$.balance", Column: "balance", SQLType: "DECIMAL(12,2)"},
			{JsonPath: "$.tags", Column: "tags", SQLType: jsonType},
		},
		Indexes: []sql.Index{{IndexName: "owner_idx", ColumnNames: "owner"}},
	}
	// The value matrix: one column per type family, fed one row per JSON
	// kind (sinkValueRows), so value rendering — not just config words — is
	// pinned. TestDestinationReferenceValueMatrixCoversEveryKind holds it complete.
	values := &sql.Config{
		Topic: "values", Table: prefix + "_values", PrimaryKey: []string{"id"},
		Mappings: []sql.Mapping{
			{JsonPath: "$.id", Column: "id", SQLType: "VARCHAR(16)"},
			{JsonPath: "$.txt", Column: "txt", SQLType: "VARCHAR(64)"},
			{JsonPath: "$.i", Column: "i", SQLType: "BIGINT"},
			{JsonPath: "$.d", Column: "d", SQLType: "DECIMAL(12,4)"},
			{JsonPath: "$.f", Column: "f", SQLType: floatType},
			{JsonPath: "$.b", Column: "b", SQLType: "BOOLEAN"},
			{JsonPath: "$.j", Column: "j", SQLType: jsonType},
		},
	}
	single := &sql.ProjectionConfig{
		Topic: "tenant-event", Table: prefix + "_single", PrimaryKey: []string{"tenant_id"}, KeyPath: []string{"$.tenant_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "tenant_id", SQLType: "VARCHAR(16)"},
			{Name: "tier", SQLType: "VARCHAR(16)"},
			{Name: "state", SQLType: "VARCHAR(16)"},
			{Name: "seats", SQLType: "INT"},
			{Name: "allocs", SQLType: jsonType},
		},
		Rules: []sql.ProjectionRule{
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "created"}},
				Set: []sql.ProjectionSet{
					{Column: "tier", From: "$.tier"},
					{Column: "state", Value: "pending"},
					{Column: "seats", Expr: "$.seats * 2"},
				},
			},
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "provisioned"}},
				Set:  []sql.ProjectionSet{{Column: "state", Value: "active"}, {Column: "allocs", From: "$.allocs"}},
			},
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "deprovisioned"}},
				Set:  []sql.ProjectionSet{{Column: "state", Value: "gone"}, {Column: "allocs", Null: true}},
			},
		},
	}
	castElement := []sql.ProjectionElementField{
		{Field: "ordering", From: "$.ordering"},
		{Field: "nconst", From: "$.nconst"},
		{Field: "category", From: "$.category"},
		{Field: "nick", From: "$.nick"},
		{Field: "name", Lookup: "names", On: "nconst", Select: "primary_name"},
	}
	movies := &sql.ProjectionConfig{
		Table: prefix + "_movies", PrimaryKey: []string{"tconst"},
		Columns: []sql.ProjectionColumn{
			{Name: "tconst", SQLType: "VARCHAR(16)"},
			{Name: "primary_title", SQLType: "VARCHAR(64)"},
			{Name: "top_cast", SQLType: jsonType},
			{Name: "n_cast", SQLType: "INT"},
			{Name: "max_order", SQLType: "INT"},
			{Name: "actors", SQLType: "INT"},
			{Name: "unnamed", SQLType: "INT"},
		},
		Sources: []sql.ProjectionSource{
			// The lookup source the aggregate's element fields enrich from.
			// Declaration order does not matter (Init's dimension pre-pass
			// covers element-field lookups); it is first here for reading.
			{
				Topic:  "name",
				Lookup: &sql.ProjectionLookup{Name: "names", Fields: []sql.ProjectionElementField{{Field: "primary_name", From: "$.primary_name"}}},
			},
			{
				Topic: "title", KeyPath: []string{"$.tconst"}, OnDelete: "delete-row", RowOwner: true,
				When:  []sql.WhenClause{{Path: "$.kind", Equals: "movie"}},
				Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "primary_title", From: "$.primary_title"}}}},
			},
			{
				Topic: "principal", KeyPath: []string{"$.tconst"}, OnDelete: "remove-from-aggregate", Normalize: "lower",
				Aggregate: &sql.ProjectionAggregate{
					Column: "top_cast", ElementKey: "$.ordering", ElementKeyType: "number", Element: castElement,
					Scalars: []sql.ProjectionScalar{
						{Column: "n_cast", Fn: "count"},
						{Column: "max_order", Fn: "max", Of: "ordering", OfType: "number"},
						{Column: "actors", Fn: "count", Where: []sql.ScalarWhere{{Field: "category", Equals: "actor"}}},
						{Column: "unnamed", Fn: "count", Where: []sql.ScalarWhere{{Field: "nick", Null: true}}},
					},
				},
			},
		},
	}
	jobs := &sql.ProjectionConfig{
		Table: prefix + "_jobs", PrimaryKey: []string{"job_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "job_id", SQLType: "VARCHAR(32)"},
			{Name: "tenant_id", SQLType: "INT"},
			{Name: "tenant_name", SQLType: "VARCHAR(64)"},
			{Name: "total", SQLType: "DECIMAL(12,2)"},
			{Name: "n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{
			{Name: "live", From: "txn", KeyPath: []string{"$.txn"}, Emit: []sql.StageEmit{{Field: "job", From: "$.job"}, {Field: "amt", From: "$.total"}}},
			{Name: "by-job", From: "live", KeyPath: []string{"$.job"}, Reduce: "aggregate", Emit: []sql.StageEmit{{Field: "total", Sum: "$.amt"}, {Field: "n", Count: true}}},
		},
		Sources: []sql.ProjectionSource{
			{
				Topic: "job", KeyPath: []string{"$.id"}, OnDelete: "delete-row", RowOwner: true,
				Rules: []sql.ProjectionRule{{
					When: []sql.WhenClause{{Path: "$.kind", Equals: "job"}},
					Set: []sql.ProjectionSet{
						{Column: "tenant_id", From: "$.tenant"},
						{Column: "tenant_name", Lookup: "tenants", On: "tenant_id", Select: "name"},
					},
				}},
			},
			{
				Topic:  "tenant",
				Lookup: &sql.ProjectionLookup{Name: "tenants", Fields: []sql.ProjectionElementField{{Field: "name", From: "$.name"}}},
			},
			{
				FromStage: "by-job", KeyPath: []string{"$.job"},
				Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "total", From: "$.total"}, {Column: "n", From: "$.n"}}}},
			},
		},
	}
	// forEach fans one event into one row per element; its rows are keyed by
	// the element, so it gets a table of its own rather than competing with
	// a row owner.
	items := &sql.ProjectionConfig{
		Table: prefix + "_items", PrimaryKey: []string{"item_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "item_id", SQLType: "VARCHAR(32)"},
			{Name: "amount", SQLType: "DECIMAL(12,2)"},
			{Name: "txn_id", SQLType: "VARCHAR(32)"},
		},
		Sources: []sql.ProjectionSource{{
			Topic: "txn", KeyPath: []string{"$.id"}, ForEach: "$.items[*]",
			Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "amount", From: "$.amount"}, {Column: "txn_id", From: "$parent.txn"}}}},
		}},
	}
	return sinkFixture{
		hist: hist, keyed: keyed, values: values, single: single, movies: movies, jobs: jobs, items: items,
		tableNames: []string{hist.Table, keyed.Table, values.Table, single.Table, movies.Table, jobs.Table, items.Table},
	}
}

// sinkEvent builds one committed Actual for a topic.
func sinkEvent(t *testing.T, index uint64, topic, key string, fields map[string]any) *cluster.Actual {
	t.Helper()
	bs, err := json.Marshal(fields)
	require.NoError(t, err)
	tp := &cluster.Type{ID: topic, Name: topic}
	return &cluster.Actual{Index: index, Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte(key), bs)}}
}

func sinkDelete(index uint64, topic, key string) *cluster.Actual {
	tp := &cluster.Type{ID: topic, Name: topic}
	return &cluster.Actual{Index: index, Entities: []*cluster.Entity{cluster.NewDeleteEntity(tp, []byte(key))}}
}

// TestDestinationReferenceFixtureCoversTheVocabulary: every config-tagged field of the
// SQL sink vocabulary is set somewhere in the fixture, or this fails naming
// it. Excluded: the database handle and id (resolved by the parser, not a
// rendering), the checkpoint policy (cadence, not a rendering), and the
// stage vocabulary (pinned by the stage store's own reference).
func TestDestinationReferenceFixtureCoversTheVocabulary(t *testing.T) {
	fx := destinationReferenceFixture("t", "JSON", "DOUBLE")
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
				if !v.Field(i).IsZero() {
					seen[tp.Name()+"."+f.Name] = true
				}
				walk(v.Field(i))
			}
		}
	}
	for _, c := range []any{fx.hist, fx.keyed, fx.values, fx.single, fx.movies, fx.jobs, fx.items} {
		walk(reflect.ValueOf(c))
	}
	excluded := map[string]bool{
		"Config.Database": true, "Config.DatabaseID": true, "Config.Checkpoint": true,
		"ProjectionConfig.Database": true, "ProjectionConfig.DatabaseID": true,
	}
	var missing []string
	for _, tp := range []reflect.Type{
		reflect.TypeOf(sql.Config{}), reflect.TypeOf(sql.Mapping{}), reflect.TypeOf(sql.Index{}),
		reflect.TypeOf(sql.ProjectionConfig{}), reflect.TypeOf(sql.ProjectionColumn{}), reflect.TypeOf(sql.ProjectionSource{}),
		reflect.TypeOf(sql.ProjectionRule{}), reflect.TypeOf(sql.ProjectionSet{}), reflect.TypeOf(sql.ProjectionAggregate{}),
		reflect.TypeOf(sql.ProjectionElementField{}), reflect.TypeOf(sql.ProjectionLookup{}), reflect.TypeOf(sql.ProjectionScalar{}),
		reflect.TypeOf(sql.ScalarWhere{}),
	} {
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			name := tp.Name() + "." + f.Name
			if !f.IsExported() || excluded[name] {
				continue
			}
			if !seen[name] {
				missing = append(missing, name)
			}
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "sink vocabulary fields the destination-reference fixture never sets — add a sink or source that uses each, so its rendering is pinned")
}

// sinkValueRows is the value matrix's feed: the same seven columns, one row
// per way a JSON document can spell a value — unicode and escapes, the
// empty string, integers past 2^53, decimals as numbers and as strings,
// exponents, negatives, both booleans, null, and every JSON kind in the
// JSON column.
func sinkValueRows() []map[string]any {
	return []map[string]any{
		{"id": "r1", "txt": "héllo \"q\" \\ 😀", "i": 42, "d": "5.0000", "f": 1e3, "b": true, "j": map[string]any{"a": []any{1, "b", nil}, "z": "é"}},
		{"id": "r2", "txt": "", "i": -7, "d": 5.25, "f": -0.5, "b": false, "j": []any{}},
		{"id": "r3", "txt": nil, "i": json.Number("9007199254740993"), "d": "0.10", "f": 2.5e-3, "b": nil, "j": nil},
		{"id": "r4", "txt": "x", "i": 0, "d": 0, "f": 0, "b": true, "j": map[string]any{"n": 7.5, "s": "str", "t": true}},
		{"id": "r5", "txt": "y", "i": nil, "d": "1", "f": 1, "b": false, "j": []any{"str", 7.5, true, nil}},
		{"id": "r6", "txt": "z", "i": 2, "d": "2.50", "f": nil, "b": true, "j": map[string]any{}},
	}
}

// TestDestinationReferenceValueMatrixCoversEveryKind: the JSON column receives an
// object, an array, and null (the kinds a JSON column takes — scalars land in
// typed columns), and every other column receives at least two distinct
// kinds (a typed value and null, or a number and its string spelling), so a
// rendering path cannot go unexercised because the feed happened to skip it.
func TestDestinationReferenceValueMatrixCoversEveryKind(t *testing.T) {
	kind := func(v any) string {
		switch v.(type) {
		case nil:
			return "null"
		case string:
			return "string"
		case bool:
			return "bool"
		case map[string]any:
			return "object"
		case []any:
			return "array"
		default:
			return "number"
		}
	}
	seen := map[string]map[string]bool{}
	for _, row := range sinkValueRows() {
		for col, v := range row {
			if seen[col] == nil {
				seen[col] = map[string]bool{}
			}
			seen[col][kind(v)] = true
		}
	}
	for _, k := range []string{"null", "object", "array"} {
		require.True(t, seen["j"][k], "the JSON column never receives a %s", k)
	}
	for _, col := range []string{"txt", "i", "d", "f", "b"} {
		require.GreaterOrEqual(t, len(seen[col]), 2, "column %s receives only one JSON kind", col)
	}
}
