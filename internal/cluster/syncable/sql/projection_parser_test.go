package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

const projectionTOML = `
[syncable]
name = "tenants"
type = "sql-projection"
mode = "always-current"

[sql-projection]
topic      = "controlplane-event"
db         = "testdb"
table      = "tenants"
primaryKey = "tenant_id"

[[sql-projection.columns]]
name = "tenant_id"
type = "VARCHAR(256)"

[[sql-projection.columns]]
name = "tier"
type = "VARCHAR(32)"

[[sql-projection.columns]]
name = "state"
type = "VARCHAR(32)"

[[sql-projection.columns]]
name = "allocs"
type = "JSONB"

[[sql-projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.created" } ]
set  = [
  { column = "tier",  from  = "$.tier" },
  { column = "state", value = "pending" },
]

[[sql-projection.rules]]
when = [
  { path = "$.event_type", equals = "tenant.provisioned" },
  { path = "$.tier",       equals = "prod" },
]
set  = [
  { column = "state",  value = "active" },
  { column = "allocs", from  = "$.allocs" },
]

[[sql-projection.rules]]
when = [ { path = "$.event_type", equals = "tenant.deprovisioned" } ]
set  = [
  { column = "state",  value = "deprovisioning" },
  { column = "allocs", null  = true },
]
`

func projectionStorage() *TestDatabaseStorage {
	return &TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}}
}

func TestParseProjectionConfig(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(projectionTOML))

	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.NoError(t, err)

	require.Equal(t, "controlplane-event", config.Topic)
	require.Equal(t, "tenants", config.Table)
	require.Equal(t, "tenant_id", config.PrimaryKey)
	require.Equal(t, "$.tenant_id", config.KeyPath, "keyPath defaults to $.<primaryKey>")
	require.Equal(t, []sql.ProjectionColumn{
		{Name: "tenant_id", SQLType: "VARCHAR(256)"},
		{Name: "tier", SQLType: "VARCHAR(32)"},
		{Name: "state", SQLType: "VARCHAR(32)"},
		{Name: "allocs", SQLType: "JSONB"},
	}, config.Columns)
	require.Equal(t, []sql.ProjectionRule{
		{
			When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.created"}},
			Set: []sql.ProjectionSet{
				{Column: "tier", From: "$.tier"},
				{Column: "state", Value: "pending"},
			},
		},
		{
			When: []sql.WhenClause{
				{Path: "$.event_type", Equals: "tenant.provisioned"},
				{Path: "$.tier", Equals: "prod"},
			},
			Set: []sql.ProjectionSet{
				{Column: "state", Value: "active"},
				{Column: "allocs", From: "$.allocs"},
			},
		},
		{
			When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.deprovisioned"}},
			Set: []sql.ProjectionSet{
				{Column: "state", Value: "deprovisioning"},
				{Column: "allocs", Null: true},
			},
		},
	}, config.Rules)
}

// The jsonpath in a when clause is a TOML *value*, so case survives
// viper (which lowercases map keys — the reason the inline-table form
// `when = { "$.eventType" = … }` does not exist).
func TestParseProjectionConfigPreservesPathCase(t *testing.T) {
	toml := `
[sql-projection]
topic      = "t"
db         = "testdb"
table      = "rows"
primaryKey = "id"

[[sql-projection.columns]]
name = "id"
type = "TEXT"

[[sql-projection.columns]]
name = "v"
type = "TEXT"

[[sql-projection.rules]]
when = [ { path = "$.eventType", equals = "x" } ]
set  = [ { column = "v", from = "$.camelCase" } ]
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.NoError(t, err)
	require.Equal(t, "$.eventType", config.Rules[0].When[0].Path)
	require.Equal(t, "$.camelCase", config.Rules[0].Set[0].From)
}

func TestParseProjectionConfigKeyPathOverride(t *testing.T) {
	toml := strings.Replace(projectionTOML,
		`primaryKey = "tenant_id"`,
		"primaryKey = \"tenant_id\"\nkeyPath    = \"$.meta.id\"", 1)
	v := readConfig(t, "toml", strings.NewReader(toml))

	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.NoError(t, err)
	require.Equal(t, "$.meta.id", config.KeyPath)
}

// A null when clause ({ path, null = true }) parses to Null: true —
// TOML has no null literal, so the flag form stands in, same as the
// set side.
func TestParseProjectionWhenNull(t *testing.T) {
	toml := `
[sql-projection]
topic      = "t"
db         = "testdb"
table      = "rows"
primaryKey = "id"

[[sql-projection.columns]]
name = "id"
type = "TEXT"

[[sql-projection.columns]]
name = "state"
type = "TEXT"

[[sql-projection.rules]]
when = [ { path = "$.allocs", null = true } ]
set  = [ { column = "state", value = "unallocated" } ]
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.NoError(t, err)
	require.Equal(t, []sql.WhenClause{{Path: "$.allocs", Null: true}}, config.Rules[0].When)
}

// Every rule/config misuse must fail at config time — nothing may wedge
// the worker at sync time.
func TestParseProjectionConfigRejectsMisuse(t *testing.T) {
	base := `
[sql-projection]
topic      = "t"
db         = "testdb"
table      = "rows"
primaryKey = "id"

[[sql-projection.columns]]
name = "id"
type = "TEXT"

[[sql-projection.columns]]
name = "v"
type = "TEXT"
`
	tests := []struct {
		name    string
		rules   string
		wantErr string
	}{
		{
			"both from and value",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\", from = \"$.v\", value = \"y\" } ]",
			"exactly one of from, value, or null",
		},
		{
			"neither from nor value nor null",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\" } ]",
			"exactly one of from, value, or null",
		},
		{
			"both value and null",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\", value = \"y\", null = true } ]",
			"exactly one of from, value, or null",
		},
		{
			"both from and null",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\", from = \"$.v\", null = true } ]",
			"exactly one of from, value, or null",
		},
		{
			"unknown column",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"nope\", value = \"y\" } ]",
			`sets unknown column "nope"`,
		},
		{
			"sets primary key",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"id\", value = \"y\" } ]",
			"may not set the primary-key column",
		},
		{
			"duplicate column in one rule",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\", value = \"a\" }, { column = \"v\", value = \"b\" } ]",
			`sets column "v" twice`,
		},
		{
			"missing when",
			"[[sql-projection.rules]]\nset = [ { column = \"v\", value = \"y\" } ]",
			"when is required",
		},
		{
			"empty set",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]",
			"set is required",
		},
		{
			"unknown when key",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equal = \"x\" } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			`unknown key "equal"`,
		},
		{
			"when missing equals and null",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\" } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			"exactly one of equals or null",
		},
		{
			"when with both equals and null",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\", null = true } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			"exactly one of equals or null",
		},
		{
			"when null false",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", null = false } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			"null = false is not a predicate",
		},
		{
			"when null not a boolean",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", null = \"yes\" } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			"when null must be a boolean",
		},
		{
			"non-scalar equals",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = { a = 1 } } ]\nset = [ { column = \"v\", value = \"y\" } ]",
			"equals must be a scalar",
		},
		{
			"non-scalar value",
			"[[sql-projection.rules]]\nwhen = [ { path = \"$.t\", equals = \"x\" } ]\nset = [ { column = \"v\", value = [1, 2] } ]",
			"value must be a scalar",
		},
		{
			"when wrong shape",
			"[[sql-projection.rules]]\nwhen = 42\nset = [ { column = \"v\", value = \"y\" } ]",
			"when must be a string",
		},
		{
			"no rules",
			"",
			"at least one rule is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := readConfig(t, "toml", strings.NewReader(base+"\n"+tt.rules))
			_, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestParseProjectionConfigRejectsPrimaryKeyNotDeclared(t *testing.T) {
	toml := `
[sql-projection]
topic      = "t"
db         = "testdb"
table      = "rows"
primaryKey = "missing"

[[sql-projection.columns]]
name = "id"
type = "TEXT"

[[sql-projection.rules]]
when = [ { path = "$.t", equals = "x" } ]
set  = [ { column = "id", value = "y" } ]
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	_, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.Error(t, err)
	require.Contains(t, err.Error(), `primaryKey "missing" is not a declared column`)
}

// when = "tenant.created" is sugar for equality on the topic type's
// declared discriminator (type-kinds).
func TestParseProjectionWhenShorthand(t *testing.T) {
	toml := `
[sql-projection]
topic      = "tenant-topic"
db         = "testdb"
table      = "rows"
primaryKey = "id"

[[sql-projection.columns]]
name = "id"
type = "TEXT"

[[sql-projection.columns]]
name = "state"
type = "TEXT"

[[sql-projection.rules]]
when = "tenant.created"
set  = [ { column = "state", value = "pending" } ]
`
	v := readConfig(t, "toml", strings.NewReader(toml))

	storage := &typeResolvingStorage{
		TestDatabaseStorage: *projectionStorage(),
		types: map[string]*cluster.Type{"tenant-topic": {
			ID: "tenant-topic", EntityKind: cluster.EntityKindEvent, Discriminator: "$.event_type",
		}},
	}
	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.NoError(t, err)
	require.Equal(t, []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.created"}}, config.Rules[0].When)

	// Without a discriminator on the type, the shorthand cannot resolve;
	// the error names the explicit escape hatch.
	storage.types["tenant-topic"].Discriminator = ""
	_, err = (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares no discriminator")
	require.Contains(t, err.Error(), "{ path, equals }")

	// Without a resolvable type at all, same escape hatch.
	delete(storage.types, "tenant-topic")
	_, err = (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no resolvable type")

	// A storage that cannot resolve types (no TypeResolver) also fails
	// loudly rather than guessing.
	_, err = (&sql.ProjectionSyncableParser{}).ParseConfig(v, projectionStorage())
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resolve types")
}

// A projection on a snapshot-kind topic is dead weight (snapshots are
// total updates with nothing to fold) — advisory warn, config still
// parses.
func TestParseProjectionWarnsOnSnapshotKindTopic(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	toml := strings.Replace(projectionTOML, `topic      = "controlplane-event"`, `topic      = "tenant-topic"`, 1)
	v := readConfig(t, "toml", strings.NewReader(toml))

	storage := &typeResolvingStorage{
		TestDatabaseStorage: *projectionStorage(),
		types: map[string]*cluster.Type{"tenant-topic": {
			ID: "tenant-topic", EntityKind: cluster.EntityKindSnapshot,
		}},
	}
	_, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.NoError(t, err)
	require.Len(t, logs.FilterMessageSnippet("snapshot-kind topic").All(), 1)

	// Event-kind is the projection's home turf: no warning.
	logs.TakeAll()
	storage.types["tenant-topic"].EntityKind = cluster.EntityKindEvent
	_, err = (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.NoError(t, err)
	require.Empty(t, logs.All())
}
