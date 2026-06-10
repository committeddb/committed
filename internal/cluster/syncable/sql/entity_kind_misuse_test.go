package sql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// typeResolvingStorage is a TestDatabaseStorage that also implements
// cluster.TypeResolver, which is what the parser's entity-kind-misuse
// check type-asserts for (the real wal.Storage implements both).
type typeResolvingStorage struct {
	TestDatabaseStorage
	types map[string]*cluster.Type
}

func (s *typeResolvingStorage) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	t, ok := s.types[ref.ID]
	if !ok {
		return nil, fmt.Errorf("type not found: %s", ref.ID)
	}
	return t, nil
}

func entityKindMisuseTOML(topic string) string {
	return `
[sql]
topic      = "` + topic + `"
db         = "testdb"
table      = "tenants"
primaryKey = "id"

[[sql.mappings]]
jsonPath = "$.id"
column   = "id"
type     = "VARCHAR(64)"
`
}

func parseTOMLWithKind(t *testing.T, p *sql.SyncableParser, storage cluster.DatabaseStorage, toml string) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	v := readConfig(t, "toml", strings.NewReader(toml))
	_, err := p.ParseConfig(v, storage)
	require.NoError(t, err)
	return logs
}

func parseWithKind(t *testing.T, p *sql.SyncableParser, storage cluster.DatabaseStorage) *observer.ObservedLogs {
	t.Helper()
	return parseTOMLWithKind(t, p, storage, entityKindMisuseTOML("tenant-topic"))
}

// A leaf-mapped sql syncable on an event-kind topic is the
// partial-clobber bug class: it must warn and count the misuse metric,
// but the config still parses (advisory, not enforcement).
func TestParseConfigWarnsOnEventKindTopic(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"tenant-topic": {ID: "tenant-topic", EntityKind: cluster.EntityKindEvent}},
	}

	logs := parseWithKind(t, &sql.SyncableParser{Metrics: m}, storage)

	entries := logs.FilterMessageSnippet("event-kind topic").All()
	require.Len(t, entries, 1, "expected the partial-clobber warning")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "committed.entity_kind.misuse" {
				found = true
				sum, ok := mt.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				require.Len(t, sum.DataPoints, 1)
				require.Equal(t, int64(1), sum.DataPoints[0].Value)
			}
		}
	}
	require.True(t, found, "committed.entity_kind.misuse must be recorded")
}

// A nil Metrics must not panic — the warning alone still fires.
func TestParseConfigWarnsOnEventKindTopicWithoutMetrics(t *testing.T) {
	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"tenant-topic": {ID: "tenant-topic", EntityKind: cluster.EntityKindEvent}},
	}

	logs := parseWithKind(t, &sql.SyncableParser{}, storage)
	require.Len(t, logs.FilterMessageSnippet("event-kind topic").All(), 1)
}

// Unspecified topics are grandfathered and must never warn; snapshot
// is the kind a key-addressed upsert is for. Unknown topics (type not
// proposed yet) and storages without type resolution stay silent too.
func TestParseConfigDoesNotWarnWhenEntityKindFits(t *testing.T) {
	for name, storage := range map[string]cluster.DatabaseStorage{
		"unspecified kind": &typeResolvingStorage{
			TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
			types:               map[string]*cluster.Type{"tenant-topic": {ID: "tenant-topic"}},
		},
		"snapshot kind": &typeResolvingStorage{
			TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
			types:               map[string]*cluster.Type{"tenant-topic": {ID: "tenant-topic", EntityKind: cluster.EntityKindSnapshot}},
		},
		"unknown topic": &typeResolvingStorage{
			TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
			types:               map[string]*cluster.Type{},
		},
		"no type resolution": &TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
	} {
		t.Run(name, func(t *testing.T) {
			logs := parseWithKind(t, &sql.SyncableParser{}, storage)
			require.Empty(t, logs.All(), "no warning expected")
		})
	}
}

// A whole-payload "$" mapping is the conventional event-log shape —
// envelope columns plus the full document — and exactly how an event
// topic should land in SQL, so it must not warn.
func TestParseConfigDoesNotWarnOnWholePayloadEventShape(t *testing.T) {
	storage := &typeResolvingStorage{
		TestDatabaseStorage: TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": testDB}},
		types:               map[string]*cluster.Type{"tenant-topic": {ID: "tenant-topic", EntityKind: cluster.EntityKindEvent}},
	}

	toml := `
[sql]
topic      = "tenant-topic"
db         = "testdb"
table      = "tenant_events"
primaryKey = "event_id"

[[sql.mappings]]
jsonPath = "$.event_id"
column   = "event_id"
type     = "VARCHAR(64)"

[[sql.mappings]]
jsonPath = "$"
column   = "payload"
type     = "JSONB"
`
	logs := parseTOMLWithKind(t, &sql.SyncableParser{}, storage, toml)
	require.Empty(t, logs.All(), "whole-payload shape on an event topic is correct, not misuse")
}
