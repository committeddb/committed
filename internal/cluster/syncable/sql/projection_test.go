package sql_test

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

var tenantEventType = &cluster.Type{ID: "controlplane-event", Name: "ControlplaneEvent"}

// tenantProjectionConfig is the hosted tenant-lifecycle shape from the
// ticket: created sets tier+pending, provisioned sets active+allocs,
// deprovisioned sets deprovisioning and clears allocs to SQL NULL.
func tenantProjectionConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      "tenants",
		PrimaryKey: "tenant_id",
		Columns: []sql.ProjectionColumn{
			{Name: "tenant_id", SQLType: "VARCHAR(64)"},
			{Name: "tier", SQLType: "VARCHAR(32)"},
			{Name: "state", SQLType: "VARCHAR(32)"},
			{Name: "allocs", SQLType: "JSON"},
		},
		Rules: []sql.ProjectionRule{
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.created"}},
				Set: []sql.ProjectionSet{
					{Column: "tier", From: "$.tier"},
					{Column: "state", Value: "pending"},
				},
			},
			{
				When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.provisioned"}},
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
		},
	}
}

func eventJSON(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	bs, err := json.Marshal(fields)
	require.NoError(t, err)
	return bs
}

func eventActual(t *testing.T, key string, fields map[string]any) *cluster.Actual {
	t.Helper()
	return &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tenantEventType, []byte(key), eventJSON(t, fields)),
	}}
}

// newMockProjection wires a Projection against sqlmock, registering the
// Init expectations (DDL, one prepare per rule, the delete prepare) and
// returning the per-rule and delete prepare handles for exec
// expectations. Expected SQL is computed through the same dialect the
// projection uses, with the synthesized pk-plus-rule-columns shape.
func newMockProjection(t *testing.T, config *sql.ProjectionConfig, m *metrics.Metrics) (
	*sql.Projection, sqlmock.Sqlmock, []*sqlmock.ExpectedPrepare, *sqlmock.ExpectedPrepare,
) {
	t.Helper()
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ddlMappings := make([]sql.Mapping, 0, len(config.Columns))
	for _, c := range config.Columns {
		ddlMappings = append(ddlMappings, sql.Mapping{Column: c.Name, SQLType: c.SQLType})
	}
	ddlConfig := &sql.Config{Table: config.Table, Mappings: ddlMappings, PrimaryKey: config.PrimaryKey}
	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)

	rulePrepares := make([]*sqlmock.ExpectedPrepare, 0, len(config.Rules))
	for _, r := range config.Rules {
		mappings := make([]sql.Mapping, 0, len(r.Set)+1)
		mappings = append(mappings, sql.Mapping{Column: config.PrimaryKey})
		for _, s := range r.Set {
			mappings = append(mappings, sql.Mapping{Column: s.Column})
		}
		ruleConfig := &sql.Config{Table: config.Table, Mappings: mappings, PrimaryKey: config.PrimaryKey}
		rulePrepares = append(rulePrepares, mock.ExpectPrepare(dialect.CreateSQL(ruleConfig)))
	}
	deletePrepare := mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	projection := sql.NewProjection(db, config, m, "tenants")
	require.NoError(t, projection.Init())
	return projection, mock, rulePrepares, deletePrepare
}

// One matching rule executes one upsert bound key-first then the set
// values in manifest order (the mock dialect doubles args like MySQL).
func TestProjectionSyncAppliesMatchingRule(t *testing.T) {
	projection, mock, rules, _ := newMockProjection(t, tenantProjectionConfig(), nil)

	mock.ExpectBegin()
	args := []driver.Value{"t1", "dev", "pending"}
	rules[0].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ss, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev",
	}))
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A null set entry binds SQL NULL — the only way to clear a column,
// since TOML has no null literal for value and a missing from path on
// a matched rule is a permanent error, not NULL.
func TestProjectionNullSetBindsSQLNull(t *testing.T) {
	projection, mock, rules, _ := newMockProjection(t, tenantProjectionConfig(), nil)

	mock.ExpectBegin()
	args := []driver.Value{"t1", "deprovisioning", nil}
	rules[2].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.deprovisioned",
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Two rules matching one event execute in manifest order inside the
// same transaction — the SQL-side mechanism behind last-rule-wins.
func TestProjectionMultiRuleMatchExecutesInOrder(t *testing.T) {
	config := tenantProjectionConfig()
	// A fourth rule that also fires on tenant.created and overwrites state.
	config.Rules = append(config.Rules, sql.ProjectionRule{
		When: []sql.WhenClause{{Path: "$.event_type", Equals: "tenant.created"}},
		Set:  []sql.ProjectionSet{{Column: "state", Value: "audited"}},
	})
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	mock.ExpectBegin()
	first := []driver.Value{"t1", "dev", "pending"}
	rules[0].ExpectExec().WithArgs(append(first, first...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	second := []driver.Value{"t1", "audited"}
	rules[3].ExpectExec().WithArgs(append(second, second...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev",
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A multi-clause when is a conjunction: all clauses must hold.
func TestProjectionWhenConjunction(t *testing.T) {
	config := tenantProjectionConfig()
	config.Rules = []sql.ProjectionRule{{
		When: []sql.WhenClause{
			{Path: "$.event_type", Equals: "tenant.provisioned"},
			{Path: "$.tier", Equals: "prod"},
		},
		Set: []sql.ProjectionSet{{Column: "state", Value: "active"}},
	}}
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	// tier=dev: conjunction fails, no SQL.
	mock.ExpectBegin()
	mock.ExpectCommit()
	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.provisioned", "tier": "dev",
	}))
	require.NoError(t, err)

	// tier=prod: both clauses hold.
	mock.ExpectBegin()
	args := []driver.Value{"t1", "active"}
	rules[0].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	_, err = projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.provisioned", "tier": "prod",
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A null when clause matches a present JSON null only: an absent
// field is "no match" (the standing missing-path invariant), and a
// non-null value is not null.
func TestProjectionWhenNullMatchesPresentNullOnly(t *testing.T) {
	config := tenantProjectionConfig()
	config.Rules = []sql.ProjectionRule{{
		When: []sql.WhenClause{
			{Path: "$.event_type", Equals: "tenant.audited"},
			{Path: "$.allocs", Null: true},
		},
		Set: []sql.ProjectionSet{{Column: "state", Value: "unallocated"}},
	}}
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	// allocs present and null: the clause holds.
	mock.ExpectBegin()
	args := []driver.Value{"t1", "unallocated"}
	rules[0].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.audited", "allocs": nil,
	}))
	require.NoError(t, err)

	// allocs absent: missing path is "no match", never a null match.
	mock.ExpectBegin()
	mock.ExpectCommit()
	_, err = projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.audited",
	}))
	require.NoError(t, err)

	// allocs non-null: no match.
	mock.ExpectBegin()
	mock.ExpectCommit()
	_, err = projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.audited", "allocs": map[string]any{"cpu": 4},
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TOML integers (int64) must match JSON numbers (float64) in when
// clauses.
func TestProjectionWhenMatchesNumericLiterals(t *testing.T) {
	config := tenantProjectionConfig()
	config.Rules = []sql.ProjectionRule{{
		When: []sql.WhenClause{{Path: "$.version", Equals: int64(3)}},
		Set:  []sql.ProjectionSet{{Column: "state", Value: "v3"}},
	}}
	projection, mock, rules, _ := newMockProjection(t, config, nil)

	mock.ExpectBegin()
	args := []driver.Value{"t1", "v3"}
	rules[0].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "version": 3,
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Zero matching rules → zero SQL (no ghost rows) and one tick on the
// unmatched counter, attributed by syncable name and topic.
func TestProjectionZeroMatchLeavesNoRowAndTicksMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	projection, mock, _, _ := newMockProjection(t, tenantProjectionConfig(), m)

	mock.ExpectBegin()
	mock.ExpectCommit()
	ss, err := projection.Sync(context.Background(), eventActual(t, "t9", map[string]any{
		"tenant_id": "t9", "event_type": "tenant.billed",
	}))
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)
	require.NoError(t, mock.ExpectationsWereMet())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "committed.sync.rules_unmatched" {
				found = true
				sum, ok := mt.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				require.Len(t, sum.DataPoints, 1)
				require.Equal(t, int64(1), sum.DataPoints[0].Value)
			}
		}
	}
	require.True(t, found, "committed.sync.rules_unmatched must be recorded")
}

// A matched rule whose from path is missing in the payload is a
// permanent error (skip, not retry) — mirrors the plain syncable's
// mapping semantics.
func TestProjectionMissingFromPathIsPermanent(t *testing.T) {
	projection, mock, _, _ := newMockProjection(t, tenantProjectionConfig(), nil)

	mock.ExpectBegin()
	mock.ExpectRollback()
	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"tenant_id": "t1", "event_type": "tenant.created", // no tier
	}))
	require.Error(t, err)
	require.True(t, errors.Is(err, cluster.ErrPermanent))
	require.NoError(t, mock.ExpectationsWereMet())
}

// A matched event with no resolvable key is a permanent
// misconfiguration; an unmatched event without a key is a non-event
// (key resolution is lazy, after matching).
func TestProjectionMissingKeyPath(t *testing.T) {
	projection, mock, _, _ := newMockProjection(t, tenantProjectionConfig(), nil)

	// Matched rule, no tenant_id → permanent.
	mock.ExpectBegin()
	mock.ExpectRollback()
	_, err := projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"event_type": "tenant.deprovisioned",
	}))
	require.Error(t, err)
	require.True(t, errors.Is(err, cluster.ErrPermanent))

	// Unmatched event, no tenant_id → clean no-op.
	mock.ExpectBegin()
	mock.ExpectCommit()
	_, err = projection.Sync(context.Background(), eventActual(t, "t1", map[string]any{
		"event_type": "tenant.billed",
	}))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Delete Actuals hard-delete the projected row by entity Key; the
// sentinel payload is never unmarshaled, and deleting an absent row is
// a no-op (fresh-replay-after-scrub).
func TestProjectionDeleteActual(t *testing.T) {
	projection, mock, _, deletePrepare := newMockProjection(t, tenantProjectionConfig(), nil)

	mock.ExpectBegin()
	deletePrepare.ExpectExec().WithArgs("t1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	ss, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(tenantEventType, []byte("t1")),
	}})
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)

	mock.ExpectBegin()
	deletePrepare.ExpectExec().WithArgs("ghost").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	_, err = projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(tenantEventType, []byte("ghost")),
	}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// An Actual for another topic is skipped before any transaction begins.
func TestProjectionSkipsOtherTopics(t *testing.T) {
	projection, mock, _, _ := newMockProjection(t, tenantProjectionConfig(), nil)

	otherType := &cluster.Type{ID: "other-topic", Name: "Other"}
	ss, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(otherType, []byte("k"), eventJSON(t, map[string]any{"x": 1})),
	}})
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(false), ss)
	require.NoError(t, mock.ExpectationsWereMet(), "no transaction may begin for a foreign topic")
}

// SyncBatch applies a mixed batch — upserts and deletes, foreign
// entities skipped — in one transaction, in order.
func TestProjectionSyncBatchMixed(t *testing.T) {
	projection, mock, rules, deletePrepare := newMockProjection(t, tenantProjectionConfig(), nil)

	otherType := &cluster.Type{ID: "other-topic", Name: "Other"}

	mock.ExpectBegin()
	created := []driver.Value{"t1", "dev", "pending"}
	rules[0].ExpectExec().WithArgs(append(created, created...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	deletePrepare.ExpectExec().WithArgs("t2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	as := []*cluster.Actual{
		eventActual(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev"}),
		{Entities: []*cluster.Entity{cluster.NewUpsertEntity(otherType, []byte("x"), eventJSON(t, map[string]any{"x": 1}))}},
		{Entities: []*cluster.Entity{cluster.NewDeleteEntity(tenantEventType, []byte("t2"))}},
	}
	ok, err := projection.SyncBatch(context.Background(), as)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- Real-fold verification on go-mysql-server (MySQL dialect family) ---

// tenantRow is one projected row. Allocs holds the decoded JSON
// document (nil when absent): native JSON columns normalize key order
// and whitespace — and go-mysql-server reads an untouched JSON column
// back as JSON null — so semantic equality is the contract here, not
// byte equality (same caveat as the whole-payload tests).
type tenantRow struct {
	Tier   string
	State  string
	Allocs any
}

func readTenants(t *testing.T, db *sql.DB) map[string]tenantRow {
	t.Helper()
	rows, err := db.DB.Query("SELECT tenant_id, tier, state, allocs FROM tenants")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]tenantRow{}
	for rows.Next() {
		var id string
		var r tenantRow
		var tier, allocs gosql.NullString
		require.NoError(t, rows.Scan(&id, &tier, &r.State, &allocs))
		r.Tier = tier.String
		if allocs.Valid {
			require.NoError(t, json.Unmarshal([]byte(allocs.String), &r.Allocs))
		}
		got[id] = r
	}
	require.NoError(t, rows.Err())
	return got
}

// The ticket's success criterion end to end: a tenant lifecycle folds
// into one converged row per tenant, interleaved entities converge
// independently, replaying everything twice over reproduces identical
// state, unmatched events leave no row, and a delete Actual removes
// exactly its row.
func TestProjectionLifecycleGoMySQLServer(t *testing.T) {
	db := newGoMySQLServerDB(t, "projectionlifecycle")
	defer db.Close()

	projection := sql.NewProjection(db, tenantProjectionConfig(), nil, "tenants")
	require.NoError(t, projection.Init())
	ctx := context.Background()

	lifecycle := []*cluster.Actual{
		eventActual(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev"}),
		eventActual(t, "t2", map[string]any{"tenant_id": "t2", "event_type": "tenant.created", "tier": "prod"}),
		eventActual(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.provisioned", "allocs": map[string]any{"cpu": 4}}),
		eventActual(t, "t3", map[string]any{"tenant_id": "t3", "event_type": "tenant.billed"}), // no rule
		eventActual(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.deprovisioned"}),
	}

	replay := func() {
		for _, a := range lifecycle {
			_, err := projection.Sync(ctx, a)
			require.NoError(t, err)
		}
	}

	// First pass runs event by event so the mid-lifecycle state is
	// observable: provisioned lands the allocs document, then
	// deprovisioned's null set entry clears it back to NULL — proving
	// the final NULL is a clear, not a never-written column.
	for _, a := range lifecycle[:3] {
		_, err := projection.Sync(ctx, a)
		require.NoError(t, err)
	}
	require.Equal(t, map[string]any{"cpu": float64(4)}, readTenants(t, db)["t1"].Allocs,
		"allocs set by tenant.provisioned")
	for _, a := range lifecycle[3:] {
		_, err := projection.Sync(ctx, a)
		require.NoError(t, err)
	}

	want := map[string]tenantRow{
		"t1": {Tier: "dev", State: "deprovisioning"},
		"t2": {Tier: "prod", State: "pending"},
	}
	require.Equal(t, want, readTenants(t, db), "fold result after first pass")

	// Replay from index 0 — twice over — must reproduce identical state
	// (idempotent, deterministic fold; redelivery is the recovery
	// mechanism).
	replay()
	replay()
	require.Equal(t, want, readTenants(t, db), "fold result after replaying twice")

	// Delete Actual hard-deletes exactly t1; the ghost delete is a no-op.
	for _, key := range []string{"t1", "ghost"} {
		_, err := projection.Sync(ctx, &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(tenantEventType, []byte(key)),
		}})
		require.NoError(t, err)
	}
	require.Equal(t, map[string]tenantRow{"t2": want["t2"]}, readTenants(t, db))
}
