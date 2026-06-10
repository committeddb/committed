//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

var projectionEventType = &cluster.Type{ID: "controlplane-event", Name: "ControlplaneEvent"}

func projectionConfig(table string) *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Topic:      "controlplane-event",
		Table:      table,
		PrimaryKey: "tenant_id",
		Columns: []sql.ProjectionColumn{
			{Name: "tenant_id", SQLType: "VARCHAR(64)"},
			{Name: "tier", SQLType: "VARCHAR(32)"},
			{Name: "state", SQLType: "VARCHAR(32)"},
			{Name: "allocs", SQLType: "JSONB"},
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
				},
			},
		},
	}
}

func projectionEvent(t *testing.T, key string, fields map[string]any) *cluster.Actual {
	t.Helper()
	bs, err := json.Marshal(fields)
	require.NoError(t, err)
	return &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(projectionEventType, []byte(key), bs),
	}}
}

// TestPostgreSQLIntegration_ProjectionLifecycle is the ticket's success
// criterion on real Postgres: the tenant lifecycle folds to one
// converged row per tenant (ON CONFLICT upserts restricted to each
// rule's columns), replaying everything twice over reproduces identical
// state, unmatched events leave no row, and delete Actuals hard-delete.
func TestPostgreSQLIntegration_ProjectionLifecycle(t *testing.T) {
	table := uniqueTable(t)
	defer dropTable(t, table)

	db, err := sql.NewDB(&dialects.PostgreSQLDialect{}, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	projection := sql.NewProjection(db, projectionConfig(table), nil, "tenants")
	require.Nil(t, projection.Init())
	ctx := context.Background()

	lifecycle := []*cluster.Actual{
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.created", "tier": "dev"}),
		projectionEvent(t, "t2", map[string]any{"tenant_id": "t2", "event_type": "tenant.created", "tier": "prod"}),
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.provisioned", "allocs": map[string]any{"cpu": 4}}),
		projectionEvent(t, "t3", map[string]any{"tenant_id": "t3", "event_type": "tenant.billed"}), // no rule
		projectionEvent(t, "t1", map[string]any{"tenant_id": "t1", "event_type": "tenant.deprovisioned"}),
	}

	replay := func() {
		for _, a := range lifecycle {
			_, err := projection.Sync(ctx, a)
			require.Nil(t, err)
		}
	}

	type row struct {
		Tier   string
		State  string
		Allocs any
	}
	readRows := func() map[string]row {
		rows, err := db.DB.Query("SELECT tenant_id, tier, state, allocs FROM " + table)
		require.Nil(t, err)
		defer rows.Close()
		got := map[string]row{}
		for rows.Next() {
			var id string
			var r row
			var tier, allocs gosql.NullString
			require.Nil(t, rows.Scan(&id, &tier, &r.State, &allocs))
			r.Tier = tier.String
			if allocs.Valid {
				require.Nil(t, json.Unmarshal([]byte(allocs.String), &r.Allocs))
			}
			got[id] = r
		}
		require.Nil(t, rows.Err())
		return got
	}

	want := map[string]row{
		"t1": {Tier: "dev", State: "deprovisioning", Allocs: map[string]any{"cpu": float64(4)}},
		"t2": {Tier: "prod", State: "pending"},
	}

	replay()
	require.Equal(t, want, readRows(), "fold result after first pass")

	// Replay from index 0 — twice over — must reproduce identical state.
	replay()
	replay()
	require.Equal(t, want, readRows(), "fold result after replaying twice")

	// Delete Actual hard-deletes exactly t1; a ghost delete is a no-op.
	for _, key := range []string{"t1", "ghost"} {
		_, err := projection.Sync(ctx, &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(projectionEventType, []byte(key)),
		}})
		require.Nil(t, err)
	}
	require.Equal(t, map[string]row{"t2": want["t2"]}, readRows())
}
