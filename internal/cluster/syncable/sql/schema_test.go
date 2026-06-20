package sql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// noSchemaSyncable is a syncable that exposes no materialized schema (the
// stand-in for a different syncable kind, e.g. an HTTP webhook).
type noSchemaSyncable struct{}

func (noSchemaSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (noSchemaSyncable) Close() error { return nil }

// ValidateReplace reads only the parsed config (no DB), so a zero-value *sql.DB
// is enough to construct the syncables.

func projectionWith(cols ...[2]string) *sql.Projection {
	cfg := &sql.ProjectionConfig{Table: "tenants", PrimaryKey: "id"}
	for _, c := range cols {
		cfg.Columns = append(cfg.Columns, sql.ProjectionColumn{Name: c[0], SQLType: c[1]})
	}
	return sql.NewProjection(&sql.DB{}, cfg, nil, "tenants")
}

// A projection rejects an added-column replacement with a *SchemaChangeError
// that satisfies the generic cluster.RebuildRequiredError — which is all the
// generic layers ever see.
func TestProjection_ValidateReplace_AddedColumn(t *testing.T) {
	prior := projectionWith([2]string{"id", "VARCHAR(128)"})
	next := projectionWith([2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})

	err := next.ValidateReplace(prior)
	require.Error(t, err)

	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
	require.Equal(t, "schema_change_requires_rebuild", rebuild.Code())
	require.Contains(t, rebuild.Error(), "tier")

	// The structured details carry the table + added column for a pipeline.
	details, ok := rebuild.Details().(*sql.SchemaChangeError)
	require.True(t, ok)
	require.Equal(t, "tenants", details.Table)
	require.Equal(t, []string{"tier"}, details.AddedColumns)
}

// An identical (or rule-only) replacement validates fine — no rebuild needed.
func TestProjection_ValidateReplace_NoChange(t *testing.T) {
	prior := projectionWith([2]string{"id", "VARCHAR(128)"})
	next := projectionWith([2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.ValidateReplace(prior))
}

// Fail-open: replacing a prior syncable that exposes no schema (a different
// kind, or one that couldn't be built) is allowed rather than blocked.
func TestProjection_ValidateReplace_FailsOpenAgainstUnknownPrior(t *testing.T) {
	next := projectionWith([2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})
	require.NoError(t, next.ValidateReplace(&noSchemaSyncable{}))
}

// The plain table syncable validates the same way, including its indexes.
func TestSyncable_ValidateReplace_IndexChange(t *testing.T) {
	base := &sql.Config{
		Table: "events", PrimaryKey: "id",
		Mappings: []sql.Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
	}
	withIdx := &sql.Config{
		Table: "events", PrimaryKey: "id",
		Mappings: []sql.Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
		Indexes:  []sql.Index{{IndexName: "idx_id", ColumnNames: "id"}},
	}

	prior := sql.New(&sql.DB{}, base)
	next := sql.New(&sql.DB{}, withIdx)

	err := next.ValidateReplace(prior)
	require.Error(t, err)
	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
}
