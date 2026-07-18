package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// otherComparable is a cluster.SyncableSchemaComparable of a different concrete
// kind (the stand-in for e.g. an HTTP webhook). schemaComparable.SchemaChange must
// fail open against it — it can't compare shapes across kinds.
type otherComparable struct{}

func (otherComparable) SchemaChange(cluster.SyncableSchemaComparable) error { return nil }

// projectionComparable builds a *schemaComparable from a projection config —
// destination shape (main columns + aggregate/lookup fingerprint) + identity, no
// DB — exactly as ProjectionSyncableParser.SchemaFromConfig does.
func projectionComparable(cols ...[2]string) *schemaComparable {
	cfg := &ProjectionConfig{Table: "tenants", PrimaryKey: "id"}
	for _, c := range cols {
		cfg.Columns = append(cfg.Columns, ProjectionColumn{Name: c[0], SQLType: c[1]})
	}
	s := schemaOf(cfg.ddlConfig())
	s.ProjectionShape = cfg.projectionShapeFingerprint()
	return &schemaComparable{schema: s, identity: projectionIdentity(cfg)}
}

// A projection rejects an added-column replacement with a *SchemaChangeError that
// satisfies the generic cluster.RebuildRequiredError — which is all the generic
// layers ever see.
func TestProjection_SchemaChange_AddedColumn(t *testing.T) {
	prior := projectionComparable([2]string{"id", "VARCHAR(128)"})
	next := projectionComparable([2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})

	err := next.SchemaChange(prior)
	require.Error(t, err)

	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
	require.Equal(t, "schema_change_requires_rebuild", rebuild.Code())
	require.Contains(t, rebuild.Error(), "tier")

	// The structured details carry the table + added column for a pipeline.
	details, ok := rebuild.Details().(*SchemaChangeError)
	require.True(t, ok)
	require.Equal(t, "tenants", details.Table)
	require.Equal(t, []string{"tier"}, details.AddedColumns)
}

// An identical (or rule-only) replacement compares clean — no rebuild needed.
func TestProjection_SchemaChange_NoChange(t *testing.T) {
	prior := projectionComparable([2]string{"id", "VARCHAR(128)"})
	next := projectionComparable([2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.SchemaChange(prior))
}

// Fail-open: comparing against a prior of a different, incomparable kind (one that
// is not a *schemaComparable) is allowed rather than blocked.
func TestProjection_SchemaChange_FailsOpenAgainstUnknownPrior(t *testing.T) {
	next := projectionComparable([2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})
	require.NoError(t, next.SchemaChange(otherComparable{}))
}

// The plain table syncable compares the same way, including its indexes.
func TestSyncable_SchemaChange_IndexChange(t *testing.T) {
	base := &Config{
		Table: "events", PrimaryKey: "id",
		Mappings: []Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
	}
	withIdx := &Config{
		Table: "events", PrimaryKey: "id",
		Mappings: []Mapping{{Column: "id", SQLType: "VARCHAR(64)"}},
		Indexes:  []Index{{IndexName: "idx_id", ColumnNames: "id"}},
	}

	prior := &schemaComparable{schema: schemaOf(base), identity: identityOf(base)}
	next := &schemaComparable{schema: schemaOf(withIdx), identity: identityOf(withIdx)}

	err := next.SchemaChange(prior)
	require.Error(t, err)
	var rebuild cluster.RebuildRequiredError
	require.ErrorAs(t, err, &rebuild)
}
